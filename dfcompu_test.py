#! /usr/bin/python
# by pts@fazekas.hu at Mon May 22 15:33:44 CEST 2017

import sys
import unittest

from dfcompu import recipe, ConstantInput, run_graph, thread_pool_runner
from dfcompu import InputSequence, ContextInput, ExceptionResult


@recipe
def area(a, b):
  answer = 42
  return a * b


@recipe
def circumference(a, b):
  return int(2) * (a + b)


@recipe(result=('a_next', 'b_next'))
def next_fib(a, b):
  return b, a + b


@recipe()  # () is not needed.
def cond(c, true_input, false_input):
  if c:
    return true_input
  else:
    return false_input


# !! This doesn't work as expected, recipes shouldn't be calling each other
#    directly, it produces sequential execution.
#@recipe
#def area_or_circumference(c, a, b):
#  if c:
#    return area(a, b)  # !!
#  else:
#    return circumference(a, b)  # !!


@recipe
def or_all(*args):
  """Returns the first true value."""
  for arg in args:
    yield arg.wait()
    value = arg.get()
    if value:
      yield value
      break


@recipe
def noop():
  if 0:
    yield 42


@recipe
def add_tuple(a, b):
  return tuple(a) + tuple(b)


@recipe
def cmul(a, b_context):
  return a * b_context


@recipe
def xkeys(context):
  return sorted(context)


@recipe
def bad_luck():
  raise ValueError('Bad luck.')


class DfcompuTest(unittest.TestCase):
  def test_early_delete(self):
    """Test that intermediate values are deleted early in graph execution.

    This test is exepected fail in Jython, PyPi and all Python implementations
    without reference counting. Probably only CPython has reference counting.
    """

    try:
      sys.getrefcount(())
      has_refcount = True
    except (AttributeError, NotImplementedError):
      has_refcount = False

    class LoggingNumber(object):
      """A number which logs __init__ and __del__ calls."""
      __slots__ = ('nvalue', 'log_list')
      def __init__(self, nvalue, log_list):
        log_list.append(nvalue)
        self.nvalue = nvalue
        self.log_list = log_list
      def __del__(self):
        self.log_list.append(-self.nvalue)
      def __add__(self, nvalue):
        return type(self)(self.nvalue + nvalue, self.log_list)
      def __radd__(self, nvalue):
        return type(self)(nvalue + self.nvalue, self.log_list)

    @recipe
    def add(a, b):
      return a + b

    def build_add_graph(ln, count):
      """Create a graph, which adds 1 count times to ln."""
      for _ in xrange(count):
        ln = add.node(ln, 1)
      return ln

    def test_with_runner(runner=None):
      import gc
      old_gc = gc.isenabled()
      try:
        try:
          gc.disable()
        except NotImplementedError:  # In Jython.
          pass
        base, count = 1, 100
        log_list = []
        result_node = build_add_graph(LoggingNumber(base, log_list), count)
        assert result_node.run(runner=runner).nvalue == base + count
        expected_log_list = [base]
        for i in xrange(base, base + count):
          expected_log_list.append(i + 1)
          # It's important that __del__ on the previous temporary value is
          # called (hence the negative -i value here) right after the
          # current value is created.
          expected_log_list.append(-i)
        assert ([x for x in expected_log_list if x > 0] ==
                [x for x in log_list if x > 0])
        if has_refcount:
          assert expected_log_list == log_list, (
              expected_log_list, log_list,
              'Expected to fail in Jython, PyPy all non-reference-counting '
              'implementations of Python.')
      finally:
        if old_gc:
          gc.enable()
        else:
          try:
            gc.disable()
          except NotImplementedError:  # In Jython.
            pass

    test_with_runner()
    test_with_runner(runner=thread_pool_runner(1))
    test_with_runner(runner=thread_pool_runner(3))

  def build_acr_graph(self):
    a = 5
    b = ConstantInput(7)
    _, c = next_fib.node(a, b)
    _, d = next_fib.node(b, c)
    area_ab = area.node(a, b)
    circumference_ab = circumference.node(a, b)
    return cond.node(d, area_ab, circumference_ab)

  def test_misc(self):
    #print area
    assert area(5, 6) == 30
    #  Simple call without a graph, for unit tests.
    assert area(ConstantInput(5), 6) == 30
    assert cond(0, 7, 8) == 8
    assert or_all(False, (), [], 33, 0, 44, 0.0) == 33

    a = 5  # TODO(pts): Reuse implicit ConstantInput objects within the graph?
    b = ConstantInput(7)
    abn = (a, b)
    abn = next_fib.node(*abn)
    abn = next_fib.node(*abn)
    abn = next_fib.node(*abn)
    rgv = run_graph((abn, b))
    #print rgv
    assert len(rgv) == 2
    assert rgv[0].get() == (19, 31)
    assert rgv[1] is b

    assert run_graph(next_fib.node(20, 30)[1])[0].get() == 50
    assert next_fib.node(20, 30)[1].run() == 50
    assert next_fib.node(20, 30).run() == (30, 50)

    acr = self.build_acr_graph()
    run_graph((acr,))
    assert acr.is_available()
    assert acr.get() == 35

    acr = self.build_acr_graph()
    debug_nodes = []
    fake_time_ary = [100]
    def fake_get_time():
      fake_time_ary[0] += 10
      return fake_time_ary[0]
    run_graph((acr,), debug_nodes=debug_nodes, get_time_func=fake_get_time)
    assert acr.is_available()
    assert acr.get() == 35
    node_names = [node.name for node in debug_nodes]
    # Node names are in BFS (depth) order, not simple_runner execution order
    # (which if DFS order).
    assert node_names == [
        'cond', 'next_fib#1', 'area', 'circumference', 'next_fib#2']
    # `circumference' is not called at all, it's on the wrong branch of `cond'.
    assert [(node.name, node.start_time, node.end_time)
            for node in debug_nodes] == [
        ('cond', 110, 180), ('next_fib#1', 120, 150), ('area', 160, 170),
        ('circumference', None, None), ('next_fib#2', 130, 140)]

    acr = self.build_acr_graph()
    run_graph((acr,), runner=thread_pool_runner(1))
    assert acr.is_available()
    assert acr.get() == 35

    acr = self.build_acr_graph()
    run_graph((acr,), runner=thread_pool_runner(3))
    assert acr.is_available()
    assert acr.get() == 35

    try:
      assert 0, noop.node().run()
    except RuntimeError, e:
      assert str(e) == 'No values yielded by recipe iterator.'

    assert add_tuple(InputSequence(5, area.node(2, 3)), [7]) == (5, 6, 7)

    assert xkeys.node(None).run(context={'Jan': 0, 'Feb': 1}) == ['Feb', 'Jan']
    assert cmul.node(5, ContextInput('x')).run(context={'x': 8}) == 40
    assert cmul.node(5, None).run(context={'b': 8}) == 40
    # It's OK to omit the _trailing_ context args.
    assert cmul.node(5).run(context={'b': 8}) == 40

    try:
      bl = bad_luck.node()
      assert 0, area(bl, bl)
    except ValueError, e:
      assert str(e) == 'Bad luck.'

    try:
      bl = bad_luck.node()
      assert 0, area.node(bl, bl).run(runner=thread_pool_runner(3))
    except ValueError, e:
      assert str(e) == 'Bad luck.'

    debug_nodes = []
    fake_time_ary[0] = 100
    try:
      bl = bad_luck.node()
      assert 0, area.node(bl, bl).run(
          get_time_func=fake_get_time, debug_nodes=debug_nodes,
          runner=thread_pool_runner(3))
    except ValueError, e:
      assert str(e) == 'Bad luck.'
    assert [(node.name, node.start_time, node.end_time, node.result_ary)
            for node in debug_nodes] == [
        ('area', 110, None, []),
        ('bad_luck', 120, 130, [ExceptionResult(ValueError('Bad luck.',))])]


if __name__ == '__main__':
  unittest.main()
