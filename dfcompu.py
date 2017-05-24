#! /usr/bin/python
# by pts@fazekas.hu at Mon May 22 15:33:44 CEST 2017
#
# Compatible with Python 2.4, 2.5, 2.6 and 2.7.
#
# doc: For simplicity, graph contexts are not supported, arguments need to
# be passed around in graph builder functions (e.g. build_acr_graph()).
#
# doc: Global caches and graph-specific caches etc. can be passed around either
# in the context or as ConstantInput values.
#
# doc: Exception behavior: The first exception halts execution and gets
# propagated to the caller of run_graph. If the thread pool is running other
# nodes in the same time, they will run until they have to wait or they are
# done, but their results won't be used.
#
# TODO(pts): Add force_name to Recipe and Node for presistence.
# TODO(pts): Add scheduling based on async I/O, sleep and external waiting.
# TODO(pts): Exception handling, error propagation.
# TODO(pts): Produce individual results incrementally?
# TODO(pts): Document mutability.
# TODO(pts): Add printing the graph and peeking.
# TODO(pts): How to delete values early during graph execution?
# TODO(pts): Get rid of memory leaks using weak references. Test when the
#            garbage collector is disabled.
# TODO(pts): Support dynamic graph building and cycles that way. This
#            doesn't play well with thread pools when called from recipe
#            functions (rather than generators), it blocks a thread.
# TODO(pts): Ensure that node names are unique.
#

import collections
import weakref

def is_generator_function(object):  # From inspect.isgeneratorfunction.
  CO_GENERATOR = 0x20  # From Include/code.h.  TODO(pts): Jython?
  return bool(object.func_code.co_flags & CO_GENERATOR)


class Wait(object):
  __slots__ = ('inputs',)
  def __init__(self, inputs):
    self.inputs = inputs


EMPTY_WAIT = Wait(())


class Input(object):
  __slots__ = ()
  def get(self):
    # Must not return a subclass of Input.
    raise NotImplementedError('Subclasses should implement this.')
  def is_available(self):
    raise NotImplementedError('Subclasses should implement this.')
  def wait(self):
    raise NotImplementedError('Subclasses should implement this.')


class ConstantInput(Input):
  __slots__ = ('value',)
  def __init__(self, value):
    self.value = value
  def __repr__(self):
    return 'ConstantInput(%r)' % (self.value,)
  def get(self):
    return self.value
  def is_available(self):
    return True
  def wait(self):
    return EMPTY_WAIT


class ContextInput(Input):
  __slots__ = ('key', 'context')
  def __init__(self, key):
    if not isinstance(key, str) and key is not None:
      raise TypeError
    self.key = key
    self.context = None
  def __repr__(self):
    return 'ContextInput(key=%r)' % (self.key,)
  def get(self):
    context = self.context
    if context is None:
      raise ValueError('Context not set in get.')
    if self.key is None:
      return context
    return self.context[self.key]  # Can raise KeyError.
  def is_available(self):
    return self.context is not None
  def wait(self):
    if self.context is None:
      raise ValueError('Cannot wait for context.')
    return EMPTY_WAIT
  def set_context(self, context):
    if self.context is context:
      return
    if self.context is not None:
      raise RuntimeError('Context already set.')
    if not isinstance(context, dict):
      raise RuntimeError('Context must be a dict.')
    self.context = context


def convert_function_to_generator(f, arg_names):
  def function_recipe_generator(*args):
    args2 = []
    for i, arg in enumerate(args):
      if i >= len(arg_names) or arg_names[i].endswith('_input'):
        args2.append(arg)
      elif (isinstance(arg, ConstantInput) and
            isinstance(arg.value, InputSequence)):
        arg2 = []
        for arg0 in arg.value.items:
          if isinstance(arg0, Input):
            yield arg0.wait()  # !! Wait for multiple events at the same time, for multithreading.
            arg2.append(arg0.get())
          else:
            arg2.append(arg0)
        args2.append(arg2)
        del arg2  # Save memory.
      else:
        yield arg.wait()  # !! Wait for multiple events at the same time, for multithreading.
        args2.append(arg.get())
    del args  # Save memory.
    yield f(*args2)
  function_recipe_generator.func_name = f.func_name
  return function_recipe_generator


class Recipe(object):
  __slots__ = ('generator', 'arg_names', 'result_names', 'has_varargs',
               'result_tuple_type')

  def __init__(self, generator, result=('result',)):
    if not callable(generator) or not getattr(generator, 'func_code', None):
      raise ValueError
    CO_VARARGS = 0x4
    CO_VARKEYWORDS = 0x8
    if generator.func_code.co_argcount < 0:  # Can this happen?
      raise ValueError
    if generator.func_code.co_cellvars:  # Is this a problem?
      raise ValueError
    if generator.func_code.co_freevars:  # Is this a problem?
      raise ValueError
    # We don't check it, because e.g. method names can be here.
    #if generator.func_code.co_names:  # Is this a problem?
    #  raise ValueError(generator.func_code.co_names)
    if generator.func_code.co_flags & CO_VARKEYWORDS:
      raise ValueError
    self.has_varargs = bool(generator.func_code.co_flags & CO_VARARGS)
    self.arg_names = arg_names = tuple(generator.func_code.co_varnames[
        :generator.func_code.co_argcount])
    if not is_generator_function(generator):
      generator = convert_function_to_generator(generator, arg_names)
    self.generator = generator
    self.result_names = tuple(map(str, result))
    if (len(self.result_names) != 1 or
        self.result_names[0] != 'result'):
      if getattr(collections, 'namedtuple', None):
        self.result_tuple_type = collections.namedtuple(
            self.generator.func_name + '___results', self.result_names)
      else:  # Python 2.4 doesn't have collections.namedtuple.
        self.result_tuple_type = lambda *args: tuple(args)
    else:
      self.result_tuple_type = None

  def __repr__(self):
    return 'Recipe(name=%r, %s)' % (
        self.generator.func_name,
        ', '.join('%s=%r' % (k, getattr(self, k)) for k in self.__slots__
                  if k != 'generator' and k != 'result_tuple_type'))

  def node(self, *args, **kwargs):
    return Node(self, self._prepare_args(*args, **kwargs))

  def __call__(self, *args, **kwargs):
    return run_graph(
        (Node(self, self._prepare_args(*args, **kwargs)),))[0].result

  def _prepare_args(self, *args, **kwargs):
    arg_names, has_varargs = self.arg_names, self.has_varargs
    del self  # Won't be needed.
    if args:
      if kwargs:
        raise ValueError('Both *args and **kwargs specified.')
      args = list(args)
    else:
      order_dict = dict((v, k) for k, v in enumerate(arg_names))
      args = [None] * len(arg_names)
      for k,v in kwargs.iteritems():
        args[order_dict[k]] = v
    for i, arg in enumerate(args):
      if not isinstance(arg, Input):
        args[i] = ConstantInput(arg)
    if len(args) < len(arg_names):
      missing_arg_names = [arg_name for arg_name in arg_names[len(args):]
                           if arg_name != 'context' and
                           not arg_name.endswith('_context')]
      if missing_arg_names:
        raise ValueError('Missing args for recipe: %r' % missing_arg_names)
      while len(args) < len(arg_names):
        args.append(ConstantInput(None))  # For context arg.
    if ((not has_varargs and len(args) != len(arg_names)) or
        (has_varargs and len(args) < len(arg_names))):
      raise ValueError('Recipe to be called with wrong number of arguments.')
    return args


class InputSequence(object):
  __slots__ = ('items',)
  def __init__(self, *args):
    self.items = tuple(args)


class NodeBase(Input):
  __slots__ = ()


class NodeSubresultInput(NodeBase):
  # .name has same interface as Node, for run_graph.
  __slots__ = ('node', 'i')
  def __init__(self, node, i):
    self.node = node
    self.i = i
    #self.name = '%s.%s' % (
    #    self.node.name, self.node.recipe.result_names[self.i])
  def __repr__(self):
    return 'NodeSubResultInput(node=%r, i=%d)' % (self.node, self.i)
  def get(self):
    return self.node.get()[self.i]
  def is_available(self):
    return self.node.is_available()
  def wait(self):
    return self.node.wait()
  def run(self, **kwargs):  # Convenience method, same interface as Node.
    return run_graph((self,))[0].get()
  @property  # Same interface as Node, for run_graph.
  def node_iterator(self):
    return self.node.node_iterator
  @property  # Same interface as Node, for run_graph.
  def inputs(self):
    return self.node.node_iterator
  @property  # Same interface as Node, for run_graph.
  def name(self):
    # Recomputing the name every time, in case self.node.name has changed,
    # e.g. in run_graph.
    return '%s.%s' % (
        self.node.name, self.node.recipe.result_names[self.i])


class Node(NodeBase):
  __slots__ = ('result', 'has_result', 'recipe', 'inputs', 'node_iterator',
               'name', '__weakref__')

  def __init__(self, recipe, inputs):
    if not isinstance(recipe, Recipe):
      raise TypeError
    for input in inputs:
      if not isinstance(input, Input):
        raise TypeError
    if ((not recipe.has_varargs and len(inputs) != len(recipe.arg_names)) or
        (recipe.has_varargs and len(inputs) < len(recipe.arg_names))):
      raise ValueError('Recipe Node with wrong number of arguments.')

    def wrap_node_iterator(weak_node, generator, inputs):
      iterator = generator(*inputs)  # This is delayed until the first call.
      del generator, inputs  # Save memory.  !!
      for value in iterator:
        if weak_node().has_result:
          raise RuntimeError('Multiple values yielded by recipe iterator.')
        if isinstance(value, Wait):
          yield value.inputs
        elif isinstance(value, Input):
          yield value.wait().inputs
          weak_node().set_result(value.get())
        else:
          weak_node().set_result(value)
      if not weak_node().has_result:
        raise RuntimeError('No values yielded by recipe iterator.')

    self.result = None
    self.has_result = False
    self.recipe = recipe
    # list instead of tuple so _fix_context_inputs can change it in place.
    self.inputs = list(inputs)  # TODO(pts): Does this cause memory leaks?
    self.name = self.recipe.generator.func_name
    self.node_iterator = wrap_node_iterator(
        weakref.ref(self), self.recipe.generator, self.inputs)

  def __repr__(self):
    # TODO(pts): Display inputs, detect cycles.
    return (
        'Node(recipe=%r, has_result=%r, result=%r, '
        'inputs=#%d, results=#%d)' %
        (self.recipe, self.has_result, self.result, len(self.inputs),
         len(self.recipe.result_names)))

  def run(self, **kwargs):
    """Convenience method to call run_graph."""
    return run_graph((self,), **kwargs)[0].get()

  def get(self):
    if not self.has_result:
      raise RuntimeError('Node result not available yet.')
    return self.result

  def is_available(self):
     return self.has_result

  def wait(self):
    if self.has_result:
      return EMPTY_WAIT
    else:
      return Wait((self,))

  def set_result(self, result):
    if self.has_result:
      raise RuntimeError('Setting result multiple times.')
    if self.recipe.result_tuple_type:
      if not isinstance(result, (tuple, list)):
        raise ValueError('tuple result expected.')
      if len(result) != len(self.recipe.result_names):
        raise ValueError('Result tuple size mismatch: expected=%d, got=%d' %
                         (len(self.recipe.result_names), len(result)))
      result = self.recipe.result_tuple_type(*result)
    self.has_result = True
    self.result = result
    self.node_iterator = None

  def __len__(self):
    return len(self.recipe.result_names)

  # Used by x, y, ... = foo.node(...)
  def __getitem__(self, i):
    # Creating a NodeSubresultInput on the fly, to avoid circular references.
    # TODO(pts): Use a weakref.
    if not (0 <= i < len(self.recipe.result_names)):
      raise IndexError
    return NodeSubresultInput(self, i)


def _find_all_nodes(inputs):
  cache = set()
  todo = []
  for input3 in inputs:
    if (isinstance(input3, ConstantInput) and
        isinstance(input3.value, InputSequence)):
      todo.extend(input2 for input2 in input3.value.items
                  if isinstance(input2, Input) and
                  not input2.is_available() and
                  input2 not in cache)
    else:
      todo.append(input3)
  result = []
  for input in todo:
    if not input.is_available() and isinstance(input, NodeBase):
      if isinstance(input, NodeSubresultInput):
        input = input.node
      if input not in cache:
        cache.add(input)
        result.append(input)
        todo.extend(input2 for input2 in input.inputs
                    if not input2.is_available() and input2 not in cache)
        for input3 in input.inputs:
          if (isinstance(input3, ConstantInput) and
              isinstance(input3.value, InputSequence)):
            todo.extend(input2 for input2 in input3.value.items
                        if isinstance(input2, Input) and
                        not input2.is_available() and
                        input2 not in cache)
  return result


def _rename_nodes(nodes):
  node_name_counts = {}
  for node in nodes:
    name = node.name
    node_name_counts[name] = node_name_counts.get(name, 0) + 1
  for node in nodes:
    name = node.name
    if node_name_counts[name] == 1:
      del node_name_counts[name]
  for node in reversed(nodes):
    name = node.name
    nc = node_name_counts.get(name)
    if nc is not None:
      node.name += '#%d' % nc
      node_name_counts[name] = nc - 1


def yield_inputs(input):
  if (isinstance(input, ConstantInput) and
      isinstance(input.value, InputSequence)):
    for input2 in input.value.items:
      if isinstance(input2, Input):
        yield input
  else:
    yield input


def _fix_context_inputs(nodes):
  for node in nodes:
    arg_names = node.recipe.arg_names
    inputs = node.inputs
    len_arg_names = len(arg_names)
    for i, input in enumerate(inputs):
      if i >= len(arg_names):
        break
      arg_name = arg_names[i]
      if (arg_name == 'context' and isinstance(input, ConstantInput) and
          input.value is None):
        inputs[i] = ContextInput(None)
      elif (arg_name.endswith('_context') and
            isinstance(input, ConstantInput) and input.value is None):
        inputs[i] = ContextInput(arg_name[:arg_name.rfind('_')])


def _add_context_to_node_inputs(nodes, context):
  for node in nodes:
    for input in node.inputs:
      for input2 in yield_inputs(input):
        if isinstance(input2, ContextInput):
          input2.set_context(context)


def _get_unavailable_input_nodes(inputs):
  if inputs:
    inputs = [input for input in inputs if not input.is_available()]
    for input in inputs:
      if not isinstance(input, NodeBase):
        raise TypeError('Node class expected, got: %r' %
                        type(input))
  return inputs


def simple_runner(pending_inputs):
  """Runs nodes in the current thread, one at a time."""
  while pending_inputs:
    input = pending_inputs[-1]
    if input.is_available():
      pending_inputs.pop()
      continue
    for wait_inputs in input.node_iterator:
      wait_inputs = _get_unavailable_input_nodes(wait_inputs)
      if wait_inputs:
        pending_inputs.extend(wait_inputs)
        del wait_inputs  # Save memory.
        break  # APPEND_BREAK.
    else:
      assert input.is_available()
      pending_inputs.pop()


def _run_worker_thread(runnable_queue, report_queue, abort_ary):
  """Takes work from runnable_queue, does work, reports to report_queue."""
  import sys
  try:
    while 1:
      input = None
      if abort_ary:
        break
      input = runnable_queue.get()
      if input is None:  # Indication that the worker thread can stop.
        break
      assert not input.is_available()
      for wait_inputs in input.node_iterator:
        wait_inputs = _get_unavailable_input_nodes(wait_inputs)
        if wait_inputs:
          report_queue.put(('wait', input, wait_inputs))
          del wait_inputs  # Save memory.
          break
      else:  # Node done.
        report_queue.put(('done', input))
  except:
    exc_info = sys.exc_info()  # (exc_type, exc_value, exc_traceback).
    # input can be None.
    report_queue.put(('exc', input, exc_info))
    # Don't continue doing more work. The first exception should stop the
    # graph run.
    return
  report_queue.put(('exit',))


def thread_pool_runner(pool_size):
  """Runs nodes in a thread pool, possibly many at a time."""
  if not isinstance(pool_size, int):
    raise TypeError
  if pool_size < 1:
    raise ValueError('Expected positive thread pool size, got: %d' % pool_size)

  def thread_pool_runner_run(pending_inputs):
    import sys
    import thread
    import Queue

    # Maps nodes to list of nodes they are blocked on.
    # TODO(pts): Use faster value types than lists.
    blocked_nodes = {}
    # Maps nodes to list of nodes they are blocking.
    # TODO(pts): Use faster value types than lists.
    blockings = {}
    # Contains nodes which are either runnable or being run by a worker thread.
    nonblocked_nodes = set(pending_inputs)

    abort_ary = []  # Workers threads ignore the queue if this is not empty.
    runnable_queue = Queue.Queue()
    report_queue = Queue.Queue()
    active_worker_thread_count = pool_size
    for _ in xrange(pool_size):
      thread.start_new_thread(
          _run_worker_thread, (runnable_queue, report_queue, abort_ary))

    for node in pending_inputs:
      runnable_queue.put(node)
    del node, pending_inputs  # Save memory.

    try:
      while nonblocked_nodes:
        assert active_worker_thread_count
        # !! Allow Ctrl-<C> to abort, doesn't work on Queue.Queue.get(),
        # Also below. Solution: run .get() in another, non-main thread.
        item = report_queue.get()
        if item[0] == 'exc':
          active_worker_thread_count -= 1
          # No need to print this, simple_runner doesn't print it either.
          #if item[1] is not None:
          #  import sys
          #  sys.stderr.write('Exception in node: %s\n' % item[1].name)
          e = item[2]
          raise e[0], e[1], e[2]
        elif item[0] == 'wait':
          node, blockers = item[1], item[2]
          assert node in nonblocked_nodes
          assert node not in blocked_nodes
          assert not node.is_available()
          nonblocked_nodes.remove(node)
          blocked_nodes[node] = blockers = list(blockers)
          for blocker_node in blockers:
            # _get_unavailable_input_nodes() ensures this.
            assert not blocker_node.is_available()
            if blocker_node in blockings:
              assert node not in blockings[blocker_node]  # Slow.
              blockings[blocker_node].append(node)
            else:
              blockings[blocker_node] = [node]
            # Can we have `blocker_node in blocked_nodes' here?
            #   Yes. Let's suppose b needs c; c needs d; e needs c;
            #   we need b and e; b executes first; b starts waiting for c; c
            #   executes; c starts waiting dor d; e executes; e starts waiting
            #   for c now. Now c is blocker_node, and c is blocked.
            # Can we have `blocker_node in nonblocked_nodes' here?
            #   Yes. Let's suppose b needs c; e needs c; we need b and e;
            #   b executes first; b starts waiting for c; c executes slowly;
            #   in another thread e executes; e starts waiting for c now.
            #   Now c is blocker_node and c is still executing, thus it's in
            #   nonblocked_nodes.
            if (blocker_node not in blocked_nodes and
                blocker_node not in nonblocked_nodes):
              nonblocked_nodes.add(blocker_node)
              runnable_queue.put(blocker_node)
          del node, blocker_node, blockers  # Save memory.
        elif item[0] == 'done':
          node = item[1]
          assert node in nonblocked_nodes
          assert node not in blocked_nodes
          assert node.is_available()
          nonblocked_nodes.remove(node)
          blocking = blockings.pop(node, None)
          if blocking:
            for blocked_node in blocking:
              assert blocked_node in blocked_nodes
              blockers = blocked_nodes[blocked_node]
              blockers.remove(node)
              if not blockers:
                del blocked_nodes[blocked_node]
                assert blocked_node not in nonblocked_nodes
                #assert blocked_node in blocked_nodes  # Just deleted.
                assert not blocked_node.is_available()
                nonblocked_nodes.add(blocked_node)
                runnable_queue.put(blocked_node)
            blocked_node = None  # Save memory.
          del node, blocking  # Save memory.
        elif item[0] == 'exit':
          assert 0, 'Unexpected early exit of worker thread.'
          active_worker_thread_count -= 1
        else:
          assert 0, 'Unknown report type: %r' % (item[0],)
        del item  # Save memory.
    finally:
      abort_ary.append(1)  # This signals busy worker threads.
      for _ in xrange(pool_size):
        # This signals worker threads waiting for more work.
        runnable_queue.put(None)
      while active_worker_thread_count > 0:
        item = report_queue.get()
        if item[0] in ('exc', 'exit'):
          active_worker_thread_count -= 1
      # Unfortunately Python doesn't let us wait for the thread exit. But it
      # will happen very soon, because _run_worker_thread returns shortly after
      # sending an 'exc' or an 'exit'.

    if blocked_nodes or blockings:
      raise RuntimeError('Unexpectedly blocked nodes in the end.')

  return thread_pool_runner_run


def run_graph(inputs, context=None, runner=None):
  """Makes sure all inputs are available.

  run_graph is idempotent, it doesn't rerun already computed nodes.

  Returns:
    inputs converted to tuple, all available.
  """
  if context is None:
    context = {}
  if runner is None:
    runner = simple_runner
  elif not isinstance(context, dict):
    raise TypeError('context must be a dict.')
  if isinstance(inputs, Input):
    inputs = (inputs,)
  else:
    inputs = tuple(inputs)
  pending_inputs = []
  for input in inputs:
    if not isinstance(input, Input):
      raise TypeError(input)
    if not input.is_available():
      # This subclass is needed by run_graph because it uses the
      # .node_iterator property.
      if not isinstance(input, NodeBase):
      #if not getattr(input, 'node_iterator', None):
        raise TypeError('Node class expected, got: %r' % type(input))
      pending_inputs.append(input)

  nodes = _find_all_nodes(pending_inputs)
  _rename_nodes(nodes)
  print 'All nodes: %r' % [node.name for node in nodes]
  _fix_context_inputs(nodes)
  _add_context_to_node_inputs(nodes, context)
  del nodes  # Save memory.

  # The runner may modify the pending_inputs list in place.
  runner(pending_inputs=pending_inputs)

  return inputs


def recipe(*args, **kwargs):
  """Annotation on functions and generators to create Recipe objects."""
  # !! doc: All *args are of type Input.
  if kwargs or not args:
    if args:
      raise ValueError
    return lambda f: Recipe(f, **kwargs)
  else:
    if len(args) != 1:
      raise ValueError
    return Recipe(args[0])


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


def build_acr_graph():
  a = 5
  b = ConstantInput(7)
  _, c = next_fib.node(a, b)
  area_ab = area.node(a, b)
  circumference_ab = circumference.node(a, b)
  return cond.node(c, area_ab, circumference_ab)


def test_main():
  print area
  print area(5, 6)
  #  Simple call without a graph, for unit tests.
  assert area(ConstantInput(5), 6) == 30
  assert cond(0, 7, 8) == 8
  assert or_all(False, (), [], 33, 0, 44, 0.0) == 33

  a = 5  # !! Reuse implicit ConstantInput objects within the graph?
  b = ConstantInput(7)
  abn = (a, b)
  abn = next_fib.node(*abn)
  abn = next_fib.node(*abn)
  abn = next_fib.node(*abn)
  rgv = run_graph((abn, b))
  print rgv
  assert len(rgv) == 2
  assert rgv[0].get() == (19, 31)
  assert rgv[1] is b

  assert run_graph(next_fib.node(20, 30)[1])[0].get() == 50
  assert next_fib.node(20, 30)[1].run() == 50
  assert next_fib.node(20, 30).run() == (30, 50)

  acr = build_acr_graph()
  run_graph((acr,))
  assert acr.is_available()
  assert acr.get() == 35

  acr = build_acr_graph()
  run_graph((acr,), runner=thread_pool_runner(1))
  assert acr.is_available()
  assert acr.get() == 35

  acr = build_acr_graph()
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

  print 'All OK.'


if __name__ == '__main__':
  test_main()
