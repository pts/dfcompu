#! /usr/bin/python
# by pts@fazekas.hu at Mon May 22 15:33:44 CEST 2017
#
# !! python2.4
# TODO(pts): Exception handling, error propagation.
# TODO(pts): Produce individual results incrementally?
# TODO(pts): Document mutability.
# TODO(pts): Add multithreaded (thread pool, thread-safe) run_graph.
# TODO(pts): Add graph context.
# TODO(pts): Add execution context.
# TODO(pts): Add printing the graph and peeking.
# TODO(pts): How to delete values early during graph execution?
#


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


def convert_function_to_generator(f, arg_names):
  def generator(*args):
    args2 = []
    for i, arg in enumerate(args):
      if i >= len(arg_names) or arg_names[i].endswith('_input'):
        args2.append(arg)
      else:
        yield arg.wait()  # !! Wait for multiple events at the same time.
        args2.append(arg.get())
    del args  # Save memory.
    yield f(*args2)
  generator.func_name = f.func_name
  return generator


class Recipe(object):
  __slots__ = ('generator', 'arg_names', 'result_names', 'has_varargs')

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

  def __repr__(self):
    return 'Recipe(name=%r, %s)' % (
        self.generator.func_name,
        ', '.join('%s=%r' % (k, getattr(self, k)) for k in self.__slots__
                  if k != 'generator'))

  def node(self, *args, **kwargs):
    return Node(self, self._prepare_args(*args, **kwargs))

  def __call__(self, *args, **kwargs):
    return run_graph(
        (Node(self, self._prepare_args(*args, **kwargs)),))[0].result

  def _prepare_args(self, *args, **kwargs):
    if args:
      if kwargs:
        raise ValueError('Both *args and **kwargs specified.')
      args = list(args)
    else:
      order_dict = dict((v, k) for k, v in enumerate(self.arg_names))
      args = [None] * len(self.arg_names)
      for k,v in kwargs.iteritems():
        args[order_dict[k]] = v
    for i, arg in enumerate(args):
      if not isinstance(arg, Input):
        args[i] = ConstantInput(arg)
    if ((not self.has_varargs and len(args) != len(self.arg_names)) or
        (self.has_varargs and len(args) < len(self.arg_names))):
      raise ValueError('Recipe to be called with wrong number of arguments.')
    return args


class NodeBase(Input):
  __slots__ = ()


class NodeSubresultInput(NodeBase):
  __slots__ = ('node', 'i')
  def __init__(self, node, i):
    self.node = node
    self.i = i
  def __repr__(self):
    return 'NodeSubResultInput(node=%r, i=%d)' % (self.node, self.i)
  def get(self):
    return self.node.get()[self.i]
  def is_available(self):
    return self.node.is_available()
  def wait(self):
    return self.node.wait()
  def run(self):  # Convenience method, same interface as Node.
    return run_graph((self,))[0].get()
  @property  # Same interface as Node, for run_graph.
  def node_iterator(self):
    return self.node.node_iterator


class Node(NodeBase):
  __slots__ = ('result', 'has_result', 'recipe', 'inputs', 'node_iterator')

  def __init__(self, recipe, inputs):
    if not isinstance(recipe, Recipe):
      raise TypeError
    for input in inputs:
      if not isinstance(input, Input):
        raise TypeError
    if ((not recipe.has_varargs and len(inputs) != len(recipe.arg_names)) or
        (recipe.has_varargs and len(inputs) < len(recipe.arg_names))):
      raise ValueError('Recipe Node with wrong number of arguments.')
    self.result = None
    self.has_result = False
    self.recipe = recipe
    self.inputs = inputs
    # !! There is a circular reference here?!
    self.node_iterator = self.wrap_node_iterator(
        self.recipe.generator(*inputs))

  def __repr__(self):
    # TODO(pts): Display inputs, detect cycles.
    return (
        'Node(recipe=%r, has_result=%r, result=%r, '
        'inputs=#%d, results=#%d)' %
        (self.recipe, self.has_result, self.result, len(self.inputs),
         len(self.recipe.result_names)))

  def run(self):
    """Convenience method to call run_graph."""
    return run_graph((self,))[0].get()

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
    self.has_result = True
    self.result = result
    self.node_iterator = None

  def wrap_node_iterator(self, iterator):
    for value in iterator:
      if self.has_result:
        raise RuntimeError('Multiple values yielded by recipe iterator.')
      if isinstance(value, Wait):
        yield value.inputs
      elif isinstance(value, Input):
        yield value.wait().inputs
        self.set_result(value.get())
      else:
        self.set_result(value)

  def __len__(self):
    return len(self.recipe.result_names)

  # Used by x, y, ... = foo.node(...)
  def __getitem__(self, i):
    # Creating a NodeSubresultInput on the fly, to avoid circular references.
    # TODO(pts): Use a weakref.
    if not (0 <= i < len(self.recipe.result_names)):
      raise IndexError
    return NodeSubresultInput(self, i)


def run_graph(inputs):
  """Makes sure all inputs are available.

  Returns:
    inputs converted to tuple, all available.
  """
  if isinstance(inputs, Input):
    inputs = (inputs,)
  else:
    inputs = tuple(inputs)
  unavailable_inputs = [] 
  for input in inputs:
    if not isinstance(input, Input):
      raise TypeError(input)
    if not input.is_available():
      # This subclass is needed by run_graph because it uses the
      # .node_iterator property.
      if not isinstance(input, NodeBase):
      #if not getattr(input, 'node_iterator', None):
        raise TypeError('Node class expected, got: %r' % type(input))
      unavailable_inputs.append(input)
      
  if unavailable_inputs:
    waits = [unavailable_inputs]
    while waits:
      inputs1 = waits[-1]
      while inputs1:
        if inputs1[-1].is_available():
          inputs1.pop()
          break
        for wait_inputs in inputs1[-1].node_iterator:
          if wait_inputs:
            wait_inputs = [wait_input for wait_input in wait_inputs if
                           not wait_input.is_available()]
            if wait_inputs:
              for wait_input in wait_inputs:
                if not isinstance(wait_input, NodeBase):
                  raise TypeError('Node class expected, got: %r' %
                                  type(wait_input))
              del wait_input  # Save memory.
              waits.append(wait_inputs)
              break  # APPEND_BREAK.
        else:
          wait_inputs = None
          assert inputs1[-1].is_available()
          inputs1.pop()
        if wait_inputs:
          break  # Break one more for APPEND_BREAK.
      if inputs1:  # Continue from APPEND_BREAK.
        continue
      waits.pop()
  return inputs


def recipe(*args, **kwargs):
  """Annotation on functions and generators to create Recipe objects."""
  # !! doc: All *args are of type Input.
  if kwargs:
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


@recipe
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


if __name__ == '__main__':
  print area
  print area(5, 6)
  #  Simple call without a graph, for unit tests.
  assert area(ConstantInput(5), 6) == 30
  assert cond(0, 7, 8) == 8
  assert or_all(False, (), [], 33, 0, 44, 0.0) == 33

  a = 5  # !! Reuse ConstantInput objects within the graph?
  b = ConstantInput(7)  # !!
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
  
  _, c = next_fib.node(a, b)
  area_ab = area.node(a, b)
  circumference_ab = circumference.node(a, b)
  acr = cond.node(c, area_ab, circumference_ab)
  
  run_graph((acr,))
  assert acr.is_available()
  assert acr.get() == 35

  print 'All OK.'
