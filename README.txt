dfcompu: lightweight Python 2 library for data flow computations
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
by pts@fazekas.hu at Wed May 24 17:28:06 CEST 2017

dfcompu is a lightweight Python 2 library for data flow computations. It
lets you define recipes (Python code), build directed acyclic graphs of
recipes, and run the nodes of these graphs in an on-demand order either
directly or in a thread pool, all conveniently, with very little boilerplate
code. In debug mode, dfcompu can remember the input and output values of
all nodes.

Short code example
~~~~~~~~~~~~~~~~~~
Run this in Python 2 (or Python 3 after conversion):

  from dfcompu import recipe

  @recipe
  def sub(a, b):
    return a - b

  @recipe
  def mul(a, b)
    return a * b

  def diffsquare_node(a, b):
    ab = sub.node(a, b)
    return mul.node(ab, ab)

  assert sub.node(5, 7).run() == -2
  assert sub(5, 7) == -2  # Simplified call, for unit tests.
  assert diffsquare_node(5, 7).run() == 4

Introduction
~~~~~~~~~~~~
In dfcompu, a computation happens in a directed acyclic graph whose nodes
contain arbitrary Python code (called recipe), and nodes are
connected to each other on their inputs and outputs, forming a directed
acyclic graph. Each node can have multiple inputs and outputs. Some inputs
are precomputed constants (represented using the ConstantInput class). Some
inputs can be taken from the context dict, which is a dictionary global to
all nodes in the computation, and it's typically prepopulated before the
computation starts.

The same recipe can be reused multiple times in the graph, i.e. forming
multiple nodes. Also an output of a node can used in multiple other nodes.
Because of these reasons, graph construction is seprated from recipe
definitions, because the Python code in the recipe doesn't know where its
inputs come from and where its outputs are going. (This is an essential
property of code reuse in data flow computations.)

Recipes can be defined by adding the `@recipe' annotation to Python function
or generator definitions. Example function:

  from dfcompu import recipe

  @recipe
  def sub(a, b):
    return a - b

By default before a recipe is executed, it waits for all its inputs to be
computed. It's possible to make the recipe use only some of its inputs, by
adding the _input suffix to the argument:

  @recipe
  def cond(condition, true_input, false_input):
    if condition:
      return true_input
    else:
      return false_input

In this case, true_input and false_input are of the type Input, which has
the .is_available(), .get() and .wait() methods. .get() raises an exception
if called before .is_available() becomes true, thus .wait() needs to be
called before .get(). But for .wait() the be used, the recipe must be
defined as a generator rather than a function, like this:

  @recipe
  def sub(a_input, b_input):
    yield a_input.wait()
    yield b_input.wait()
    yield a_input.get() - b_input.get()

Or, equivaletly, only to the `a' argument:

  @recipe
  def sub(a_input, b):
    yield a_input.wait()
    yield a_input.get() - b

It's possible to wait for multiple inputs at the same time:

  from dfcompu import recipe, Wait

  @recipe
  def sub(a_input, b_input):
    yield Wait((a_input, b_input))
    yield a_input.get() - b_input.get()

The generator version of `cond' looks like this:

  @recipe
  def cond(condition, true_input, false_input):
    if condition:
      input = true_input
    else:
      input = false_input
    yield input.wait()
    yield input.get()

If variable number of arguments (*args) are passed to the recipe generator,
they are all considered as Input objects. For example:

  @recipe
  def or_all(*args):
    """Returns the first true value."""
    for arg_input in args:
      yield arg_input.wait()
      value = arg_input.get()
      if value:
        yield value
        break

The `or_all' recipe above is an example of a recipe not using all its
inputs: it will stop at the first true input.

If a recipe returns a tuple, the tuple fields can be named, and the return
value will be converted to a collections.namedtuple. For example, generating
subsequent elements of Fibonacci:

  @recipe(result=('a_next', 'b_next'))
  def next_fib(a, b):
    return b, a + b

TODO(pts): Add more examples, also how to run the computation.

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

FAQ
~~~
Q1. Does dfcompu support Jython, PyPy or any Python implementation other than
    CPython?

    dfcompu works with Jython (tried with jython-standalone-2.7.0.jar):
    
    $ java -jar jython-standalone-2.7.0.jar dfcompu_test.py
    ...
    OK

    dfcompu works with PyPy (tried with pypy2-v5.7.1):

    $ pypy2-v5.7.1-linux64/bin/pypy dfcompu_test.py
    ...
    OK

    The fundamental difference between CPython and Python implementations
    lacking reference counting (e.g. Jython, PyPy and .NET Pythons) is that
    some temporary values will be deleted later than in CPython. This is
    true with or without dfcompu, and dfcompu works nevertheless.

Q2. Does dfcompu support Python 3?

    Yes, here is how to convert dfcompu to Python 3 and run it:
   
      $ python3 genpy3.py
      $ python3 py3/dfcompu_test.py

    The genpy3.py script works both in Python 2 and Python 3.

Q3. Which Python versions does dfcompu support?

    Python 2.4, 2.5, 2.6 and 2.7 and Python 3. See Q2 for how to make it
    work in Python 3.

Q4. Is there a visualization of a graph execution?

    To get debug output, call .run() or run_graph() by passing an empty
    list variable as debug_nodes=, and then print and examine the list.

    A graphical visualization is not implemented yet, feel free to do so.

Q5. Does dfcompu support multiprocess or multihost execution?

    Not yet. The required serialization and shared-memory support is not
    implemented yet.

Q6. Does dfcompu support nodes being blocked on I/O or on a timer?

    Not yet.

Q7. Does dfcompu support passing mutable values between nodes?

    Yes, but you have to make sure that there is no concurrent access
    (except for reading) of those values.

Q8. Can a recipe emit one of its results without emitting all results?

    Not yet.

Q9. Can dfcompu take advantage of all CPU cores on a system?

    Not on CPython or PyPy, because of the global interpreter lock (GIL).
    In these systems dfcompu uses 1 CPU core at most in Python code, no
    matter the thread pool size. On Jython, dfcompu can naturally take
    advantage of multiple CPU cores if .run() or run_graph() is called with
    thread_pool_runner(...) with a large enough pool size.

Q10. Can a dfcompu graph be modified while it's being executed?

     Not yet.

Q11. Can a dfcompu graph execution be done from within a recipe?

     This works with some caveats, but it's not recommended.
     
     For example, the context and thread pools are not propagated, and while
     the outer node waits for the inner graph to execute, it sits idle,
     takes a slot from the outer thread pool etc.

Q12. Can a dfcompu graph contain cycles?

     No.


Q13. Is dfcompu suitable for writing response handlers for HTTP, WSGI or RPC
     requests?

     Yes, it's a great fit, although there is no example code yet.

Q14. Is dfcompu suitable for writing chat systems or chatbots?

     No, because they typically run in an infinite loop, waiting for more
     I/O from the peer, and dfcompu doesn't support this infinite operation.

Q15. Does dfcompu support persistence and checkpointed continuation of
     aborted computations?

     Not yet. Implementing this is hard, and is currently out of scope.

Q16. Does dfcompu support skipping some of the computation based on input?

     Yes, see `def cond(' in dfcompu_test.py.

__END__
