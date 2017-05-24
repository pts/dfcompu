#! /usr/bin/python
# by pts@fazekas.hu at Wed May 24 18:02:09 CEST 2017

"""Converts the dfcompu source code from Python 2 to Python 3."""

import os
import os.path

replacements = (
    (r'xrange(', r'range('),
    (r'.func_name', r'.__name__'),
    (r'.func_code.', r'.__code__.'),
    ('\'func_code\'', '\'__code__\''),
    (r'raise exc_info[0], exc_info[1], exc_info[2]', r'raise exc_info[1]'),
    (r'import thread', r'import _thread as thread'),
    (r'import Queue', r'import queue as Queue'),
    (r'except RuntimeError, e', r'except RuntimeError as e'),
    (r'except ValueError, e', r'except ValueError as e'),
    (r'.iteritems()', r'.items()'),
)

try:
  os.mkdir('py3')
except OSError:
  pass

do_fix = True
for filename in ('dfcompu.py', 'dfcompu_test.py'):
  data = open(filename, 'rb').read()
  if do_fix:
    if not isinstance(data, str):  # Python 3.
      replacements = [(a.encode('UTF-8'), b.encode('UTF-8'))
                      for a, b in replacements]
      do_fix = False
  for a, b in replacements:
    # TODO(pts): Faster, process only once.
    data = data.replace(a, b)
  open(os.path.join('py3', filename), 'wb').write(data)
