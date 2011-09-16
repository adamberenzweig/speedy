import logging
import sys

# For some reason, the default implementation of frame fetching for the logger
# does not offset correctly; fix that here.
def currentFrame():
  return sys._getframe(6)

def findCaller():
  f = currentFrame()
  code = f.f_code
  return (code.co_filename, f.f_lineno, code.co_name)

logging.root.findCaller = findCaller
logging.basicConfig(level = logging.INFO,
                    format = '%(asctime)s %(levelname).1s %(filename)s:%(lineno)d %(message)s')


def debug(fmt, *args, **kw):
  logging.debug(fmt % args, **kw)

def info(fmt, *args, **kw):
  logging.info(fmt % args, **kw)

def warn(fmt, *args, **kw):
  logging.warn(fmt % args, **kw)

def error(fmt, *args, **kw):
  logging.error(fmt % args, **kw)

def fatal(fmt, *args, **kw):
  logging.fatal(fmt % args, **kw)

