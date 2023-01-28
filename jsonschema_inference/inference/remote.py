import execnet
import inspect
from functools import wraps
exec('from ..schema.objs import *')


def build_remote(gw, function, engine='pypy'):
    func_name = function.__name__
    consumer_str = f"""
{inspect.getsource(function)}
args, kwargs = channel.receive()
channel.send(repr({func_name}(*args, **kwargs)))
"""
    consumer_str = consumer_str.replace(f'@{engine}', '')
    channel = gw.remote_exec(consumer_str)
    return channel


def wrap(func, channel):
    @wraps(func)
    def wrapped_func(*args, **kwargs):
        channel.send((args, kwargs))
        return eval(channel.receive())
    return wrapped_func


def pypy(func):
    gw = execnet.makegateway("popen// pypy3")
    channel = build_remote(gw, func, engine='pypy')
    wrapped_func = wrap(func, channel)
    return gw, wrapped_func


def python(func):
    gw = execnet.makegateway("popen// python")
    channel = build_remote(gw, func, engine='python')
    wrapped_func = wrap(func, channel)
    return gw, wrapped_func
