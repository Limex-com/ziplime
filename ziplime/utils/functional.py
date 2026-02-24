from operator import itemgetter
from toolz import curry


@curry
def apply(f, *args, **kwargs):
    """Apply a function to arguments.

    Parameters
    ----------
    f : callable
        The function to call.
    *args, **kwargs
    **kwargs
        Arguments to feed to the callable.

    Returns
    -------
    a : any
        The result of ``f(*args, **kwargs)``

    Examples
    --------
    (2, -1)

    Class decorator
    'f'
    >>> issubclass(obj, object)
    Traceback (most recent call last):
        ...
    TypeError: issubclass() arg 1 must be a class
    >>> isinstance(obj, type)
    False

    See Also
    --------
    unpack_apply
    mapply
    """
    return f(*args, **kwargs)


# Alias for use as a class decorator.
instance = apply


@curry
def set_attribute(name, value):
    """
    Decorator factory for setting attributes on a function.

    Doesn't change the behavior of the wrapped function.

    Examples
    --------
    >>> @set_attribute('__name__', 'foo')
    ... def bar():
    ...     return 3
    ...
    >>> bar()
    3
    >>> bar.__name__
    'foo'
    """

    def decorator(f):
        setattr(f, name, value)
        return f

    return decorator


# Decorators for setting the __name__ and __doc__ properties of a decorated
# function.
# Example:
with_name = set_attribute("__name__")
with_doc = set_attribute("__doc__")


def invert(d):
    """
    Invert a dictionary into a dictionary of sets.

    >>> invert({'a': 1, 'b': 2, 'c': 1})  # doctest: +SKIP
    {1: {'a', 'c'}, 2: {'b'}}
    """
    out = {}
    for k, v in d.items():
        try:
            out[v].add(k)
        except KeyError:
            out[v] = {k}
    return out


def keysorted(d):
    """Get the items from a dict, sorted by key.

    Example
    -------
    # >>> keysorted({'c': 1, 'b': 2, 'a': 3})
    [('a', 3), ('b', 2), ('c', 1)]
    """
    return sorted(d.items(), key=itemgetter(0))
