from functools import reduce
from operator import itemgetter
from pprint import pformat
from toolz import curry, flip

from .sentinel import sentinel


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
    >>> fs = add(1), sub(1)
    >>> tuple(map(apply, fs, (1, 2)))
    (2, -1)

    Class decorator
    >>> instance = apply
    >>> @instance
    ... class obj:
    ...     def f(self):
    ...         return 'f'
    ...
    >>> obj.f()
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


def mapall(funcs, seq):
    """
    Parameters
    ----------
    funcs : iterable[function]
        Sequence of functions to map over `seq`.
    seq : iterable
        Sequence over which to map funcs.

    Yields
    ------
    elem : object
        Concatenated result of mapping each ``func`` over ``seq``.

    Examples
    --------
    [2, 3, 4, 0, 1, 2]
    """
    for func in funcs:
        for elem in seq:
            yield func(elem)


def same(*values):
    """
    Check if all values in a sequence are equal.

    Returns True on empty sequences.

    Examples
    --------
    True
    """
    if not values:
        return True
    first, rest = values[0], values[1:]
    return all(value == first for value in rest)


def _format_unequal_keys(dicts):
    return pformat([sorted(d.keys()) for d in dicts])




def _gen_unzip(it, elem_len):
    """Helper for unzip which checks the lengths of each element in it.
    Parameters
    ----------
    it : iterable[tuple]
        An iterable of tuples. ``unzip`` should map ensure that these are
        already tuples.
    elem_len : int or None
        The expected element length. If this is None it is infered from the
        length of the first element.
    Yields
    ------
    elem : tuple
        Each element of ``it``.
    Raises
    ------
    ValueError
        Raised when the lengths do not match the ``elem_len``.
    """
    try:
        elem = next(it)
    except StopIteration:
        return (), ()

    first_elem_len = len(elem)

    if elem_len is not None and elem_len != first_elem_len:
        raise ValueError(
            "element at index 0 was length %d, expected %d"
            % (
                first_elem_len,
                elem_len,
            )
        )
    else:
        elem_len = first_elem_len

    yield elem
    for n, elem in enumerate(it, 1):
        if len(elem) != elem_len:
            raise ValueError(
                "element at index %d was length %d, expected %d"
                % (
                    n,
                    len(elem),
                    elem_len,
                ),
            )
        yield elem


def unzip(seq, elem_len=None):
    """Unzip a length n sequence of length m sequences into m seperate length
    n sequences.
    Parameters
    ----------
    seq : iterable[iterable]
        The sequence to unzip.
    elem_len : int, optional
        The expected length of each element of ``seq``. If not provided this
        will be infered from the length of the first element of ``seq``. This
        can be used to ensure that code like: ``a, b = unzip(seq)`` does not
        fail even when ``seq`` is empty.
    Returns
    -------
    seqs : iterable[iterable]
        The new sequences pulled out of the first iterable.
    Raises
    ------
    ValueError
        Raised when ``seq`` is empty and ``elem_len`` is not provided.
        Raised when elements of ``seq`` do not match the given ``elem_len`` or
        the length of the first element of ``seq``.
    Examples
    --------
    >>> seq = [('a', 1), ('b', 2), ('c', 3)]
    >>> cs, ns = unzip(seq)
    >>> cs
    ('a', 'b', 'c')
    >>> ns
    (1, 2, 3)

    # checks that the elements are the same length
    >>> seq = [('a', 1), ('b', 2), ('c', 3, 'extra')]
    >>> cs, ns = unzip(seq)
    Traceback (most recent call last):
       ...
    ValueError: element at index 2 was length 3, expected 2

    # allows an explicit element length instead of infering
    >>> seq = [('a', 1, 'extra'), ('b', 2), ('c', 3)]
    >>> cs, ns = unzip(seq, 2)
    Traceback (most recent call last):
      ...
    ValueError: element at index 0 was length 3, expected 2

    # handles empty sequences when a length is given
    >>> cs, ns = unzip([], elem_len=2)
    >>> cs == ns == ()
    True

    Notes
    -----
    This function will force ``seq`` to completion.
    """
    ret = tuple(zip(*_gen_unzip(map(tuple, seq), elem_len)))
    if ret:
        return ret

    if elem_len is None:
        raise ValueError("cannot unzip empty sequence without 'elem_len'")
    return ((),) * elem_len


_no_default = sentinel("_no_default")


def getattrs(value, attrs, default=_no_default):
    """
    Perform a chained application of ``getattr`` on ``value`` with the values
    in ``attrs``.

    If ``default`` is supplied, return it if any of the attribute lookups fail.

    Parameters
    ----------
    value : object
        Root of the lookup chain.
    attrs : iterable[str]
        Sequence of attributes to look up.
    default : object, optional
        Value to return if any of the lookups fail.

    Returns
    -------
    result : object
        Result of the lookup sequence.

    Examples
    --------
    >>> class EmptyObject:
    ...     pass
    ...
    >>> obj = EmptyObject()
    >>> obj.foo = EmptyObject()
    >>> obj.foo.bar = "value"
    >>> getattrs(obj, ('foo', 'bar'))
    'value'

    >>> getattrs(obj, ('foo', 'buzz'))
    Traceback (most recent call last):
       ...
    AttributeError: 'EmptyObject' object has no attribute 'buzz'

    >>> getattrs(obj, ('foo', 'buzz'), 'default')
    'default'
    """
    try:
        for attr in attrs:
            value = getattr(value, attr)
    except AttributeError:
        if default is _no_default:
            raise
        value = default
    return value


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


def foldr(f, seq, default=_no_default):
    """Fold a function over a sequence with right associativity.

    Parameters
    ----------
    f : callable[any, any]
        The function to reduce the sequence with.
        The first argument will be the element of the sequence; the second
        argument will be the accumulator.
    seq : iterable[any]
        The sequence to reduce.
    default : any, optional
        The starting value to reduce with. If not provided, the sequence
        cannot be empty, and the last value of the sequence will be used.

    Returns
    -------
    folded : any
        The folded value.

    Notes
    -----
    This functions works by reducing the list in a right associative way.

    For example, imagine we are folding with ``operator.add`` or ``+``:

    .. code-block:: python

       foldr(add, seq) -> seq[0] + (seq[1] + (seq[2] + (...seq[-1], default)))

    In the more general case with an arbitrary function, ``foldr`` will expand
    like so:

    .. code-block:: python

       foldr(f, seq) -> f(seq[0], f(seq[1], f(seq[2], ...f(seq[-1], default))))

    For a more in depth discussion of left and right folds, see:
    `https://en.wikipedia.org/wiki/Fold_(higher-order_function)`_
    The images in that page are very good for showing the differences between
    ``foldr`` and ``foldl`` (``reduce``).

    .. note::

       For performance reasons is is best to pass a strict (non-lazy) sequence,
       for example, a list.

    See Also
    --------
    :func:`functools.reduce`
    :func:`sum`
    """
    return reduce(
        flip(f), reversed(seq), *(default,) if default is not _no_default else ()
    )


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
    >>> keysorted({'c': 1, 'b': 2, 'a': 3})
    [('a', 3), ('b', 2), ('c', 1)]
    """
    return sorted(d.items(), key=itemgetter(0))
