"""
An ndarray subclass for working with arrays of strings.
"""

from functools import partial, total_ordering
from operator import eq, ne
import re

import numpy as np
from numpy import ndarray
import pandas as pd
from toolz import compose

from ziplime.utils.compat import unicode
from ziplime.utils.functional import instance
from ziplime.utils.numpy_utils import (
    bool_dtype,
    unsigned_int_dtype_with_size_in_bytes,
    is_object,
    object_dtype,
)
from ziplime.utils.pandas_utils import ignore_pandas_nan_categorical_warning

from ziplime.lib.factorize import (
    factorize_strings,
    factorize_strings_known_categories,
    smallest_uint_that_can_hold,
)


def compare_arrays(left, right):
    "Eq check with a short-circuit for identical objects."
    return left is right or ((left.shape == right.shape) and (left == right).all())


def _make_unsupported_method(name):
    def method(*args, **kwargs):
        raise NotImplementedError("Method %s is not supported on LabelArrays." % name)

    method.__name__ = name
    method.__doc__ = "Unsupported LabelArray Method: %s" % name
    return method


class MissingValueMismatch(ValueError):
    """
    Error raised on attempt to perform operations between LabelArrays with
    mismatched missing_values.
    """

    def __init__(self, left, right):
        super(MissingValueMismatch, self).__init__(
            "LabelArray missing_values don't match:"
            " left={}, right={}".format(left, right)
        )


class CategoryMismatch(ValueError):
    """
    Error raised on attempt to perform operations between LabelArrays with
    mismatched category arrays.
    """

    def __init__(self, left, right):
        (mismatches,) = np.where(left != right)
        assert len(mismatches), "Not actually a mismatch!"
        super(CategoryMismatch, self).__init__(
            "LabelArray categories don't match:\n"
            "Mismatched Indices: {mismatches}\n"
            "Left: {left}\n"
            "Right: {right}".format(
                mismatches=mismatches,
                left=left[mismatches],
                right=right[mismatches],
            )
        )


_NotPassed = "_NotPassed"


class LabelArray(ndarray):
    """
    An ndarray subclass for working with arrays of strings.

    Factorizes the input array into integers, but overloads equality on strings
    to check against the factor label.

    Parameters
    ----------
    values : array-like
        Array of values that can be passed to np.asarray with dtype=object.
    missing_value : str
        Scalar value to treat as 'missing' for operations on ``self``.
    categories : list[str], optional
        List of values to use as categories.  If not supplied, categories will
        be inferred as the unique set of entries in ``values``.
    sort : bool, optional
        Whether to sort categories.  If sort is False and categories is
        supplied, they are left in the order provided.  If sort is False and
        categories is None, categories will be constructed in a random order.

    Attributes
    ----------
    categories : ndarray[str]
        An array containing the unique labels of self.
    reverse_categories : dict[str -> int]
        Reverse lookup table for ``categories``. Stores the index in
        ``categories`` at which each entry each unique entry is found.
    missing_value : str or None
        A sentinel missing value with NaN semantics for comparisons.

    Notes
    -----
    Consumers should be cautious when passing instances of LabelArray to numpy
    functions.  We attempt to disallow as many meaningless operations as
    possible, but since a LabelArray is just an ndarray of ints with some
    additional metadata, many numpy functions (for example, trigonometric) will
    happily accept a LabelArray and treat its values as though they were
    integers.

    In a future change, we may be able to disallow more numerical operations by
    creating a wrapper dtype which doesn't register an implementation for most
    numpy ufuncs. Until that change is made, consumers of LabelArray should
    assume that it is undefined behavior to pass a LabelArray to any numpy
    ufunc that operates on semantically-numerical data.

    See Also
    --------
    https://docs.scipy.org/doc/numpy-1.11.0/user/basics.subclassing.html
    """

    SUPPORTED_SCALAR_TYPES = (bytes, unicode, type(None))
    SUPPORTED_NON_NONE_SCALAR_TYPES = (bytes, unicode)

    # @preprocess(
    #     values=coerce(list, partial(np.asarray, dtype=object)),
    #     # Coerce ``list`` to ``list`` to make a copy. Code internally may call
    #     # ``categories.insert(0, missing_value)`` which will mutate this list
    #     # in place.
    #     categories=coerce((list, np.ndarray, set), list),
    # )
    def __new__(cls, values: np.ndarray, missing_value: SUPPORTED_SCALAR_TYPES, categories: list|None=None, sort=True):

        # Numpy's fixed-width string types aren't very efficient. Working with
        # object arrays is faster than bytes or unicode arrays in almost all
        # cases.
        if not is_object(values):
            values = values.astype(object)

        if values.flags.f_contiguous:
            ravel_order = "F"
        else:
            ravel_order = "C"

        if categories is None:
            codes, categories, reverse_categories = factorize_strings(
                values.ravel(ravel_order),
                missing_value=missing_value,
                sort=sort,
            )
        else:
            (
                codes,
                categories,
                reverse_categories,
            ) = factorize_strings_known_categories(
                values.ravel(ravel_order),
                categories=categories,
                missing_value=missing_value,
                sort=sort,
            )
        categories.setflags(write=False)

        return cls.from_codes_and_metadata(
            codes=codes.reshape(values.shape, order=ravel_order),
            categories=categories,
            reverse_categories=reverse_categories,
            missing_value=missing_value,
        )

    @classmethod
    def from_codes_and_metadata(
        cls, codes, categories, reverse_categories, missing_value
    ):
        """
        Rehydrate a LabelArray from the codes and metadata.

        Parameters
        ----------
        codes : np.ndarray[integral]
            The codes for the label array.
        categories : np.ndarray[object]
            The unique string categories.
        reverse_categories : dict[str, int]
            The mapping from category to its code-index.
        missing_value : any
            The value used to represent missing data.
        """
        ret = codes.view(type=cls, dtype=np.void)
        ret._categories = categories
        ret._reverse_categories = reverse_categories
        ret._missing_value = missing_value
        return ret

    @classmethod
    def from_categorical(cls, categorical, missing_value=None):
        """
        Create a LabelArray from a pandas categorical.

        Parameters
        ----------
        categorical : pd.Categorical
            The categorical object to convert.
        missing_value : bytes, unicode, or None, optional
            The missing value to use for this LabelArray.

        Returns
        -------
        la : LabelArray
            The LabelArray representation of this categorical.
        """
        return LabelArray(
            categorical,
            missing_value,
            categorical.categories,
        )

    @property
    def categories(self):
        # This is a property because it should be immutable.
        return self._categories

    @property
    def reverse_categories(self):
        # This is a property because it should be immutable.
        return self._reverse_categories

    @property
    def missing_value(self):
        # This is a property because it should be immutable.
        return self._missing_value

    @property
    def missing_value_code(self):
        return self.reverse_categories[self.missing_value]

    def has_label(self, value):
        return value in self.reverse_categories

    def __array_finalize__(self, obj):
        """
        Called by Numpy after array construction.

        There are three cases where this can happen:

        1. Someone tries to directly construct a new array by doing::

            >>> ndarray.__new__(LabelArray, ...)  # doctest: +SKIP

           In this case, obj will be None.  We treat this as an error case and
           fail.

        2. Someone (most likely our own __new__) does::

           >>> other_array.view(type=LabelArray)  # doctest: +SKIP

           In this case, `self` will be the new LabelArray instance, and
           ``obj` will be the array on which ``view`` is being called.

           The caller of ``obj.view`` is responsible for setting category
           metadata on ``self`` after we exit.

        3. Someone creates a new LabelArray by slicing an existing one.

           In this case, ``obj`` will be the original LabelArray.  We're
           responsible for copying over the parent array's category metadata.
        """
        if obj is None:
            raise TypeError("Direct construction of LabelArrays is not supported.")

        # See docstring for an explanation of when these will or will not be
        # set.
        self._categories = getattr(obj, "categories", None)
        self._reverse_categories = getattr(obj, "reverse_categories", None)
        self._missing_value = getattr(obj, "missing_value", None)

    def as_int_array(self):
        """
        Convert self into a regular ndarray of ints.

        This is an O(1) operation. It does not copy the underlying data.
        """
        return self.view(
            type=ndarray,
            dtype=unsigned_int_dtype_with_size_in_bytes(self.itemsize),
        )

    def as_string_array(self):
        """
        Convert self back into an array of strings.

        This is an O(N) operation.
        """
        return self.categories[self.as_int_array()]

    def as_categorical(self):
        """
        Coerce self into a pandas categorical.

        This is only defined on 1D arrays, since that's all pandas supports.
        """
        if len(self.shape) > 1:
            raise ValueError("Can't convert a 2D array to a categorical.")

        with ignore_pandas_nan_categorical_warning():
            return pd.Categorical.from_codes(
                self.as_int_array(),
                # We need to make a copy because pandas >= 0.17 fails if this
                # buffer isn't writeable.
                self.categories.copy(),
                ordered=False,
            )

    def as_categorical_frame(self, index, columns, name=None):
        """
        Coerce self into a pandas DataFrame of Categoricals.
        """
        if len(self.shape) != 2:
            raise ValueError("Can't convert a non-2D LabelArray into a DataFrame.")

        expected_shape = (len(index), len(columns))
        if expected_shape != self.shape:
            raise ValueError(
                "Can't construct a DataFrame with provided indices:\n\n"
                "LabelArray shape is {actual}, but index and columns imply "
                "that shape should be {expected}.".format(
                    actual=self.shape,
                    expected=expected_shape,
                )
            )

        return pd.Series(
            index=pd.MultiIndex.from_product([index, columns]),
            data=self.ravel().as_categorical(),
            name=name,
        ).unstack()

    def __setitem__(self, indexer, value):
        self_categories = self.categories

        if isinstance(value, self.SUPPORTED_SCALAR_TYPES):
            value_code = self.reverse_categories.get(value, None)
            if value_code is None:
                raise ValueError("%r is not in LabelArray categories." % value)
            self.as_int_array()[indexer] = value_code
        elif isinstance(value, LabelArray):
            value_categories = value.categories
            if compare_arrays(self_categories, value_categories):
                return super(LabelArray, self).__setitem__(indexer, value)
            elif self.missing_value == value.missing_value and set(
                value.categories
            ) <= set(self.categories):
                rhs = LabelArray.from_codes_and_metadata(
                    *factorize_strings_known_categories(
                        value.as_string_array().ravel(),
                        list(self.categories),
                        self.missing_value,
                        False,
                    ),
                    missing_value=self.missing_value,
                ).reshape(value.shape)
                super(LabelArray, self).__setitem__(indexer, rhs)
            else:
                raise CategoryMismatch(self_categories, value_categories)
        else:
            raise NotImplementedError(
                "Setting into a LabelArray with a value of "
                "type {type} is not yet supported.".format(
                    type=type(value).__name__,
                ),
            )

    def set_scalar(self, indexer, value):
        """
        Set scalar value into the array.

        Parameters
        ----------
        indexer : any
            The indexer to set the value at.
        value : str
            The value to assign at the given locations.

        Raises
        ------
        ValueError
            Raised when ``value`` is not a value element of this this label
            array.
        """
        try:
            value_code = self.reverse_categories[value]
        except KeyError as exc:
            raise ValueError("%r is not in LabelArray categories." % value) from exc

        self.as_int_array()[indexer] = value_code

    def __getitem__(self, indexer):
        result = super(LabelArray, self).__getitem__(indexer)
        if result.ndim:
            # Result is still a LabelArray, so we can just return it.
            return result

        # Result is a scalar value, which will be an instance of np.void.
        # Map it back to one of our category entries.
        index = result.view(
            unsigned_int_dtype_with_size_in_bytes(self.itemsize),
        )
        return self.categories[index]

    def is_missing(self):
        """
        Like isnan, but checks for locations where we store missing values.
        """
        return self.as_int_array() == self.reverse_categories[self.missing_value]

    def not_missing(self):
        """
        Like ~isnan, but checks for locations where we store missing values.
        """
        return self.as_int_array() != self.reverse_categories[self.missing_value]

    def _equality_check(op):
        """
        Shared code for __eq__ and __ne__, parameterized on the actual
        comparison operator to use.
        """

        def method(self, other):

            if isinstance(other, LabelArray):
                self_mv = self.missing_value
                other_mv = other.missing_value
                if self_mv != other_mv:
                    raise MissingValueMismatch(self_mv, other_mv)

                self_categories = self.categories
                other_categories = other.categories
                if not compare_arrays(self_categories, other_categories):
                    raise CategoryMismatch(self_categories, other_categories)

                return (
                    op(self.as_int_array(), other.as_int_array())
                    & self.not_missing()
                    & other.not_missing()
                )

            elif isinstance(other, ndarray):
                # Compare to ndarrays as though we were an array of strings.
                # This is fairly expensive, and should generally be avoided.
                return op(self.as_string_array(), other) & self.not_missing()

            elif isinstance(other, self.SUPPORTED_SCALAR_TYPES):
                i = self._reverse_categories.get(other, -1)
                return op(self.as_int_array(), i) & self.not_missing()

            return op(super(LabelArray, self), other)

        return method

    __eq__ = _equality_check(eq)
    __ne__ = _equality_check(ne)
    del _equality_check

    def view(self, dtype=_NotPassed, type=_NotPassed):
        if type is _NotPassed and dtype not in (_NotPassed, self.dtype):
            raise TypeError("Can't view LabelArray as another dtype.")

        # The text signature on ndarray.view makes it look like the default
        # values for dtype and type are `None`, but passing None explicitly has
        # different semantics than not passing an arg at all, so we reconstruct
        # the kwargs dict here to simulate the args not being passed at all.
        kwargs = {}
        if dtype is not _NotPassed:
            kwargs["dtype"] = dtype
        if type is not _NotPassed:
            kwargs["type"] = type
        return super(LabelArray, self).view(**kwargs)

    def astype(self, dtype, order="K", casting="unsafe", subok=True, copy=True):
        if dtype == self.dtype:
            if not subok:
                array = self.view(type=np.ndarray)
            else:
                array = self

            if copy:
                return array.copy()
            return array

        if dtype == object_dtype:
            return self.as_string_array()

        if dtype.kind == "S":
            return self.as_string_array().astype(
                dtype,
                order=order,
                casting=casting,
                subok=subok,
                copy=copy,
            )

        raise TypeError(
            "%s can only be converted into object, string, or void,"
            " got: %r"
            % (
                type(self).__name__,
                dtype,
            ),
        )

    # In general, we support resizing, slicing, and reshaping methods, but not
    # numeric methods.
    SUPPORTED_NDARRAY_METHODS = frozenset(
        [
            "astype",
            "base",
            "compress",
            "copy",
            "data",
            "diagonal",
            "dtype",
            "flat",
            "flatten",
            "item",
            "itemset",
            "itemsize",
            "nbytes",
            "ndim",
            "ravel",
            "repeat",
            "reshape",
            "resize",
            "setflags",
            "shape",
            "size",
            "squeeze",
            "strides",
            "swapaxes",
            "take",
            "trace",
            "transpose",
            "view",
        ]
    )
    PUBLIC_NDARRAY_METHODS = frozenset(
        [s for s in dir(ndarray) if not s.startswith("_")]
    )

    # Generate failing wrappers for all unsupported methods.
    locals().update(
        {
            method: _make_unsupported_method(method)
            for method in PUBLIC_NDARRAY_METHODS - SUPPORTED_NDARRAY_METHODS
        }
    )

    def __repr__(self):
        repr_lines = repr(self.as_string_array()).splitlines()
        repr_lines[0] = repr_lines[0].replace("array(", "LabelArray(", 1)
        repr_lines[-1] = repr_lines[-1].rsplit(",", 1)[0] + ")"
        # The extra spaces here account for the difference in length between
        # 'array(' and 'LabelArray('.
        return "\n     ".join(repr_lines)

    def empty_like(self, shape):
        """
        Make an empty LabelArray with the same categories as ``self``, filled
        with ``self.missing_value``.
        """
        return type(self).from_codes_and_metadata(
            codes=np.full(
                shape,
                self.reverse_categories[self.missing_value],
                dtype=unsigned_int_dtype_with_size_in_bytes(self.itemsize),
            ),
            categories=self.categories,
            reverse_categories=self.reverse_categories,
            missing_value=self.missing_value,
        )

    def map_predicate(self, f):
        """
        Map a function from str -> bool element-wise over ``self``.

        ``f`` will be applied exactly once to each non-missing unique value in
        ``self``. Missing values will always return False.
        """
        # Functions passed to this are of type str -> bool.  Don't ever call
        # them on None, which is the only non-str value we ever store in
        # categories.
        if self.missing_value is None:

            def f_to_use(x):
                return False if x is None else f(x)

        else:
            f_to_use = f

        # Call f on each unique value in our categories.
        results = np.vectorize(f_to_use, otypes=[bool_dtype])(self.categories)

        # missing_value should produce False no matter what
        results[self.reverse_categories[self.missing_value]] = False

        # unpack the results form each unique value into their corresponding
        # locations in our indices.
        return results[self.as_int_array()]

    def map(self, f):
        """
        Map a function from str -> str element-wise over ``self``.

        ``f`` will be applied exactly once to each non-missing unique value in
        ``self``. Missing values will always map to ``self.missing_value``.
        """
        # f() should only return None if None is our missing value.
        if self.missing_value is None:
            allowed_outtypes = self.SUPPORTED_SCALAR_TYPES
        else:
            allowed_outtypes = self.SUPPORTED_NON_NONE_SCALAR_TYPES

        def f_to_use(x, missing_value=self.missing_value, otypes=allowed_outtypes):

            # Don't call f on the missing value; those locations don't exist
            # semantically. We return _sortable_sentinel rather than None
            # because the np.unique call below sorts the categories array,
            # which raises an error on Python 3 because None and str aren't
            # comparable.
            if x == missing_value:
                return _sortable_sentinel

            ret = f(x)

            if not isinstance(ret, otypes):
                raise TypeError(
                    "LabelArray.map expected function {f} to return a string"
                    " or None, but got {type} instead.\n"
                    "Value was {value}.".format(
                        f=f.__name__,
                        type=type(ret).__name__,
                        value=ret,
                    )
                )

            if ret == missing_value:
                return _sortable_sentinel

            return ret

        new_categories_with_duplicates = np.vectorize(f_to_use, otypes=[object])(
            self.categories
        )

        # If f() maps multiple inputs to the same output, then we can end up
        # with the same code duplicated multiple times. Compress the categories
        # by running them through np.unique, and then use the reverse lookup
        # table to compress codes as well.
        new_categories, bloated_inverse_index = np.unique(
            new_categories_with_duplicates, return_inverse=True
        )

        if new_categories[0] is _sortable_sentinel:
            # f_to_use return _sortable_sentinel for locations that should be
            # missing values in our output. Since np.unique returns the uniques
            # in sorted order, and since _sortable_sentinel sorts before any
            # string, we only need to check the first array entry.
            new_categories[0] = self.missing_value

        # `reverse_index` will always be a 64 bit integer even if we can hold a
        # smaller array.
        reverse_index = bloated_inverse_index.astype(
            smallest_uint_that_can_hold(len(new_categories))
        )
        new_codes = np.take(reverse_index, self.as_int_array())

        return self.from_codes_and_metadata(
            new_codes,
            new_categories,
            dict(zip(new_categories, range(len(new_categories)))),
            missing_value=self.missing_value,
        )

    def startswith(self, prefix):
        """
        Element-wise startswith.

        Parameters
        ----------
        prefix : str

        Returns
        -------
        matches : np.ndarray[bool]
            An array with the same shape as self indicating whether each
            element of self started with ``prefix``.
        """
        return self.map_predicate(lambda elem: elem.startswith(prefix))

    def endswith(self, suffix):
        """
        Elementwise endswith.

        Parameters
        ----------
        suffix : str

        Returns
        -------
        matches : np.ndarray[bool]
            An array with the same shape as self indicating whether each
            element of self ended with ``suffix``
        """
        return self.map_predicate(lambda elem: elem.endswith(suffix))

    def has_substring(self, substring):
        """
        Elementwise contains.

        Parameters
        ----------
        substring : str

        Returns
        -------
        matches : np.ndarray[bool]
            An array with the same shape as self indicating whether each
            element of self ended with ``suffix``.
        """
        return self.map_predicate(lambda elem: substring in elem)

    # @preprocess(pattern=coerce(from_=(bytes, unicode), to=re.compile))
    def matches(self, pattern: re.Pattern):
        """
        Elementwise regex match.

        Parameters
        ----------
        pattern : str or compiled regex

        Returns
        -------
        matches : np.ndarray[bool]
            An array with the same shape as self indicating whether each
            element of self was matched by ``pattern``.
        """
        return self.map_predicate(compose(bool, pattern.match))

    # These types all implement an O(N) __contains__, so pre-emptively
    # coerce to `set`.
    #@preprocess(container=coerce((list, tuple, np.ndarray), set))
    def element_of(self, container:set):
        """
        Check if each element of self is an of ``container``.

        Parameters
        ----------
        container : object
            An object implementing a __contains__ to call on each element of
            ``self``.

        Returns
        -------
        is_contained : np.ndarray[bool]
            An array with the same shape as self indicating whether each
            element of self was an element of ``container``.
        """
        return self.map_predicate(container.__contains__)


@instance  # This makes _sortable_sentinel a singleton instance.
@total_ordering
class _sortable_sentinel:
    """Dummy object that sorts before any other python object."""

    def __eq__(self, other):
        return self is other

    def __lt__(self, other):
        return True


def labelarray_where(cond, trues: LabelArray, falses: LabelArray):
    """LabelArray-aware implementation of np.where."""
    if trues.missing_value != falses.missing_value:
        raise ValueError("Can't compute where on arrays with different missing values.")

    strs = np.where(cond, trues.as_string_array(), falses.as_string_array())
    return LabelArray(strs, missing_value=trues.missing_value)
