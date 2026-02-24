from ziplime.errors import UnsupportedDataType
from ziplime.pipeline.dtypes import CLASSIFIER_DTYPES, FACTOR_DTYPES
from ziplime.pipeline.mixins import PositiveWindowLengthMixin, CustomTermMixin
from ziplime.pipeline.terms.filters.filter import Filter


class CustomFilter(PositiveWindowLengthMixin, CustomTermMixin, Filter):
    """
    Base class for user-defined Filters.

    Parameters
    ----------
    inputs : iterable, optional
        An iterable of `BoundColumn` instances (e.g. USEquityPricing.close),
        describing the data to load and pass to ``self.compute``.  If this
        argument is passed to the CustomFilter constructor, we look for a
        class-level attribute named ``inputs``.
    window_length : int, optional
        Number of rows to pass for each input.  If this argument is not passed
        to the CustomFilter constructor, we look for a class-level attribute
        named `window_length`.

    Notes
    -----
    Users implementing their own Filters should subclass CustomFilter and
    implement a method named ``compute`` with the following signature:

    .. code-block:: python

        def compute(self, today, assets, out, *inputs):
           ...

    On each simulation date, ``compute`` will be called with the current date,
    an array of sids, an output array, and an input array for each expression
    passed as inputs to the CustomFilter constructor.

    The specific types of the values passed to ``compute`` are as follows::

        today : np.datetime64[ns]
            Row label for the last row of all arrays passed as `inputs`.
        assets : np.array[int64, ndim=1]
            Column labels for `out` and`inputs`.
        out : np.array[bool, ndim=1]
            Output array of the same shape as `assets`.  `compute` should write
            its desired return values into `out`.
        *inputs : tuple of np.array
            Raw data arrays corresponding to the values of `self.inputs`.

    See the documentation for
    :class:`~ziplime.pipeline.CustomFactor` for more details on
    implementing a custom ``compute`` method.

    See Also
    --------
    ziplime.pipeline.CustomFactor
    """

    def _validate(self):
        try:
            super(CustomFilter, self)._validate()
        except UnsupportedDataType as exc:
            if self.dtype in CLASSIFIER_DTYPES:
                raise UnsupportedDataType(
                    typename=type(self).__name__,
                    dtype=self.dtype,
                    hint="Did you mean to create a CustomClassifier?",
                ) from exc
            elif self.dtype in FACTOR_DTYPES:
                raise UnsupportedDataType(
                    typename=type(self).__name__,
                    dtype=self.dtype,
                    hint="Did you mean to create a CustomFactor?",
                ) from exc
            raise

