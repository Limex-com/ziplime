from ziplime.pipeline.terms.factors.factor import Factor
from ziplime.pipeline.terms.factors.recarray_field import RecarrayField
from ziplime.errors import UnsupportedDataType
from ziplime.pipeline.dtypes import CLASSIFIER_DTYPES, FILTER_DTYPES

from ziplime.pipeline.mixins import CustomTermMixin, PositiveWindowLengthMixin

from ziplime.utils.numpy_utils import float64_dtype


class CustomFactor(PositiveWindowLengthMixin, CustomTermMixin, Factor):
    '''
    Base class for user-defined Factors.

    Parameters
    ----------
    inputs : iterable, optional
        An iterable of `BoundColumn` instances (e.g. USEquityPricing.close),
        describing the data to load and pass to `self.compute`.  If this
        argument is not passed to the CustomFactor constructor, we look for a
        class-level attribute named `inputs`.
    outputs : iterable[str], optional
        An iterable of strings which represent the names of each output this
        factor should compute and return. If this argument is not passed to the
        CustomFactor constructor, we look for a class-level attribute named
        `outputs`.
    window_length : int, optional
        Number of rows to pass for each input.  If this argument is not passed
        to the CustomFactor constructor, we look for a class-level attribute
        named `window_length`.
    mask : ziplime.pipeline.Filter, optional
        A Filter describing the assets on which we should compute each day.
        Each call to ``CustomFactor.compute`` will only receive assets for
        which ``mask`` produced True on the day for which compute is being
        called.

    Notes
    -----
    Users implementing their own Factors should subclass CustomFactor and
    implement a method named `compute` with the following signature:

    .. code-block:: python

        def compute(self, today, assets, out, *inputs):
           ...

    On each simulation date, ``compute`` will be called with the current date,
    an array of sids, an output array, and an input array for each expression
    passed as inputs to the CustomFactor constructor.

    The specific types of the values passed to `compute` are as follows::

        today : np.datetime64[ns]
            Row label for the last row of all arrays passed as `inputs`.
        assets : np.array[int64, ndim=1]
            Column labels for `out` and`inputs`.
        out : np.array[self.dtype, ndim=1]
            Output array of the same shape as `assets`.  `compute` should write
            its desired return values into `out`. If multiple outputs are
            specified, `compute` should write its desired return values into
            `out.<output_name>` for each output name in `self.outputs`.
        *inputs : tuple of np.array
            Raw data arrays corresponding to the values of `self.inputs`.

    ``compute`` functions should expect to be passed NaN values for dates on
    which no data was available for an asset.  This may include dates on which
    an asset did not yet exist.

    For example, if a CustomFactor requires 10 rows of close price data, and
    asset A started trading on Monday June 2nd, 2014, then on Tuesday, June
    3rd, 2014, the column of input data for asset A will have 9 leading NaNs
    for the preceding days on which data was not yet available.

    Examples
    --------

    A CustomFactor with pre-declared defaults:

    .. code-block:: python

        class TenDayRange(CustomFactor):
            """
            Computes the difference between the highest high in the last 10
            days and the lowest low.

            Pre-declares high and low as default inputs and `window_length` as
            10.
            """

            inputs = [USEquityPricing.high, USEquityPricing.low]
            window_length = 10

            def compute(self, today, assets, out, highs, lows):
                from numpy import nanmin, nanmax

                highest_highs = nanmax(highs, axis=0)
                lowest_lows = nanmin(lows, axis=0)
                out[:] = highest_highs - lowest_lows


        # Doesn't require passing inputs or window_length because they're
        # pre-declared as defaults for the TenDayRange class.
        ten_day_range = TenDayRange()

    A CustomFactor without defaults:

    .. code-block:: python

        class MedianValue(CustomFactor):
            """
            Computes the median value of an arbitrary single input over an
            arbitrary window..

            Does not declare any defaults, so values for `window_length` and
            `inputs` must be passed explicitly on every construction.
            """

            def compute(self, today, assets, out, data):
                from numpy import nanmedian
                out[:] = data.nanmedian(data, axis=0)

        # Values for `inputs` and `window_length` must be passed explicitly to
        # MedianValue.
        median_close10 = MedianValue([USEquityPricing.close], window_length=10)
        median_low15 = MedianValue([USEquityPricing.low], window_length=15)

    A CustomFactor with multiple outputs:

    .. code-block:: python

        class MultipleOutputs(CustomFactor):
            inputs = [USEquityPricing.close]
            outputs = ['alpha', 'beta']
            window_length = N

            def compute(self, today, assets, out, close):
                computed_alpha, computed_beta = some_function(close)
                out.alpha[:] = computed_alpha
                out.beta[:] = computed_beta

        # Each output is returned as its own Factor upon instantiation.
        alpha, beta = MultipleOutputs()

        # Equivalently, we can create a single factor instance and access each
        # output as an attribute of that instance.
        multiple_outputs = MultipleOutputs()
        alpha = multiple_outputs.alpha
        beta = multiple_outputs.beta

    Note: If a CustomFactor has multiple outputs, all outputs must have the
    same dtype. For instance, in the example above, if alpha is a float then
    beta must also be a float.
    '''

    dtype = float64_dtype

    def _validate(self):
        try:
            super(CustomFactor, self)._validate()
        except UnsupportedDataType as exc:
            if self.dtype in CLASSIFIER_DTYPES:
                raise UnsupportedDataType(
                    typename=type(self).__name__,
                    dtype=self.dtype,
                    hint="Did you mean to create a CustomClassifier?",
                ) from exc
            elif self.dtype in FILTER_DTYPES:
                raise UnsupportedDataType(
                    typename=type(self).__name__,
                    dtype=self.dtype,
                    hint="Did you mean to create a CustomFilter?",
                ) from exc
            raise

    def __getattribute__(self, name):
        outputs = object.__getattribute__(self, "outputs")
        if outputs is None:
            return super(CustomFactor, self).__getattribute__(name)
        elif name in outputs:
            return RecarrayField(factor=self, attribute=name)
        else:
            try:
                return super(CustomFactor, self).__getattribute__(name)
            except AttributeError as exc:
                raise AttributeError(
                    "Instance of {factor} has no output named {attr!r}. "
                    "Possible choices are: {choices}.".format(
                        factor=type(self).__name__,
                        attr=name,
                        choices=self.outputs,
                    )
                ) from exc

    def __iter__(self):
        if self.outputs is None:
            raise ValueError(
                "{factor} does not have multiple outputs.".format(
                    factor=type(self).__name__,
                )
            )
        return (RecarrayField(self, attr) for attr in self.outputs)
