


from numbers import Number
from numpy import (
    exp,
    log,
)

from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.pipeline.terms.factors.custom_factor import CustomFactor


class ExponentialWeightedFactor(SingleInputMixin, CustomFactor):
    """
    Base class for factors implementing exponential-weighted operations.

    **Default Inputs:** None

    **Default Window Length:** None

    Parameters
    ----------
    inputs : length-1 list or tuple of BoundColumn
        The expression over which to compute the average.
    window_length : int > 0
        Length of the lookback window over which to compute the average.
    decay_rate : float, 0 < decay_rate <= 1
        Weighting factor by which to discount past observations.

        When calculating historical averages, rows are multiplied by the
        sequence::

            decay_rate, decay_rate ** 2, decay_rate ** 3, ...

    Methods
    -------
    weights
    from_span
    from_halflife
    from_center_of_mass
    """

    params = ("decay_rate",)

    @classmethod
    def from_span(cls, inputs, window_length, span: Number, **kwargs):
        """
        Convenience constructor for passing `decay_rate` in terms of `span`.

        Forwards `decay_rate` as `1 - (2.0 / (1 + span))`.  This provides the
        behavior equivalent to passing `span` to pandas.ewma.

        Examples
        --------
        .. code-block:: python

            # Equivalent to:
            # my_ewma = EWMA(
            #    inputs=[EquityPricing.close],
            #    window_length=30,
            #    decay_rate=(1 - (2.0 / (1 + 15.0))),
            # )
            my_ewma = EWMA.from_span(
                inputs=[EquityPricing.close],
                window_length=30,
                span=15,
            )

        Notes
        -----
        This classmethod is provided by both
        :class:`ExponentialWeightedMovingAverage` and
        :class:`ExponentialWeightedMovingStdDev`.
        """
        if span <= 1:
            raise ValueError("`span` must be a positive number. %s was passed." % span)

        decay_rate = 1.0 - (2.0 / (1.0 + span))
        assert 0.0 < decay_rate <= 1.0

        return cls(
            inputs=inputs, window_length=window_length, decay_rate=decay_rate, **kwargs
        )

    @classmethod
    def from_halflife(cls, inputs, window_length, halflife: Number, **kwargs):
        """
        Convenience constructor for passing ``decay_rate`` in terms of half
        life.

        Forwards ``decay_rate`` as ``exp(log(.5) / halflife)``.  This provides
        the behavior equivalent to passing `halflife` to pandas.ewma.

        Examples
        --------
        .. code-block:: python

            # Equivalent to:
            # my_ewma = EWMA(
            #    inputs=[EquityPricing.close],
            #    window_length=30,
            #    decay_rate=np.exp(np.log(0.5) / 15),
            # )
            my_ewma = EWMA.from_halflife(
                inputs=[EquityPricing.close],
                window_length=30,
                halflife=15,
            )

        Notes
        -----
        This classmethod is provided by both
        :class:`ExponentialWeightedMovingAverage` and
        :class:`ExponentialWeightedMovingStdDev`.
        """
        if halflife <= 0:
            raise ValueError(
                "`span` must be a positive number. %s was passed." % halflife
            )
        decay_rate = exp(log(0.5) / halflife)
        assert 0.0 < decay_rate <= 1.0

        return cls(
            inputs=inputs, window_length=window_length, decay_rate=decay_rate, **kwargs
        )

    @classmethod
    def from_center_of_mass(cls, inputs, window_length, center_of_mass, **kwargs):
        """
        Convenience constructor for passing `decay_rate` in terms of center of
        mass.

        Forwards `decay_rate` as `1 - (1 / 1 + center_of_mass)`.  This provides
        behavior equivalent to passing `center_of_mass` to pandas.ewma.

        Examples
        --------
        .. code-block:: python

            # Equivalent to:
            # my_ewma = EWMA(
            #    inputs=[EquityPricing.close],
            #    window_length=30,
            #    decay_rate=(1 - (1 / 15.0)),
            # )
            my_ewma = EWMA.from_center_of_mass(
                inputs=[EquityPricing.close],
                window_length=30,
                center_of_mass=15,
            )

        Notes
        -----
        This classmethod is provided by both
        :class:`ExponentialWeightedMovingAverage` and
        :class:`ExponentialWeightedMovingStdDev`.
        """
        return cls(
            inputs=inputs,
            window_length=window_length,
            decay_rate=(1.0 - (1.0 / (1.0 + center_of_mass))),
            **kwargs,
        )