
from toolz import first
from ziplime.currency import Currency
from ziplime.data.fx import DEFAULT_FX_RATE
from ziplime.pipeline.terms.asset_exists import AssetExists

from ziplime.pipeline.terms.classifiers import Classifier, Latest as LatestClassifier
from ziplime.pipeline.domain import GENERIC
from ziplime.pipeline.terms.factors import Factor, Latest as LatestFactor
from ziplime.pipeline.terms.filters import Filter, Latest as LatestFilter
from ziplime.pipeline.terms.loadable_term import LoadableTerm
from ziplime.utils.numpy_utils import float64_dtype


class BoundColumn(LoadableTerm):
    """
    A column of data that's been concretely bound to a particular dataset.

    Attributes
    ----------
    dtype : numpy.dtype
        The dtype of data produced when this column is loaded.
    latest : ziplime.pipeline.LoadableTerm
        A :class:`~ziplime.pipeline.Filter`, :class:`~ziplime.pipeline.Factor`,
        or :class:`~ziplime.pipeline.Classifier` computing the most recently
        known value of this column on each date.
        See :class:`ziplime.pipeline.mixins.LatestMixin` for more details.
    dataset : ziplime.pipeline.data.DataSet
        The dataset to which this column is bound.
    name : str
        The name of this column.
    metadata : dict
        Extra metadata associated with this column.
    currency_aware : bool
        Whether or not this column produces currency-denominated data.

    Notes
    -----
    Instances of this class are dynamically created upon access to attributes
    of :class:`~ziplime.pipeline.data.DataSet`. For example,
    :attr:`~ziplime.pipeline.data.EquityPricing.close` is an instance of this
    class. Pipeline API users should never construct instances of this
    directly.
    """

    mask = AssetExists()
    window_safe = True

    def __new__(
            cls,
            dtype,
            missing_value,
            dataset,
            name,
            doc,
            metadata,
            currency_conversion,
            currency_aware,
    ):
        if currency_aware and dtype != float64_dtype:
            raise AssertionError(
                "The {} column on dataset {} cannot be constructed with "
                "currency_aware={}, dtype={}. Currency aware columns must "
                "have a float64 dtype.".format(
                    name,
                    dataset,
                    currency_aware,
                    dtype,
                )
            )

        return super(BoundColumn, cls).__new__(
            cls,
            domain=dataset.domain,
            dtype=dtype,
            missing_value=missing_value,
            dataset=dataset,
            name=name,
            ndim=dataset.ndim,
            doc=doc,
            metadata=metadata,
            currency_conversion=currency_conversion,
            currency_aware=currency_aware,
        )

    def _init(
            self,
            dataset,
            name,
            doc,
            metadata,
            currency_conversion,
            currency_aware,
            *args,
            **kwargs,
    ):
        self._dataset = dataset
        self._name = name
        self.__doc__ = doc
        self._metadata = metadata
        self._currency_conversion = currency_conversion
        self._currency_aware = currency_aware
        return super(BoundColumn, self)._init(*args, **kwargs)

    @classmethod
    def _static_identity(
            cls,
            dataset,
            name,
            doc,
            metadata,
            currency_conversion,
            currency_aware,
            *args,
            **kwargs,
    ):
        return (
            super(BoundColumn, cls)._static_identity(*args, **kwargs),
            dataset,
            name,
            doc,
            frozenset(sorted(metadata.items(), key=first)),
            currency_conversion,
            currency_aware,
        )

    def __lt__(self, other):
        msg = "Can't compare '{}' with '{}'. (Did you mean to use '.latest'?)"
        raise TypeError(msg.format(self.qualname, other.__class__.__name__))

    __gt__ = __le__ = __ge__ = __lt__

    def _replace(self, **kwargs):
        kw = dict(
            dtype=self.dtype,
            missing_value=self.missing_value,
            dataset=self._dataset,
            name=self._name,
            doc=self.__doc__,
            metadata=self._metadata,
            currency_conversion=self._currency_conversion,
            currency_aware=self._currency_aware,
        )
        kw.update(kwargs)

        return type(self)(**kw)

    def specialize(self, domain):
        """Specialize ``self`` to a concrete domain."""
        if domain == self.domain:
            return self

        return self._replace(dataset=self._dataset.specialize(domain))

    def unspecialize(self):
        """
        Unspecialize a column to its generic form.

        This is equivalent to ``column.specialize(GENERIC)``.
        """
        return self.specialize(GENERIC)

    def fx(self, currency: str | Currency):
        """
        Construct a currency-converted version of this column.

        Parameters
        ----------
        currency : str or ziplime.currency.Currency
            Currency into which to convert this column's data.

        Returns
        -------
        column : BoundColumn
            Column producing the same data as ``self``, but currency-converted
            into ``currency``.
        """
        conversion = self._currency_conversion

        if not self._currency_aware:
            raise TypeError(
                "The .fx() method cannot be called on {} because it does not "
                "produce currency-denominated data.".format(self.qualname)
            )
        elif conversion is not None and conversion.currency == currency:
            return self

        return self._replace(
            currency_conversion=CurrencyConversion(
                currency=currency,
                field=DEFAULT_FX_RATE,
            )
        )

    @property
    def currency_conversion(self):
        """Specification for currency conversions applied for this term."""
        return self._currency_conversion

    @property
    def currency_aware(self):
        """
        Whether or not this column produces currency-denominated data.
        """
        return self._currency_aware

    @property
    def dataset(self):
        """
        The dataset to which this column is bound.
        """
        return self._dataset

    @property
    def name(self):
        """
        The name of this column.
        """
        return self._name

    @property
    def metadata(self):
        """
        A copy of the metadata for this column.
        """
        return self._metadata.copy()

    @property
    def qualname(self):
        """The fully-qualified name of this column."""
        out = ".".join([self.dataset.qualname, self.name])
        conversion = self._currency_conversion
        if conversion is not None:
            out += ".fx({!r})".format(conversion.currency.code)
        return out

    @property
    def latest(self):
        dtype = self.dtype
        if dtype in Filter.ALLOWED_DTYPES:
            Latest = LatestFilter
        elif dtype in Classifier.ALLOWED_DTYPES:
            Latest = LatestClassifier
        else:
            assert dtype in Factor.ALLOWED_DTYPES, "Unknown dtype %s." % dtype
            Latest = LatestFactor

        return Latest(
            inputs=(self,),
            dtype=dtype,
            missing_value=self.missing_value,
            ndim=self.ndim,
        )

    def __repr__(self):
        return "{qualname}::{dtype}".format(
            qualname=self.qualname,
            dtype=self.dtype.name,
        )

    def graph_repr(self):
        """Short repr to use when rendering Pipeline graphs."""
        # Graphviz interprets `\l` as "divide label into lines, left-justified"
        return "BoundColumn:\\l  Dataset: {}\\l  Column: {}\\l".format(
            self.dataset.__name__, self.name
        )

    def recursive_repr(self):
        """Short repr used to render in recursive contexts."""
        return self.qualname
