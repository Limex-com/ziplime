from collections import namedtuple
from ziplime.pipeline.data.bound_column_descr import BoundColumnDescr
from ziplime.pipeline.data.dataset_meta import DataSetMeta

from ziplime.pipeline.domain import GENERIC
from ziplime.utils.string_formatting import bulleted_list



class DataSet(object, metaclass=DataSetMeta):
    """
    Base class for Pipeline datasets.

    A :class:`DataSet` is defined by two parts:

    1. A collection of :class:`~ziplime.pipeline.data.Column` objects that
       describe the queryable attributes of the dataset.

    2. A :class:`~ziplime.pipeline.domain.Domain` describing the assets and
       calendar of the data represented by the :class:`DataSet`.

    To create a new Pipeline dataset, define a subclass of :class:`DataSet` and
    set one or more :class:`Column` objects as class-level attributes. Each
    column requires a ``np.dtype`` that describes the type of data that should
    be produced by a loader for the dataset. Integer columns must also provide
    a "missing value" to be used when no value is available for a given
    asset/date combination.

    By default, the domain of a dataset is the special singleton value,
    :data:`~ziplime.pipeline.domain.GENERIC`, which means that they can be used
    in a Pipeline running on **any** domain.

    In some cases, it may be preferable to restrict a dataset to only allow
    support a single domain. For example, a DataSet may describe data from a
    vendor that only covers the US. To restrict a dataset to a specific domain,
    define a `domain` attribute at class scope.

    You can also define a domain-specific version of a generic DataSet by
    calling its ``specialize`` method with the domain of interest.

    Examples
    --------
    The built-in EquityPricing dataset is defined as follows::

        class EquityPricing(DataSet):
            open = Column(float)
            high = Column(float)
            low = Column(float)
            close = Column(float)
            volume = Column(float)

    The built-in USEquityPricing dataset is a specialization of
    EquityPricing. It is defined as::

        from ziplime.pipeline.domain import US_EQUITIES
        USEquityPricing = EquityPricing.specialize(US_EQUITIES)

    Columns can have types other than float. A dataset containing assorted
    company metadata might be defined like this::

        class CompanyMetadata(DataSet):
            # Use float for semantically-numeric data, even if it's always
            # integral valued (see Notes section below). The default missing
            # value for floats is NaN.
            shares_outstanding = Column(float)

            # Use object for string columns. The default missing value for
            # object-dtype columns is None.
            ticker = Column(object)

            # Use integers for integer-valued categorical data like sector or
            # industry codes. Integer-dtype columns require an explicit missing
            # value.
            sector_code = Column(int, missing_value=-1)

            # Use bool for boolean-valued flags. Note that the default missing
            # value for bool-dtype columns is False.
            is_primary_share = Column(bool)

    Notes
    -----
    Because numpy has no native support for integers with missing values, users
    are strongly encouraged to use floats for any data that's semantically
    numeric. Doing so enables the use of `NaN` as a natural missing value,
    which has useful propagation semantics.
    """

    domain = GENERIC
    ndim = 2

    @classmethod
    def get_column(cls, name):
        """Look up a column by name.

        Parameters
        ----------
        name : str
            Name of the column to look up.

        Returns
        -------
        column : ziplime.pipeline.data.BoundColumn
            Column with the given name.

        Raises
        ------
        AttributeError
            If no column with the given name exists.
        """
        clsdict = vars(cls)
        try:
            maybe_column = clsdict[name]
            if not isinstance(maybe_column, BoundColumnDescr):
                raise KeyError(name)
        except KeyError as exc:
            raise AttributeError(
                "{dset} has no column {colname!r}:\n\n"
                "Possible choices are:\n"
                "{choices}".format(
                    dset=cls.qualname,
                    colname=name,
                    choices=bulleted_list(
                        sorted(cls._column_names),
                        max_count=10,
                    ),
                )
            ) from exc

        # Resolve column descriptor into a BoundColumn.
        return maybe_column.__get__(None, cls)


# This attribute is set by DataSetMeta to mark that a class is the root of a
# family of datasets with diffent domains. We don't want that behavior for the
# base DataSet class, and we also don't want to accidentally use a shared
# version of this attribute if we fail to set this in a subclass somewhere.
del DataSet._domain_specializations

CurrencyConversion = namedtuple(
    "CurrencyConversion",
    ["currency", "field"],
)
