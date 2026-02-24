from itertools import repeat

from collections import OrderedDict

from ziplime.pipeline.data.dataset_family_meta import DataSetFamilyMeta
from ziplime.pipeline.data.dataset_family_slice import DataSetFamilySlice
from ziplime.pipeline.domain import GENERIC
from ziplime.utils.formatting import plural, s


# XXX: This docstring was mostly written when the abstraction here was
# "MultiDimensionalDataSet". It probably needs some rewriting.
class DataSetFamily(metaclass=DataSetFamilyMeta):
    """
    Base class for Pipeline dataset families.

    Dataset families are used to represent data where the unique identifier for
    a row requires more than just asset and date coordinates. A
    :class:`DataSetFamily` can also be thought of as a collection of
    :class:`~ziplime.pipeline.data.DataSet` objects, each of which has the same
    columns, domain, and ndim.

    :class:`DataSetFamily` objects are defined with one or more
    :class:`~ziplime.pipeline.data.Column` objects, plus one additional field:
    ``extra_dims``.

    The ``extra_dims`` field defines coordinates other than asset and date that
    must be fixed to produce a logical timeseries. The column objects determine
    columns that will be shared by slices of the family.

    ``extra_dims`` are represented as an ordered dictionary where the keys are
    the dimension name, and the values are a set of unique values along that
    dimension.

    To work with a :class:`DataSetFamily` in a pipeline expression, one must
    choose a specific value for each of the extra dimensions using the
    :meth:`~ziplime.pipeline.data.DataSetFamily.slice` method.
    For example, given a :class:`DataSetFamily`:

    .. code-block:: python

       class SomeDataSet(DataSetFamily):
           extra_dims = [
               ('dimension_0', {'a', 'b', 'c'}),
               ('dimension_1', {'d', 'e', 'f'}),
           ]

           column_0 = Column(float)
           column_1 = Column(bool)

    This dataset might represent a table with the following columns:

    ::

      sid :: int64
      asof_date :: datetime64[ns]
      timestamp :: datetime64[ns]
      dimension_0 :: str
      dimension_1 :: str
      column_0 :: float64
      column_1 :: bool

    Here we see the implicit ``sid``, ``asof_date`` and ``timestamp`` columns
    as well as the extra dimensions columns.

    This :class:`DataSetFamily` can be converted to a regular :class:`DataSet`
    with:

    .. code-block:: python

       DataSetSlice = SomeDataSet.slice(dimension_0='a', dimension_1='e')

    This sliced dataset represents the rows from the higher dimensional dataset
    where ``(dimension_0 == 'a') & (dimension_1 == 'e')``.
    """

    _abstract = True  # Removed by metaclass

    domain = GENERIC
    slice_ndim = 2

    _SliceType = DataSetFamilySlice

    @type.__call__
    class extra_dims:
        """OrderedDict[str, frozenset] of dimension name -> unique values

        May be defined on subclasses as an iterable of pairs: the
        metaclass converts this attribute to an OrderedDict.
        """

        __isabstractmethod__ = True

        def __get__(self, instance, owner):
            return []

    @classmethod
    def _canonical_key(cls, args, kwargs):
        extra_dims = cls.extra_dims
        dimensions_set = set(extra_dims)
        if not set(kwargs) <= dimensions_set:
            extra = sorted(set(kwargs) - dimensions_set)
            raise TypeError(
                "%s does not have the following %s: %s\n"
                "Valid dimensions are: %s"
                % (
                    cls.__name__,
                    s("dimension", extra),
                    ", ".join(extra),
                    ", ".join(extra_dims),
                ),
            )

        if len(args) > len(extra_dims):
            raise TypeError(
                "%s has %d extra %s but %d %s given"
                % (
                    cls.__name__,
                    len(extra_dims),
                    s("dimension", extra_dims),
                    len(args),
                    plural("was", "were", args),
                ),
            )

        missing = object()
        coords = OrderedDict(zip(extra_dims, repeat(missing)))
        to_add = dict(zip(extra_dims, args))
        coords.update(to_add)
        added = set(to_add)

        for key, value in kwargs.items():
            if key in added:
                raise TypeError(
                    "%s got multiple values for dimension %r"
                    % (
                        cls.__name__,
                        coords,
                    ),
                )
            coords[key] = value
            added.add(key)

        missing = {k for k, v in coords.items() if v is missing}
        if missing:
            missing = sorted(missing)
            raise TypeError(
                "no coordinate provided to %s for the following %s: %s"
                % (
                    cls.__name__,
                    s("dimension", missing),
                    ", ".join(missing),
                ),
            )

        # validate that all of the provided values exist along their given
        # dimensions
        for key, value in coords.items():
            if value not in cls.extra_dims[key]:
                raise ValueError(
                    "%r is not a value along the %s dimension of %s"
                    % (
                        value,
                        key,
                        cls.__name__,
                    ),
                )

        return coords, tuple(coords.items())

    @classmethod
    def _make_dataset(cls, coords):
        """Construct a new dataset given the coordinates."""

        class Slice(cls._SliceType):
            extra_coords = coords

        Slice.__name__ = "%s.slice(%s)" % (
            cls.__name__,
            ", ".join("%s=%r" % item for item in coords.items()),
        )
        return Slice

    @classmethod
    def slice(cls, *args, **kwargs):
        """Take a slice of a DataSetFamily to produce a dataset
        indexed by asset and date.

        Parameters
        ----------
        *args
        **kwargs
            The coordinates to fix along each extra dimension.

        Returns
        -------
        dataset : DataSet
            A regular pipeline dataset indexed by asset and date.

        Notes
        -----
        The extra dimensions coords used to produce the result are available
        under the ``extra_coords`` attribute.
        """
        coords, hash_key = cls._canonical_key(args, kwargs)
        try:
            return cls._slice_cache[hash_key]
        except KeyError:
            pass

        Slice = cls._make_dataset(coords)
        cls._slice_cache[hash_key] = Slice
        return Slice
