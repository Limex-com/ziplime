import abc
from collections import OrderedDict

from ziplime.pipeline.data import Column
from ziplime.pipeline.data.dataset_family_column import DataSetFamilyColumn


class DataSetFamilyMeta(abc.ABCMeta):
    def __new__(cls, name, bases, dict_):
        columns = {}
        for k, v in dict_.items():
            if isinstance(v, Column):
                # capture all the columns off the DataSetFamily class
                # and replace them with a descriptor that will raise a helpful
                # error message. The columns will get added to the BaseSlice
                # for this type.
                columns[k] = v
                dict_[k] = DataSetFamilyColumn(k)

        is_abstract = dict_.pop("_abstract", False)

        self = super(DataSetFamilyMeta, cls).__new__(
            cls,
            name,
            bases,
            dict_,
        )

        if not is_abstract:
            self.extra_dims = extra_dims = OrderedDict(
                [(k, frozenset(v)) for k, v in OrderedDict(self.extra_dims).items()]
            )
            if not extra_dims:
                raise ValueError(
                    "DataSetFamily must be defined with non-empty"
                    " extra_dims, or with `_abstract = True`",
                )

            class BaseSlice(self._SliceType):
                dataset_family = self

                ndim = self.slice_ndim
                domain = self.domain

                locals().update(columns)

            BaseSlice.__name__ = "%sBaseSlice" % self.__name__
            self._SliceType = BaseSlice

        # each type gets a unique cache
        self._slice_cache = {}
        return self

    def __repr__(self):
        return "<DataSetFamily: %r, extra_dims=%r>" % (
            self.__name__,
            list(self.extra_dims),
        )

