from ziplime.utils.numpy_utils import float64_dtype


class Column:
    """
    An abstract column of data, not yet associated with a dataset.
    """

    def __init__(
            self,
            dtype,
            missing_value=None,
            doc=None,
            metadata=None,
            currency_aware=False,
    ):
        if currency_aware and dtype != float64_dtype:
            raise ValueError(
                "Columns cannot be constructed with currency_aware={}, "
                "dtype={}. Currency aware columns must have a float64 dtype.".format(
                    currency_aware, dtype
                )
            )

        self.dtype = dtype
        self.missing_value = missing_value
        self.doc = doc
        self.metadata = metadata.copy() if metadata is not None else {}
        self.currency_aware = currency_aware

    def bind(self, name):
        from ziplime.pipeline.data.bound_column_descr import BoundColumnDescr

        """
        Bind a `Column` object to its name.
        """
        return BoundColumnDescr(
            dtype=self.dtype,
            missing_value=self.missing_value,
            name=name,
            doc=self.doc,
            metadata=self.metadata,
            currency_aware=self.currency_aware,
        )
