from ziplime.pipeline.terms.bound_column import BoundColumn
from ziplime.pipeline.terms.utils import validate_dtype
from ziplime.utils.numpy_utils import NoDefaultMissingValue


class BoundColumnDescr:
    """
    Intermediate class that sits on `DataSet` objects and returns memoized
    `BoundColumn` objects when requested.

    This exists so that subclasses of DataSets don't share columns with their
    parent classes.
    """

    def __init__(self, dtype, missing_value, name, doc, metadata, currency_aware):
        # Validating and calculating default missing values here guarantees
        # that we fail quickly if the user passes an unsupporte dtype or fails
        # to provide a missing value for a dtype that requires one
        # (e.g. int64), but still enables us to provide an error message that
        # points to the name of the failing column.
        try:
            self.dtype, self.missing_value = validate_dtype(
                termname="Column(name={name!r})".format(name=name),
                dtype=dtype,
                missing_value=missing_value,
            )
        except NoDefaultMissingValue as exc:
            # Re-raise with a more specific message.
            raise NoDefaultMissingValue(
                "Failed to create Column with name {name!r} and"
                " dtype {dtype} because no missing_value was provided\n\n"
                "Columns with dtype {dtype} require a missing_value.\n"
                "Please pass missing_value to Column() or use a different"
                " dtype.".format(dtype=dtype, name=name)
            ) from exc
        self.name = name
        self.doc = doc
        self.metadata = metadata
        self.currency_aware = currency_aware

    def __get__(self, instance, owner):
        """
        Produce a concrete BoundColumn object when accessed.

        We don't bind to datasets at class creation time so that subclasses of
        DataSets produce different BoundColumns.
        """
        return BoundColumn(
            dtype=self.dtype,
            missing_value=self.missing_value,
            dataset=owner,
            name=self.name,
            doc=self.doc,
            metadata=self.metadata,
            currency_conversion=None,
            currency_aware=self.currency_aware,
        )