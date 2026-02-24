from ziplime.pipeline.errors.dataset_family_lookup_error import DataSetFamilyLookupError


class DataSetFamilyColumn:
    """Descriptor used to raise a helpful error when a column is accessed on a
    DataSetFamily instead of on the result of a slice.

    Parameters
    ----------
    column_names : str
        The name of the column.
    """

    def __init__(self, column_name):
        self.column_name = column_name

    def __get__(self, instance, owner):
        raise DataSetFamilyLookupError(
            owner.__name__,
            self.column_name,
        )
