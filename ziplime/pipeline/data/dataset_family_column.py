from typing import TYPE_CHECKING

from ziplime.pipeline.errors.dataset_family_lookup_error import DataSetFamilyLookupError

# DataSetFamily imports this descriptor through its metaclass.
if TYPE_CHECKING:
    from ziplime.pipeline.data.dataset_family import DataSetFamily


class DataSetFamilyColumn:
    """Descriptor used to raise a helpful error when a column is accessed on a
    DataSetFamily instead of on the result of a slice.

    Parameters
    ----------
    column_names : str
        The name of the column.
    """

    def __init__(self, column_name: str) -> None:
        self.column_name = column_name

    def __get__(self, instance: 'DataSetFamily | None', owner: 'type[DataSetFamily]'):
        raise DataSetFamilyLookupError(
            owner.__name__,
            self.column_name,
        )
