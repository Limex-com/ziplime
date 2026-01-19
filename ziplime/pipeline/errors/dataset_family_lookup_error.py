from textwrap import dedent


class DataSetFamilyLookupError(AttributeError):
    """Exception thrown when a column is accessed on a DataSetFamily
    instead of on the result of a slice.

    Parameters
    ----------
    family_name : str
        The name of the DataSetFamily on which the access occurred.
    column_name : str
        The name of the column accessed.
    """

    def __init__(self, family_name, column_name):
        self.family_name = family_name
        self.column_name = column_name

    def __str__(self):
        # NOTE: when ``aggregate`` is added, remember to update this message
        return dedent(
            """\
            Attempted to access column {c} from DataSetFamily {d}:

            To work with dataset families, you must first select a
            slice using the ``slice`` method:

                {d}.slice(...).{c}
            """.format(
                c=self.column_name, d=self.family_name
            )
        )
