"""The shared boolean term contract for computed filters and root masks."""

from ziplime.pipeline.terms.term import Term
from ziplime.utils.numpy_utils import bool_dtype


class BooleanTerm(Term):
    """A boolean-valued term used to select asset/date pairs in a pipeline."""

    dtype = bool_dtype
