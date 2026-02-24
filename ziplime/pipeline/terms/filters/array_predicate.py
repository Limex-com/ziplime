from ziplime.pipeline.terms.term import Term
from ziplime.pipeline.mixins import SingleInputMixin
from ziplime.pipeline.terms.filters.filter import Filter


class ArrayPredicate(SingleInputMixin, Filter):
    """
    A filter applying a function from (ndarray, *args) -> ndarray[bool].

    Parameters
    ----------
    term : ziplime.pipeline.Term
        Term producing the array over which the predicate will be computed.
    op : function(ndarray, *args) -> ndarray[bool]
        Function to apply to the result of `term`.
    opargs : tuple[hashable]
        Additional argument to apply to ``op``.
    """

    params = ("op", "opargs")
    window_length = 0

    def __new__(cls, term: Term, op, opargs: tuple):
        hash(opargs)  # fail fast if opargs isn't hashable.
        return super(ArrayPredicate, cls).__new__(
            ArrayPredicate,
            op=op,
            opargs=opargs,
            inputs=(term,),
            mask=term.mask,
        )

    def _compute(self, arrays, dates, assets, mask):
        params = self.params
        data = arrays[0]
        return params["op"](data, *params["opargs"]) & mask

    def graph_repr(self):
        # Graphviz interprets `\l` as "divide label into lines, left-justified"
        return "{}:\\l  op: {}.{}()".format(
            type(self).__name__,
            self.params["op"].__module__,
            self.params["op"].__name__,
        )
