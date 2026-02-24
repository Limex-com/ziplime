from ziplime.pipeline.terms.factors.factor import Factor
from ziplime.pipeline.terms.asset_exists import AssetExists
from ziplime.errors import UnknownRankMethod
from ziplime.lib.rank import masked_rankdata_2d
from ziplime.pipeline.mixins import SingleInputMixin

from ziplime.utils.numpy_utils import float64_dtype

_RANK_METHODS = frozenset(["average", "min", "max", "dense", "ordinal"])

class Rank(SingleInputMixin, Factor):
    """
    A Factor representing the row-wise rank data of another Factor.

    Parameters
    ----------
    factor : ziplime.pipeline.Factor
        The factor on which to compute ranks.
    method : str, {'average', 'min', 'max', 'dense', 'ordinal'}
        The method used to assign ranks to tied elements.  See
        `scipy.stats.rankdata` for a full description of the semantics for each
        ranking method.

    See Also
    --------
    :func:`scipy.stats.rankdata`
    :class:`Factor.rank`

    Notes
    -----
    Most users should call Factor.rank rather than directly construct an
    instance of this class.
    """

    window_length = 0
    dtype = float64_dtype
    window_safe = True

    def __new__(cls, factor, method, ascending, mask):
        return super(Rank, cls).__new__(
            cls,
            inputs=(factor,),
            method=method,
            ascending=ascending,
            mask=mask,
        )

    def _init(self, method, ascending, *args, **kwargs):
        self._method = method
        self._ascending = ascending
        return super(Rank, self)._init(*args, **kwargs)

    @classmethod
    def _static_identity(cls, method, ascending, *args, **kwargs):
        return (
            super(Rank, cls)._static_identity(*args, **kwargs),
            method,
            ascending,
        )

    def _validate(self):
        """
        Verify that the stored rank method is valid.
        """
        if self._method not in _RANK_METHODS:
            raise UnknownRankMethod(
                method=self._method,
                choices=set(_RANK_METHODS),
            )
        return super(Rank, self)._validate()

    def _compute(self, arrays, dates, assets, mask):
        """
        For each row in the input, compute a like-shaped array of per-row
        ranks.
        """
        return masked_rankdata_2d(
            arrays[0],
            mask,
            self.inputs[0].missing_value,
            self._method,
            self._ascending,
        )

    def __repr__(self):
        if self.mask is AssetExists():
            # Don't include mask in repr if it's the default.
            mask_info = ""
        else:
            mask_info = ", mask={}".format(self.mask.recursive_repr())

        return "{type}({input_}, method='{method}'{mask_info})".format(
            type=type(self).__name__,
            input_=self.inputs[0].recursive_repr(),
            method=self._method,
            mask_info=mask_info,
        )

    def graph_repr(self):
        # Graphviz interprets `\l` as "divide label into lines, left-justified"
        return "Rank:\\l  method: {!r}\\l  mask: {}\\l".format(
            self._method,
            type(self.mask).__name__,
        )
