from ziplime.pipeline.terms.term import Term
from ziplime.utils.numpy_utils import bool_dtype


class AssetExists(Term):
    """
    Pseudo-filter describing whether or not an asset existed on a given day.
    This is the default mask for all terms that haven't been passed a mask
    explicitly.

    This is morally a Filter, in the sense that it produces a boolean value for
    every asset on every date.  We don't subclass Filter, however, because
    `AssetExists` is computed directly by the PipelineEngine.

    This term is guaranteed to be available as an input for any term computed
    by SimplePipelineEngine.run_pipeline().

    See Also
    --------
    ziplime.assets.AssetFinder.lifetimes
    """

    dtype = bool_dtype
    dataset = None
    inputs = ()
    dependencies = {}
    mask = None
    windowed = False

    def __repr__(self):
        return "AssetExists()"

    graph_repr = __repr__

    def _compute(self, today, assets, out):
        raise NotImplementedError(
            "AssetExists cannot be computed directly."
            " Check your PipelineEngine configuration."
        )