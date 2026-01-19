from ziplime.pipeline import Term
from ziplime.utils.numpy_utils import datetime64ns_dtype


class InputDates(Term):
    """
    1-Dimensional term providing date labels for other term inputs.

    This term is guaranteed to be available as an input for any term computed
    by SimplePipelineEngine.run_pipeline().
    """

    ndim = 1
    dataset = None
    dtype = datetime64ns_dtype
    inputs = ()
    dependencies = {}
    mask = None
    windowed = False
    window_safe = True

    def __repr__(self):
        return "InputDates()"

    graph_repr = __repr__

    def _compute(self, today, assets, out):
        raise NotImplementedError(
            "InputDates cannot be computed directly."
            " Check your PipelineEngine configuration."
        )
