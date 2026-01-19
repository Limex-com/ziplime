
from .iface import PipelineHooks
from .no import NoHooks

class DelegatingHooks(PipelineHooks):
    """A PipelineHooks that delegates to one or more other hooks.

    Parameters
    ----------
    hooks : list[implements(PipelineHooks)]
        Sequence of hooks to delegate to.
    """

    def __new__(cls, hooks):
        if len(hooks) == 0:
            # OPTIMIZATION: Short-circuit to a NoHooks if we don't have any
            # sub-hooks.
            return NoHooks()
        elif len(hooks) == 1:
            # OPTIMIZATION: Unwrap delegation layer if we only have one
            # sub-hook.
            return hooks[0]
        else:
            self = super(DelegatingHooks, cls).__new__(cls)
            self._hooks = hooks
            return self


