from ziplime.pipeline.terms.term import Term


class LoadableTerm(Term):
    """
    A Term that should be loaded from an external resource by a PipelineLoader.

    This is the base class for :class:`ziplime.pipeline.data.BoundColumn`.
    """

    windowed = False
    inputs = ()

    @property
    def dependencies(self):
        return {self.mask: 0}