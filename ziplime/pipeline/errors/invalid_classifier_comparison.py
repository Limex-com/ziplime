from ziplime.pipeline.terms.term import Term


class InvalidClassifierComparison(TypeError):
    def __init__(self, classifier: Term, compval) -> None:
        super(InvalidClassifierComparison, self).__init__(
            "Can't compare classifier of dtype"
            " {dtype} to value {value} of type {type}.".format(
                dtype=classifier.dtype,
                value=compval,
                type=type(compval).__name__,
            )
        )
