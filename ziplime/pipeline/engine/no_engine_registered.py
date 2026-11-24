"""No engine registered for engine."""


class NoEngineRegistered(Exception):
    """Raised if a user tries to call pipeline_output in an algorithm that hasn't
    set up a pipeline engine.
    """
