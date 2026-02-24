@object.__new__
class nop_context:
    """A nop context manager."""

    def __enter__(self):
        pass

    def __exit__(self, *excinfo):
        pass

