"""Run plain for engine."""


class RunPlain(Exception):
    """A compiled run met what only a plain one checks (a computation on history whose parameter
    changed during the run): the batch is run again without compiling."""
