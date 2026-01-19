from ziplime.pipeline.terms.filters.custom_filter import CustomFilter


class Any(CustomFilter):
    """
    A Filter requiring that assets produce True for at least one day in the
    last ``window_length`` days.

    **Default Inputs:** None

    **Default Window Length:** None
    """

    def compute(self, today, assets, out, arg):
        out[:] = arg.sum(axis=0) > 0

