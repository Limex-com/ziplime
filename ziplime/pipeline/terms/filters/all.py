from ziplime.pipeline.terms.filters.custom_filter import CustomFilter


class All(CustomFilter):
    """
    A Filter requiring that assets produce True for ``window_length``
    consecutive days.

    **Default Inputs:** None

    **Default Window Length:** None
    """

    def compute(self, today, assets, out, arg):
        out[:] = arg.sum(axis=0) == self.window_length
