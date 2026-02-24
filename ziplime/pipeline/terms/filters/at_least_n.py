from ziplime.pipeline.terms.filters.custom_filter import CustomFilter


class AtLeastN(CustomFilter):
    """
    A Filter requiring that assets produce True for at least N days in the
    last ``window_length`` days.

    **Default Inputs:** None

    **Default Window Length:** None
    """

    params = ("N",)

    def compute(self, today, assets, out, arg, N):
        out[:] = arg.sum(axis=0) >= N
