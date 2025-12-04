class BadBinaryOperator(TypeError):
    """
    Called when a bad binary operation is encountered.

    Parameters
    ----------
    op : str
        The attempted operation
    left : ziplime.computable.Term
        The left hand side of the operation.
    right : ziplime.computable.Term
        The right hand side of the operation.
    """

    def __init__(self, op, left, right):
        super(BadBinaryOperator, self).__init__(
            "Can't compute {left} {op} {right}".format(
                op=op,
                left=type(left).__name__,
                right=type(right).__name__,
            )
        )
