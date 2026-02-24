from datetime import datetime

from ziplime.finance.controls.account_control import AccountControl


class MinLeverage(AccountControl):
    """AccountControl representing a limit on the minimum leverage allowed
    by the algorithm after a threshold period of time.

    Parameters
    ----------
    min_leverage : float
        The gross leverage in decimal form.
    deadline : datetime
        The date the min leverage must be achieved by.

    For example, min_leverage=2 limits an algorithm to trading at minimum
    double the account value by the deadline date.
    """

    def __init__(self, min_leverage, deadline):
        super(MinLeverage, self).__init__(min_leverage=min_leverage, deadline=deadline)
        self.min_leverage = min_leverage
        self.deadline = deadline

    def validate(self, _portfolio, account, algo_datetime, _algo_current_data):
        """Make validation checks if we are after the deadline.
        Fail if the leverage is less than the min leverage.
        """
        if (
                algo_datetime > self.deadline.tz_localize(algo_datetime.tzinfo)
                and account.leverage < self.min_leverage
        ):
            self.fail()
