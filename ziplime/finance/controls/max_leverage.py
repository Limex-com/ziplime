import structlog

from ziplime.finance.controls.account_control import AccountControl


class MaxLeverage(AccountControl):
    """AccountControl representing a limit on the maximum leverage allowed
    by the algorithm.
    """

    def __init__(self, max_leverage:float, fail_on_error: bool = True,
                 logger = structlog.get_logger(__name__)):
        """max_leverage is the gross leverage in decimal form. For example,
        2, limits an algorithm to trading at most double the account value.
        """
        super(MaxLeverage, self).__init__(max_leverage=max_leverage)
        self.max_leverage = max_leverage
        self._logger = logger

        if max_leverage is None:
            raise ValueError("Must supply max_leverage")

        if max_leverage < 0:
            raise ValueError("max_leverage must be positive")
        self.fail_on_error = fail_on_error

    def validate(self, _portfolio, _account, _algo_datetime, _algo_current_data):
        """Fail if the leverage is greater than the allowed leverage."""
        if _account.leverage > self.max_leverage:
            if self.fail_on_error:
                self._logger.error(f"Current leverage {_account.leverage} exceeds max_leverage of {self.max_leverage}.", dt=_algo_current_data.current_dt)
                self.fail()
            else:
                self._logger.warning(f"Current leverage {_account.leverage} exceeds max_leverage of {self.max_leverage}.", dt=_algo_current_data.current_dt)
