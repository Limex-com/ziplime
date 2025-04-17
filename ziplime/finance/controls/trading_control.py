import abc

import structlog

from ziplime.errors import TradingControlViolation


class TradingControl(metaclass=abc.ABCMeta):
    """Abstract base class representing a fail-safe control on the behavior of any
    algorithm.
    """

    def __init__(self, on_error, **kwargs):
        """Track any arguments that should be printed in the error message
        generated by self.fail.
        """
        self.on_error = on_error
        self.__fail_args = kwargs
        self._logger = structlog.get_logger(__name__)

    @abc.abstractmethod
    def validate(self, asset, amount, portfolio, algo_datetime, algo_current_data):
        """Before any order is executed by TradingAlgorithm, this method should be
        called *exactly once* on each registered TradingControl object.

        If the specified asset and amount do not violate this TradingControl's
        restraint given the information in `portfolio`, this method should
        return None and have no externally-visible side-effects.

        If the desired order violates this TradingControl's contraint, this
        method should call self.fail(asset, amount).
        """
        raise NotImplementedError

    def _constraint_msg(self, metadata):
        constraint = repr(self)
        if metadata:
            constraint = "{constraint} (Metadata: {metadata})".format(
                constraint=constraint, metadata=metadata
            )
        return constraint

    def handle_violation(self, asset, amount, datetime, metadata=None):
        """Handle a TradingControlViolation, either by raising or logging and
        error with information about the failure.

        If dynamic information should be displayed as well, pass it in via
        `metadata`.
        """
        constraint = self._constraint_msg(metadata)

        if self.on_error == "fail":
            raise TradingControlViolation(
                asset=asset, amount=amount, datetime=datetime, constraint=constraint
            )
        elif self.on_error == "log":
            self._logger.error(
                "Order for %(amount)s shares of %(asset)s at %(dt)s "
                "violates trading constraint %(constraint)s",
                dict(amount=amount, asset=asset, dt=datetime, constraint=constraint),
            )

    def __repr__(self):
        return "{name}({attrs})".format(
            name=self.__class__.__name__, attrs=self.__fail_args
        )
