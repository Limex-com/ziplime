"""The scoped trading algorithm API context."""

from types import TracebackType
from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm
from ziplime.utils.algo_instance import get_algo_instance, set_algo_instance


class ZiplineAPI(object):
    """
    Context manager for making an algorithm instance available to ziplime API
    functions within a scoped block.
    """

    def __init__(self, algo_instance: BaseTradingAlgorithm) -> None:
        self.algo_instance = algo_instance

    def __enter__(self) -> None:
        """
        Set the given algo instance, storing any previously-existing instance.
        """
        self.old_algo_instance = get_algo_instance()
        set_algo_instance(self.algo_instance)

    def __exit__(
        self,
        _type: type[BaseException] | None,
        _value: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        """
        Restore the algo instance stored in __enter__.
        """
        set_algo_instance(self.old_algo_instance)
