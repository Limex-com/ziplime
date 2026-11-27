import pytest

from ziplime.utils.zipline_api import ZiplineAPI
from ziplime.utils.api_support import (
    api_method,
    disallowed_in_before_trading_start,
    require_initialized,
    require_not_initialized,
)
from ziplime.utils.algo_instance import get_algo_instance
from ziplime.trading.base_trading_algorithm import BaseTradingAlgorithm


def test_zipline_api_context_manager():
    initial = get_algo_instance()

    dummy_algo = object()
    with ZiplineAPI(dummy_algo):
        assert get_algo_instance() is dummy_algo

    assert get_algo_instance() is initial


def test_base_algorithm_contexts_restore_outer_instance_after_exception():
    initial = get_algo_instance()
    outer, inner = BaseTradingAlgorithm(), BaseTradingAlgorithm()

    with ZiplineAPI(outer):
        with pytest.raises(ValueError, match="callback failed"):
            with ZiplineAPI(inner):
                assert get_algo_instance() is inner
                raise ValueError("callback failed")
        assert get_algo_instance() is outer

    assert get_algo_instance() is initial


def test_api_method_decorator():
    class DummyAlgo:
        @api_method
        def my_test_method(self, x):
            return x * 2

    algo = DummyAlgo()

    import ziplime.api
    assert hasattr(ziplime.api, "my_test_method")

    # Calling outside of simulation raises RuntimeError
    with pytest.raises(RuntimeError, match="must be called during a simulation"):
        ziplime.api.my_test_method(10)

    # Inside simulation
    with ZiplineAPI(algo):
        assert ziplime.api.my_test_method(10) == 20


def test_require_not_initialized():
    class DummyAlgo:
        initialized = False

        @require_not_initialized(ValueError("Already initialized"))
        def configure(self):
            return "configured"

    algo = DummyAlgo()
    assert algo.configure() == "configured"

    algo.initialized = True
    with pytest.raises(ValueError, match="Already initialized"):
        algo.configure()


def test_require_initialized():
    class DummyAlgo:
        initialized = False

        @require_initialized(ValueError("Not initialized"))
        def trade(self):
            return "trading"

    algo = DummyAlgo()
    with pytest.raises(ValueError, match="Not initialized"):
        algo.trade()

    algo.initialized = True
    assert algo.trade() == "trading"


def test_disallowed_in_before_trading_start():
    class DummyAlgo:
        _in_before_trading_start = False

        @disallowed_in_before_trading_start(ValueError("Not allowed in before_trading_start"))
        def order(self):
            return "ordered"

    algo = DummyAlgo()
    assert algo.order() == "ordered"

    algo._in_before_trading_start = True
    with pytest.raises(ValueError, match="Not allowed in before_trading_start"):
        algo.order()
