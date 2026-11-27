import pickle

import pytest

from ziplime.currency import Currency
from ziplime.data.fx import DEFAULT_FX_RATE
from ziplime.pipeline.data import Column, DataSet
from ziplime.pipeline.data.currency_conversion import CurrencyConversion
from ziplime.pipeline.domain import Domain, GENERIC
from ziplime.pipeline.errors.invalid_classifier_comparison import InvalidClassifierComparison


class Prices(DataSet):
    close = Column(float, currency_aware=True)
    sector = Column(int, missing_value=-1)


class InheritedPrices(Prices):
    pass


def test_column_descriptor_binds_and_memoizes_each_dataset_owner():
    assert Prices.close.dataset is Prices
    assert Prices.get_column("close") is Prices.close
    assert InheritedPrices.close.dataset is InheritedPrices
    assert InheritedPrices.close is not Prices.close
    assert Prices().close is Prices.close


def test_column_specialization_preserves_owner_and_domain():
    concrete = Domain()
    specialized = Prices.close.specialize(concrete)

    assert specialized.dataset is Prices.specialize(concrete)
    assert specialized.domain is concrete
    assert specialized.unspecialize() is Prices.close
    assert Prices.close.domain is GENERIC


def test_currency_conversion_pickle_round_trip():
    conversion = CurrencyConversion(currency="USD", field=DEFAULT_FX_RATE)

    assert CurrencyConversion.__module__ == "ziplime.pipeline.data.currency_conversion"
    restored = pickle.loads(pickle.dumps(conversion))
    assert type(restored) is CurrencyConversion
    assert restored == conversion


def test_currency_aware_columns_keep_conversion_metadata():
    currency = Currency("USD")
    converted = Prices.close.fx(currency)

    assert converted.currency_conversion == CurrencyConversion(currency, DEFAULT_FX_RATE)
    assert converted.dataset is Prices
    assert converted is Prices.close.fx(currency)
    assert converted.qualname == "Prices.close.fx('USD')"


def test_invalid_classifier_comparison_keeps_its_error_message():
    with pytest.raises(InvalidClassifierComparison) as caught:
        Prices.sector.latest.eq("technology")

    assert str(caught.value) == (
        "Can't compare classifier of dtype int64 to value technology of type str."
    )
