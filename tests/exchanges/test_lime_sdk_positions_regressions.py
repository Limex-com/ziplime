import unittest
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from lime_trader import AsyncLimeClient
from lime_trader.models.accounts import AccountPosition, SecurityType

from tests.assets.repositories.test_sqlalchemy_asset_repository import make_currency, make_equity
from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.services.asset_service import AssetService
from ziplime.exchanges.lime_trader_sdk import lime_trader_sdk_exchange as sdk_exchange
from ziplime.utils.calendar_utils import get_calendar


class LimeSdkPositionRegressions(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.client = Mock(spec=AsyncLimeClient)
        self.client.account = Mock(
            get_positions=AsyncMock(),
            get_balances=AsyncMock(),
        )
        self.client.market = Mock()
        self.asset_service = Mock(spec=AssetService)
        self.exchange = sdk_exchange.LimeTraderSdkExchange(
            name="LIME", canonical_name="LIME", country_code="US",
            trading_calendar=get_calendar("XNYS"), clock=Mock(),
            start_cash_balance=1000.0, account_id="account", is_default=True,
            asset_service=self.asset_service, client=self.client, logger=Mock(),
        )
        self.asset = ExchangeAsset(
            sid=7, symbol="ABC", start_date=None, end_date=None, first_traded=None,
            auto_close_date=None, external_id="", exchange=ExchangeInfo("XNAS", "NASDAQ", "NASDAQ", "US"),
            asset=make_equity(), quote=make_currency(),
        )
        self.asset_service.get_exchange_asset_by_symbol.return_value = self.asset
        self.position = AccountPosition(
            symbol="ABC", quantity=-3, average_open_price=Decimal("12.50"),
            current_price=Decimal("14.25"), security_type=SecurityType.COMMON_STOCK,
        )
        self.client.account.get_positions.return_value = [self.position]
        self.balance = SimpleNamespace(
            account_number="account", account_value_total=Decimal("957.25"),
            position_market_value=Decimal("-42.75"), cash=Decimal("1000"),
            margin_buying_power=Decimal("2000"), cash_to_withdraw=Decimal("1000"),
            non_margin_buying_power=Decimal("1000"), daytrades_count=0,
        )
        self.client.account.get_balances.return_value = [self.balance]

    async def test_positions_resolve_domain_listings_and_await_sdk(self):
        result = await self.exchange.get_positions()

        self.client.account.get_balances.assert_awaited_once_with()
        self.client.account.get_positions.assert_awaited_once_with(
            account_number="account", date=None, strategy=None,
        )
        self.asset_service.get_exchange_asset_by_symbol.assert_awaited_once_with(
            symbol=AssetSymbol(symbol="ABC", mic="XNGS"), asset_type=AssetType.EQUITY,
        )
        self.assertEqual(self.client.market.mock_calls, [])
        self.assertEqual(list(result), [self.asset])
        position = result[self.asset]
        self.assertEqual(position.amount, -3)
        self.assertEqual(position.cost_basis, 12.5)
        self.assertEqual(position.last_sale_price, 14.25)
        self.assertIsNone(position.last_sale_date)
        self.assertEqual((position.exchange_name, position.trading_account_id), ("LIME", "account"))

    async def test_empty_positions_do_not_resolve_assets_or_fetch_quotes(self):
        self.client.account.get_positions.return_value = []

        self.assertEqual(await self.exchange.get_positions(), {})
        self.asset_service.get_exchange_asset_by_symbol.assert_not_awaited()
        self.assertEqual(self.client.market.mock_calls, [])

    async def test_asset_resolution_errors_propagate(self):
        self.asset_service.get_exchange_asset_by_symbol.side_effect = RuntimeError("asset repository unavailable")

        with self.assertRaisesRegex(RuntimeError, "asset repository unavailable"):
            await self.exchange.get_positions()
        self.assertEqual(self.client.market.mock_calls, [])

    async def test_unknown_symbols_are_logged_and_skipped(self):
        self.asset_service.get_exchange_asset_by_symbol.return_value = None

        self.assertEqual(await self.exchange.get_positions(), {})
        self.exchange._logger.warning.assert_called_once_with(
            "Lime position in an instrument the asset database lacks; left out of the portfolio",
            symbol="ABC", quantity=-3,
        )

    async def test_positions_use_broker_mark_without_a_quote_request(self):
        self.client.market.get_current_quotes = AsyncMock(side_effect=RuntimeError("quotes unavailable"))

        positions = await self.exchange.get_positions()

        self.assertEqual(positions[self.asset].last_sale_price, 14.25)
        self.client.market.get_current_quotes.assert_not_called()

    async def test_portfolio_uses_scoped_position_keys_and_async_balances(self):
        portfolio = await self.exchange.get_portfolio()

        self.assertEqual(list(portfolio.positions), [("LIME", "account", self.asset)])
        self.assertEqual(portfolio.portfolio_value, 957.25)
        self.assertEqual(portfolio.cash, 1000.0)
        self.assertEqual(
            await portfolio.get_exchange_asset_positions_amount(
                self.asset, exchange_name="LIME", trading_account_id="account",
            ),
            -3,
        )
        self.client.account.get_balances.assert_awaited_once_with()
        self.client.account.get_positions.assert_awaited_once_with(
            account_number="account", date=None, strategy=None,
        )

    async def test_account_uses_the_selected_async_balance(self):
        self.client.account.get_balances.return_value.insert(
            0, SimpleNamespace(account_number="other"),
        )

        account = await self.exchange.get_account()

        self.assertEqual(account.net_liquidation, 957.25)
        self.assertEqual(account.settled_cash, 1000.0)
        self.assertEqual(account.total_positions_value, -42.75)
        self.client.account.get_balances.assert_awaited_once_with()
        self.client.account.get_positions.assert_not_awaited()
