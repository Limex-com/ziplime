"""Trade the basis between MOEX and NYMEX natural gas.

Both venues list a future on Henry Hub gas and both quote it in dollars per MMBtu, so the
difference between them is a real number rather than an FX artefact. They are not the same
contract: MOEX's is 100 MMBtu against NYMEX's 10 000, so the position is matched by *notional* --
roughly a hundred MOEX contracts to one NYMEX contract.

**Matching the delivery months is the whole trick.** MOEX's NG-10.25 stops trading in late October,
after the NYMEX October contract has already expired, so it settles against NYMEX **November**.
Pair the same-named months and you are not trading a basis at all -- measured over the sample,
MOEX Dec-25 against NYMEX Dec-25 has a mean gap of +$0.259 with a standard deviation of $0.061,
while against NYMEX Jan-26 it is -$0.023 with $0.049 and a correlation of 0.986. The first is a
calendar spread wearing a basis costume; the second is the arbitrage.

**Two asymmetries between the legs matter more than the signal.**

*Delivery.* NYMEX Henry Hub is **physically delivered** -- carry it past the last trading day and
you owe someone gas at a pipeline hub. MOEX's contract is cash-settled. ziplime force-closes a
deliverable position at its notice date, but that would close *one leg of a spread* and leave the
other naked, so the strategy exits the pair itself while both legs are still liquid.

*Margin currency.* Both contracts are quoted in dollars, but MOEX collects margin in **roubles**
and CME collects **dollars** (Finam's ``GetAssetParams`` reports the currency per contract, and it
is ``RUB`` for MOEX gas against a ``USD`` quote). The two requirements cannot be added, so the
example uses a per-contract margin model -- which is how exchanges publish margin anyway -- and
reads the requirement back per currency.

What this example shows: two chains on two exchanges, each resolved to its own live contract,
paired explicitly by delivery month, and managed around one leg's delivery obligation.
"""
import datetime
import statistics
from collections import deque

from ziplime.data.data_sources.finam.cme_futures import parse_cme_contract_name
from ziplime.data.data_sources.finam.moex_futures import parse_contract_name
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

STRATEGY_INFO = {
    "roots": ["NG", "NG.XNYM"],
    "bundle": "finam_natgas_arbitrage",
    "start": datetime.date(2023, 9, 1),
    "end": datetime.date(2026, 7, 31),
    "description": "Cross-venue natural gas basis: MOEX against NYMEX one month out",
    # Per-contract margin, in the currency each exchange actually collects. A rate-of-notional
    # model would have to convert dollars of notional into roubles of margin for the MOEX leg.
    "margin": "per_root",
}

WINDOW = 30                 # sessions of basis history before the rule may act
ENTRY_Z, EXIT_Z = 1.5, 0.4
NYMEX_CONTRACTS = 3         # the small leg; the MOEX leg is matched to its notional
#: Leave the deliverable leg alone this many days before its notice date. Letting the engine do it
#: would close one leg of the pair and leave the other unhedged.
DELIVERY_BUFFER_DAYS = 5


def _next_month(month: int, year: int) -> tuple[int, int]:
    return (1, year + 1) if month == 12 else (month + 1, year)


async def initialize(context: TradingAlgorithm):
    context.moex = await context.continuous_future("NG", offset=0, roll="volume", adjustment=None)
    context.nymex = await context.continuous_future("NG.XNYM", offset=0, roll="volume",
                                                    adjustment=None)
    context.basis_history = deque(maxlen=WINDOW)
    context.legs = {}           # root symbol -> (contract, amount)
    context.direction = 0       # +1 long the basis (long MOEX, short NYMEX), -1 the other way
    context.delivery_exits = 0  # how often the pair was closed to stay clear of delivery
    context.reported_margin = False


async def _close_leg(context, root_symbol):
    contract, amount = context.legs.get(root_symbol, (None, 0))
    if contract is not None and amount:
        await context.order(asset=contract, amount=-amount, style=MarketOrder())
    context.legs[root_symbol] = (None, 0)


async def _flatten(context):
    await _close_leg(context, "NG")
    await _close_leg(context, "NG.XNYM")
    context.direction = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    moex_contract = await data.current_contract(context.moex)
    if moex_contract is None:
        return
    moex_month = parse_contract_name(moex_contract.asset.asset_name)
    if moex_month is None:
        return

    # The NYMEX contract for the delivery month after MOEX's: that is what MOEX settles against.
    wanted_month, wanted_year = _next_month(*moex_month)
    nymex_contract = None
    for candidate in await data.current_chain(context.nymex):
        parsed = parse_cme_contract_name(candidate.asset.asset_name)
        if parsed == (wanted_month, wanted_year):
            nymex_contract = candidate
            break
    if nymex_contract is None:
        await _flatten(context)
        return

    quotes = await data.current(assets=[moex_contract, nymex_contract], fields=["close", "volume"])
    if len(quotes) < 2:
        return
    prices = dict(zip(quotes["sid"].to_list(), quotes["close"].to_list()))
    volumes = dict(zip(quotes["sid"].to_list(), quotes["volume"].to_list()))
    moex_price = prices.get(moex_contract.sid)
    nymex_price = prices.get(nymex_contract.sid)
    if not moex_price or not nymex_price:
        return
    # Both venues must actually have traded. A forward-filled bar carries zero volume, and a basis
    # computed against a stale price is not one anybody could have traded on.
    if not volumes.get(moex_contract.sid) or not volumes.get(nymex_contract.sid):
        return

    # The NYMEX leg delivers physical gas. Get the whole pair out before its notice date rather
    # than letting the engine close that leg and leave the MOEX one unhedged.
    today = context.get_datetime().date()
    for contract in (nymex_contract, moex_contract):
        if not contract.asset.is_deliverable:
            continue
        days_to_notice = (contract.asset.notice_date - today).days
        if days_to_notice <= DELIVERY_BUFFER_DAYS:
            if context.direction:
                context.delivery_exits += 1
                await _flatten(context)
            return

    if not context.reported_margin:
        # Two venues, two margin currencies: MOEX collects roubles even for this dollar-quoted
        # contract, CME collects dollars. Reported separately because adding them would need an
        # FX rate the ledger does not carry.
        posted = context.futures_margin_by_currency()
        if posted:
            context.reported_margin = True
            print(f"\n  margin posted per currency: "
                  f"{ {k: round(v, 2) for k, v in posted.items()} }")
            print(f"  NYMEX leg {nymex_contract.symbol}: deliverable="
                  f"{nymex_contract.asset.is_deliverable}, margin in "
                  f"{nymex_contract.asset.margin_currency}, notice "
                  f"{nymex_contract.asset.notice_date}")
            print(f"  MOEX  leg {moex_contract.symbol}: deliverable="
                  f"{moex_contract.asset.is_deliverable}, margin in "
                  f"{moex_contract.asset.margin_currency}, quoted in USD\n")

    basis = moex_price - nymex_price
    context.basis_history.append(basis)
    if len(context.basis_history) < WINDOW:
        return

    mean = statistics.fmean(context.basis_history)
    spread = statistics.pstdev(context.basis_history)
    if spread <= 0:
        return
    z = (basis - mean) / spread

    # Either leg rolling ends the position: the pair has to be re-established on the new contracts.
    for root_symbol, contract in (("NG", moex_contract), ("NG.XNYM", nymex_contract)):
        held, _ = context.legs.get(root_symbol, (None, 0))
        if held is not None and held.sid != contract.sid:
            await _flatten(context)
            break

    if context.direction and abs(z) <= EXIT_Z:
        await _flatten(context)
        return

    if not context.direction and abs(z) >= ENTRY_Z:
        # Fade the deviation: a rich basis is sold, a cheap one bought.
        direction = -1 if z > 0 else 1
        nymex_notional = context.notional_exposure(asset=nymex_contract, amount=NYMEX_CONTRACTS,
                                                   price=nymex_price)
        moex_amount = context.contracts_for_notional(asset=moex_contract,
                                                     notional=nymex_notional, price=moex_price)
        if not moex_amount:
            return
        await context.order(asset=moex_contract, amount=direction * moex_amount,
                            style=MarketOrder())
        await context.order(asset=nymex_contract, amount=-direction * NYMEX_CONTRACTS,
                            style=MarketOrder())
        context.legs["NG"] = (moex_contract, direction * moex_amount)
        context.legs["NG.XNYM"] = (nymex_contract, -direction * NYMEX_CONTRACTS)
        context.direction = direction


