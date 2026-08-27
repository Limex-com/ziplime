"""What a backtest does and does not model.

A backtest that runs cleanly is not the same as a backtest that is realistic. This module states
the gaps explicitly so a result can be read with them in mind, rather than leaving them to be
rediscovered later.
"""
import dataclasses

from ziplime.finance.margin import FuturesMarginModel


@dataclasses.dataclass(frozen=True)
class RealismWarning:
    """One effect the simulation does not reproduce."""

    code: str
    detail: str

    def __str__(self) -> str:
        return f"{self.code}: {self.detail}"


#: Effects no execution model in ziplime currently reproduces.
ALWAYS_UNMODELLED = (
    RealismWarning(
        "NO PRICE LIMITS",
        "limit-up, limit-down and locked markets are not modelled. A bar with volume and a last "
        "price is assumed fillable, so a strategy can trade through a halt it could not have "
        "traded through.",
    ),
    RealismWarning(
        "DELIVERY IS AVOIDED, NOT SIMULATED",
        "a physically delivered contract is force-closed at its notice date so the backtest never "
        "takes delivery, but assignment, delivery costs and the squeeze risk of being caught long "
        "into a delivery window are not modelled. Cash settlement at expiry is likewise a plain "
        "liquidation at the last mark.",
    ),
    RealismWarning(
        "SINGLE-CURRENCY CASH",
        "the portfolio holds one untyped cash balance. A book spanning venues that collect margin "
        "in different currencies -- MOEX takes roubles even for its dollar-quoted contracts -- has "
        "its requirements reported per currency, but P&L and cash are still added up as one "
        "number. Cross-currency results need an FX series the ledger does not carry.",
    ),
)


#: Effects a bond backtest does not reproduce, whatever it is configured with.
BONDS_UNMODELLED = (
    RealismWarning(
        "NO CREDIT RISK",
        "an issuer always pays. Default, restructuring and a coupon that simply does not arrive "
        "are not modelled, so a high-yield issue shows its yield without the risk that earns it.",
    ),
    RealismWarning(
        "OFFERS ARE NOT EXERCISED",
        "a put or call window is recorded in the schedule and left there. A strategy can read it "
        "and trade around it, but nothing exercises it, and a bond whose issuer would have called "
        "it is carried to maturity instead.",
    ),
    RealismWarning(
        "FORWARD COUPONS ARE ASSUMED FIXED",
        "coupons already paid come from the vendor's calendar and are exact. Coupons still ahead "
        "of an issue are generated from its last known rate, which is right for a fixed-coupon "
        "bond and wrong for a floater, whose future rate nobody knows.",
    ),
)


def realism_warnings(futures_margin_model: FuturesMarginModel | None = None,
                     same_bar_execution: bool = False,
                     trades_futures: bool = True,
                     trades_bonds: bool = False) -> list[RealismWarning]:
    """Return the realism gaps that apply to a given simulation configuration.

    Args:
        futures_margin_model: The margin model in use, if any.
        same_bar_execution: Whether orders fill on the bar that produced the decision.
        trades_futures: Whether the simulation trades futures at all.
        trades_bonds: Whether the simulation trades bonds at all.
    """
    warnings: list[RealismWarning] = []
    if same_bar_execution:
        warnings.append(RealismWarning(
            "SAME-BAR EXECUTION",
            "orders fill on the bar that produced the decision, so a close-based rule is filled "
            "at a close the market had not printed yet. Results are optimistic by about one bar.",
        ))
    if trades_bonds:
        warnings.extend(BONDS_UNMODELLED)
    if not trades_futures:
        return warnings
    if futures_margin_model is None or not futures_margin_model.models_margin:
        warnings.append(RealismWarning(
            "NO FUTURES MARGIN MODEL",
            "margin, buying power and margin calls are not simulated, so leverage is "
            "unconstrained. Position PnL is still correct.",
        ))
    warnings.extend(ALWAYS_UNMODELLED)
    return warnings


def format_realism_warnings(warnings: list[RealismWarning]) -> str:
    """Render warnings as the report block a reviewer expects to see."""
    if not warnings:
        return "REALISM / DATA WARNINGS\n  none"
    lines = ["REALISM / DATA WARNINGS"]
    for warning in warnings:
        lines.append(f"  - {warning.code}")
        lines.append(f"      {warning.detail}")
    return "\n".join(lines)
