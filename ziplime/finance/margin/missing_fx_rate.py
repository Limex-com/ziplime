"""Missing fx rate for margin."""


class MissingFxRate(ValueError):
    """Raised when margin would have to cross currencies without a rate to do it with."""

    def __init__(self, quote_currency: str, margin_currency: str, root_symbol: str | None = None) -> None:
        super().__init__(
            f"Margin for {root_symbol or 'this contract'} is posted in {margin_currency} but the "
            f"contract is quoted in {quote_currency}. A rate-of-notional model cannot convert "
            f"between them; pass fx_rates={{('{quote_currency}', '{margin_currency}'): rate}} or "
            f"use PerRootFuturesMarginModel, which quotes margin per contract in the currency the "
            f"exchange actually collects."
        )
        self.quote_currency = quote_currency
        self.margin_currency = margin_currency
