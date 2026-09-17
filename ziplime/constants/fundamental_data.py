import enum

from ziplime.domain.column_specification import ColumnSpecification


class FundamentalDataValueType(enum.Enum):
    VALUE = "value"
    TTM = "ttm"
    ADD_VALUE = "add_value"


class FundamentalData(enum.Enum):
    TOTAL_SHARE_HOLDER_EQUITY = "total_share_holder_equity"
    ROI = "roi"
    ROE = "roe"
    ROA = "roa"
    REVENUE = "revenue"
    RETURN_ON_TANGIBLE_EQUITY = "return_on_tangible_equity"
    QUICK_RATIO = "quick_ratio"
    PRICE_SALES = "price_sales"
    PRICE_FCF = "price_fcf"
    PE_RATIO = "pe_ratio"
    PRICE_BOOK = "price_book"
    OPERATING_MARGIN = "operating_margin"
    NET_WORTH = "net_worth"
    NET_INCOME = "net_income"
    NUMBER_OF_EMPLOYEES = "number_of_employees"
    GROSS_PROFIT = "gross_profit"
    EPS_FORECAST = "eps_forecast"
    NET_PROFIT_MARGIN = "net_profit_margin"
    DIVIDEND_YIELD = "dividend_yield"
    GROSS_MARGIN = "gross_margin"
    EBITDA = "ebitda"
    EPS = "eps"
    DEBT_EQUITY_RATIO = "debt_equity_ratio"
    CURRENT_RATIO = "current_ratio"
    LONG_TERM_DEBT = "long_term_debt"

    def property(self, value_type: FundamentalDataValueType):
        return self.value + "_" + value_type.value


FUNDAMENTAL_DATA_COLUMNS = [
    ColumnSpecification(name=f"{fd.value}_{fdt.value}", write_type="uint32", original_type='float', scale_factor=1000,
                        scaled_type="int")
    for fd in FundamentalData for fdt in FundamentalDataValueType
]

FUNDAMENTAL_DATA_COLUMN_NAMES = [
    c.name for c in
    FUNDAMENTAL_DATA_COLUMNS
]
