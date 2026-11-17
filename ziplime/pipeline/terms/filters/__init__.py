from .array_predicate import ArrayPredicate
from .custom_filter import CustomFilter
from .all_present import AllPresent
from .filter import Filter
from .latest import Latest
from .not_null_filter import NotNullFilter
from .null_filter import NullFilter
from .num_expr_filter import NumExprFilter
from .single_asset import SingleAsset
from .all import All
from .any import Any
from .at_least_n import AtLeastN
from .static_assets import StaticAssets
from .static_sids import StaticSids

# Declare the lazy names for source inspection; __init__.pyi provides their exact public types.
MaximumFilter: type[Filter]
PercentileFilter: type[Filter]

__all__ = [
    "All",
    "AllPresent",
    "Any",
    "ArrayPredicate",
    "AtLeastN",
    "CustomFilter",
    "Filter",
    "Latest",
    "MaximumFilter",
    "NotNullFilter",
    "NullFilter",
    "NumExprFilter",
    "PercentileFilter",
    "SingleAsset",
    "StaticAssets",
    "StaticSids",
]


def __getattr__(name: str) -> type[Filter]:
    # These filters depend on Factor and Classifier, which import the base filters.
    if name == "MaximumFilter":
        from .maximum_filter import MaximumFilter

        globals()[name] = MaximumFilter
        return MaximumFilter
    if name == "PercentileFilter":
        from .percentile_filter import PercentileFilter

        globals()[name] = PercentileFilter
        return PercentileFilter
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
