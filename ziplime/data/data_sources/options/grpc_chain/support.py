"""Support for grpc_chain."""

from __future__ import annotations
import structlog
from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.domain.settlement_type import SettlementType


_logger = structlog.get_logger(__name__)


_OPTION_TYPES = {0: OptionType.CALL, 1: OptionType.PUT}


_EXERCISE_STYLES = {0: ExerciseStyle.EUROPEAN, 1: ExerciseStyle.AMERICAN}


_SETTLEMENT_TYPES = {
    1: SettlementType.PHYSICAL,
    2: SettlementType.CASH,
    3: SettlementType.PHYSICAL,   # SHARES: delivery of the underlying, which is physical
}
