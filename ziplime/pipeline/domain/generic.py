"""Generic for domain."""

import pandas as pd
from ziplime.pipeline.domain.base import Domain


class GenericDomain(Domain):
    """Special singleton class used to represent generic DataSets and Columns."""

    def sessions(self) -> pd.DatetimeIndex:
        raise NotImplementedError("Can't get sessions for generic domain.")

    @property
    def country_code(self) -> str:
        raise NotImplementedError("Can't get country code for generic domain.")

    def data_query_cutoff_for_sessions(self, sessions: pd.DatetimeIndex) -> pd.DatetimeIndex:
        raise NotImplementedError(
            "Can't compute data query cutoff times for generic domain.",
        )

    def __repr__(self) -> str:
        return "GENERIC"


GENERIC = GenericDomain()
