import datetime
import uuid
from uuid import UUID

from sqlalchemy import Numeric, String, TextClause, ForeignKey
from sqlalchemy.orm import mapped_column
from typing_extensions import Annotated

IntegerPK = Annotated[int, mapped_column(primary_key=True)]
UuidPK = Annotated[UUID, mapped_column(primary_key=True)]
StringPK = Annotated[str, mapped_column(primary_key=True)]
UuidUnique = Annotated[UUID, mapped_column(unique=True, default_factory=uuid.uuid4,
                                           server_default=TextClause("gen_random_uuid()"))]
StringUnique = Annotated[str, mapped_column(unique=True)]
String32 = Annotated[str, mapped_column(String(length=32))]
String64 = Annotated[str, mapped_column(String(length=64))]
String256 = Annotated[str, mapped_column(String(length=256))]
String2048 = Annotated[str, mapped_column(String(length=2048))]
String4096 = Annotated[str, mapped_column(String(length=4096))]
String2048Indexed = Annotated[str, mapped_column(String(length=2048), index=True)]
StringIndexed = Annotated[str, mapped_column(index=True)]
StringDefaultEmpty = Annotated[str, mapped_column(default="", index=True, server_default="")]
Decimal8 = Annotated[float, mapped_column(Numeric(precision=12, scale=8))]
Decimal4 = Annotated[float, mapped_column(Numeric(precision=8, scale=4))]
Decimal2 = Annotated[float, mapped_column(Numeric(precision=8, scale=2))]
DateTimeIndexed = Annotated[datetime.datetime, mapped_column(index=True)]
DateIndexed = Annotated[datetime.date, mapped_column(index=True)]

BooleanDefaultTrue = Annotated[bool, mapped_column(default=True, server_default="TRUE")]
BooleanDefaultFalse = Annotated[bool, mapped_column(default=False, server_default="FALSE")]
IntegerDefaultZero = Annotated[int, mapped_column(default=0, server_default="0")]
IntegerIndexed = Annotated[int, mapped_column(index=True)]

ExchangeFK = Annotated[str, mapped_column(ForeignKey("exchanges.exchange"), index=True)]
EquityFK = Annotated[int, mapped_column(ForeignKey("equities.sid"), index=True)]
AssetFK = Annotated[int, mapped_column(ForeignKey("assets.sid"), index=True)]
AssetRouterFK = Annotated[int, mapped_column(ForeignKey("asset_router.sid"), index=True)]
CurrencyFK = Annotated[int, mapped_column(ForeignKey("currencies.sid"), index=True)]

AssetRouterFKPK = Annotated[int, mapped_column(ForeignKey("asset_router.sid"), primary_key=True)]
SymbolsUniverseFKPK = Annotated[int, mapped_column(ForeignKey("symbols_universe.id"), primary_key=True)]

FuturesRootSymbolFK = Annotated[str, mapped_column(ForeignKey("futures_root_symbols.root_symbol"), index=True)]
