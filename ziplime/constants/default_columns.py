from ziplime.constants.fundamental_data import FUNDAMENTAL_DATA_COLUMNS
from ziplime.domain.column_specification import ColumnSpecification

OHLCV_COLUMNS = [
                    ColumnSpecification(name="close", write_type="uint32", original_type='float'),
                    ColumnSpecification(name="open", write_type="uint32", original_type='float'),
                    ColumnSpecification(name="high", write_type="uint32", original_type='float'),
                    ColumnSpecification(name="low", write_type="uint32", original_type='float'),
                    ColumnSpecification(name="volume", write_type="uint32", original_type='float')
                ]

DEFAULT_COLUMNS = OHLCV_COLUMNS + FUNDAMENTAL_DATA_COLUMNS
