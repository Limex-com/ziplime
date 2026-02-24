To be able to successfully test your algorithms, the first step after installation is to ingest data you are going to
use for backtesting.

Ziplime can ingest market and fundamental data from various sources into normalized, queryable formats. Typical
ingestion
tasks:

- Fetch raw data from APIs/files
- Validate and normalize schema
- Store into local files or databases
- Create indexes and metadata for efficient access

There are 3 types of data: Assets data, market data, other data

### Assets data

Assets data contains information about tradeable assets, asset symbols, exchanges, options, futures etc.
It's kept in sqlite database.

Supported data sources are:

1) LimexHub
2) GRPC data source

Default assets data is provided in `data/assets.sqlite` database

### Market data

Historical OHLCV data

Supported data sources are:

1) LimexHub
2) YahooFinance
3) CSV file
4) GRPC data source

# Other data

Any other data that helps you in backtesting

Supported data sources are:

1) LimexHub
2) YahooFinance
3) CSV file
4) GRPC data source

