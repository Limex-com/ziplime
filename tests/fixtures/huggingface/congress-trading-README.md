---
license: cc0-1.0
language:
  - en
pretty_name: US Congress Trading Disclosures
tags:
  - finance
  - alternative-data
  - congress
  - stock-act
  - ziplime
configs:
  - config_name: trades
    data_files:
      - split: house
        path: data/trades/trades-house.parquet
      - split: senate
        path: data/trades/trades-senate.parquet
  - config_name: features
    data_files:
      - split: train
        path: data/features/daily-by-ticker.parquet
  - config_name: holdings
    data_files:
      - split: house
        path: data/holdings/holdings-house.parquet
  - config_name: liabilities
    data_files:
      - split: house
        path: data/liabilities/liabilities-house.parquet
  - config_name: filings
    data_files:
      - split: train
        path: data/filings/filings.parquet
  - config_name: legislators
    data_files:
      - split: train
        path: data/reference/legislators.parquet
  - config_name: legislator_terms
    data_files:
      - split: train
        path: data/reference/legislator_terms.parquet
  - config_name: committees
    data_files:
      - split: train
        path: data/reference/committees.parquet
  - config_name: committee_members
    data_files:
      - split: train
        path: data/reference/committee_members.parquet
---

