---
license: apache-2.0
language:
  - en
pretty_name: ZipLime Institutional Fund Portfolios (PIT)
tags:
  - finance
  - sec
  - edgar
  - 13f
  - institutional-holdings
  - point-in-time
  - alternative-data
  - ziplime
configs:
  - config_name: positions
    default: true
    data_dir: data/positions/fund_portfolios.delta
    data_files:
      - split: train
        path: "*.parquet"
  - config_name: filings
    data_dir: data/filings
    data_files:
      - split: train
        path: "source=*/*.parquet"
  - config_name: holdings
    data_dir: data/holdings
    data_files:
      - split: train
        path: "source=*/*.parquet"
  - config_name: filing_flags
    data_dir: data/quality
    data_files:
      - split: train
        path: "filing_flags.parquet"
  - config_name: unit_corrections
    data_dir: data/quality
    data_files:
      - split: train
        path: "unit_corrections.parquet"
---

