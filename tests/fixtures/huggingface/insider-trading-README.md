---
license: apache-2.0
language:
  - en
pretty_name: ZipLime US Insider Trading Disclosures (PIT)
tags:
  - finance
  - sec
  - edgar
  - insider-trading
  - point-in-time
  - alternative-data
  - ziplime
configs:
  - config_name: transactions
    default: true
    data_dir: data/transactions
    data_files:
      - split: train
        path: "knowledge_year=*/knowledge_month=*/*.parquet"
  - config_name: filings
    data_dir: data/filings
    data_files:
      - split: train
        path: "knowledge_year=*/knowledge_month=*/*.parquet"
  - config_name: owners
    data_dir: data/owners
    data_files:
      - split: train
        path: "knowledge_year=*/knowledge_month=*/*.parquet"
  - config_name: features
    data_dir: data/features
    data_files:
      - split: train
        path: "knowledge_year=*/*.parquet"
  - config_name: pit
    data_dir: data/pit
    data_files:
      - split: train
        path: "knowledge_year=*/*.parquet"
---

