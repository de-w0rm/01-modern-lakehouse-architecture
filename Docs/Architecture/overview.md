# Modern Lakehouse Architecture (DuckDB + dbt)

## Goals
- Reproducible local lakehouse using free tooling
- Medallion architecture: Bronze (raw) → Silver (clean) → Gold (marts)
- Incremental-ready modeling and partition-aware storage layout
- Automated data quality + basic observability

## Architecture Diagram
```mermaid

flowchart LR
  %% ======================
  %% Modern Lakehouse (Free Stack)
  %% DuckDB + dbt-core + Python + Docker + Makefile
  %% ======================

  subgraph Sources[Source Systems]
    S1[(SaaS API)]
    S2[(Files: CSV/JSON)]
    S3[(DB Extract)]
  end

  subgraph Ingestion[Ingestion Layer (Python)]
    I1[Extract\n(rate limits, retries)]
    I2[Land Raw\n(immutable)]
    I3[Validate Schema\n(basic checks)]
  end

  subgraph Bronze[Bronze Zone (Raw Parquet)]
    B1[(bronze/*.parquet)]
    B2[Partitioning Strategy\n(dt=YYYY-MM-DD)\nsource=...]
  end

  subgraph Compute[Compute / Query Engine]
    D1[DuckDB\n(local compute)]
  end

  subgraph dbtLayer[Transformation Layer (dbt-core)]
    T1[Bronze Models\n(staging, typing)]
    T2[Silver Models\n(clean, dedupe, conform)]
    T3[Gold Marts\n(business-ready)]
  end

  subgraph Silver[Silver Zone (Cleaned Parquet)]
    SIV[(silver/*.parquet)]
  end

  subgraph Gold[Gold Zone (Marts Parquet)]
    G1[(gold/*.parquet)]
    G2[Semantic Marts\nfacts/dimensions]
  end

  subgraph Quality[Quality + Observability]
    Q1[dbt tests\n(unique, not_null,\nrelationships)]
    Q2[Data contracts\n(schema.yml)]
    Q3[Structured logging\n(json logs)]
  end

  subgraph Orchestration[Orchestration / DevEx]
    O1[Makefile\n(one-command runs)]
    O2[Docker\n(repro env)]
    O3[CI\n(GitHub Actions)\nlint + tests]
  end

  %% Flows
  S1 --> I1
  S2 --> I1
  S3 --> I1

  I1 --> I2 --> I3 --> B1
  B1 --> D1
  D1 --> T1 --> SIV --> T2 --> G1
  T2 --> T3 --> G2

  T1 --> Q1
  T2 --> Q1
  T3 --> Q1
  Q2 --> Q1
  I1 --> Q3
  T2 --> Q3

  O2 --> Ingestion
  O2 --> dbtLayer
  O1 --> Ingestion
  O1 --> dbtLayer
  O3 --> Quality