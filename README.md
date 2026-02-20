# 01-modern-lakehouse-architecture
Arquitetura:  Ingestion → Bronze (raw parquet) → Silver (cleaned, deduplicated) → Gold (business marts com dbt)  Stack :  DuckDB dbt-core Python Docker Makefile

modern-lakehouse-architecture/
│
├── ingestion/
│   ├── extract.py
│   ├── load_bronze.py
│   └── sources.yaml
│
├── lakehouse/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── dbt/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   ├── tests/
│   └── dbt_project.yml
│
├── docker/
│   └── Dockerfile
│
├── scripts/
│   ├── run_pipeline.sh
│   └── run_dbt.sh
│
├── Makefile
├── docker-compose.yml
├── requirements.txt
└── README.md
