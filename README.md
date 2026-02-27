graph TD
    subgraph Ingestion_Layer [Ingestion Layer]
        A[Raw Data: CSV/JSON/XLSX] -->|Sensor Check| B(Bash: ingest.sh)
        B -->|Validation| C[Staging Folder]
    end

    subgraph Processing_Layer [Processing & AI Layer]
        C --> D[Python ETL]
        D -->|AI Anomaly Detection| E{Z-Score Check}
        E -->|Outlier| F[Quality Log]
        E -->|Clean| G[Transformation: Green Metrics]
    end

    subgraph Storage_Layer [Storage Layer]
        G --> H[PgBouncer: Port 6432]
        H --> I[(Postgres: Range Partitioned)]
        I -->|SCD Type 2| J[dim_product / dim_customer]
    end

    subgraph Monitoring_Layer [Visualization Layer]
        I --> K[Streamlit Dashboard]
        F --> K
    end

    style G fill:#90ee90,stroke:#333,stroke-width:2px
    style E fill:#ffcccb,stroke:#333

    Data Models

    erDiagram
    FACT_SALES ||--o{ DIM_PRODUCT : "product_id"
    FACT_SALES ||--o{ DIM_CUSTOMER : "customer_id"
    FACT_SALES ||--o{ DIM_DATE : "date_id"
    FACT_SALES ||--o{ DIM_LOCATION : "location_id"

    FACT_SALES {
        int sale_id PK
        timestamp sale_timestamp "Partition Key"
        float revenue
        float carbon_savings
        int quantity_sold
    }

    DIM_PRODUCT {
        int product_id PK
        string product_name
        float price
        boolean is_current "SCD Type 2"
        timestamp effective_start
    }

    DIM_CUSTOMER {
        int customer_id PK
        string email
        string loyalty_level "SCD Type 2"
        timestamp effective_end
    }