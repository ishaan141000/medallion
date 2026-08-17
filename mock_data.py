PRESETS = {
    "ecommerce": {
        "name": "E-Commerce Transaction Logs (Messy CSV)",
        "source_name": "Web Server Transaction Log",
        "source_format": "CSV",
        "source_path": "/mnt/raw/webshop/transactions/*.csv",
        "bronze_table_name": "raw_transactions",
        "silver_table_name": "conformed_transactions",
        "gold_model_name": "revenue_by_category_daily",
        "legacy_schema": """order_id,cust_id,txn_dt,item_details,gross_amt,tax_rate,status,p_method
O-9982,C_1082,2026/08/01 14:22:11,"[101:2:19.99|105:1:4.99]",44.97,0.08,COMPLETED,cc
O-9983,C_2201,02-Aug-2026,"[203:1:89.00]",89.00,0.00,PENDING,paypal
O-9984,C_1082,2026-08-02 09:15:00,NULL,0.00,0.08,CANCELLED,cc
O-9985,C_4022,2026/08/03,"[102:4:9.99]",39.96,0.05,COMPLETED,giftcard""",
        "silver_transformations": """1. Parse item_details array from custom string format [item_id:quantity:unit_price|...] to separate line items if necessary, or clean and cast values.
2. Standardize column names: order_id -> order_id, cust_id -> customer_id, txn_dt -> transaction_timestamp, gross_amt -> gross_amount, tax_rate -> tax_rate, status -> order_status, p_method -> payment_method.
3. Cast transaction_timestamp from varying formats ('YYYY/MM/DD HH:MM:SS', 'DD-Mon-YYYY', 'YYYY-MM-DD') into proper TimestampType.
4. Deduplicate by order_id, keeping the latest record.
5. Create surrogate key `sk_transaction` using SHA2 of (order_id, customer_id).
6. Replace null gross_amount with 0.00.""",
        "business_requirements": "Group sales by day and item category, calculate daily gross revenue, total tax collected, and compute a rolling 7-day average of order counts.",
        "bronze_code": """# Bronze Ingestion Script - Auto Loader
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.appName("BronzeIngestion-Ecom").getOrCreate()

# Schema defined for initial parsing (can evolve)
source_path = "dbfs:/mnt/raw/webshop/transactions/"
target_table = "bronze.raw_transactions"

# Auto Loader stream configuration
df_bronze = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", "dbfs:/mnt/schemas/raw_transactions")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  .option("header", "true")
  .load(source_path)
)

# Append Bronze Metadata
df_bronze_enriched = (df_bronze
  .withColumn("_ingested_at", current_timestamp())
  .withColumn("_source_file", input_file_name())
)

# Write to Delta table
query = (df_bronze_enriched.writeStream
  .format("delta")
  .option("checkpointLocation", "dbfs:/mnt/checkpoints/raw_transactions")
  .outputMode("append")
  .toTable(target_table)
)
""",
        "silver_code": """# Silver Transformation Script - Conformed Transactions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, coalesce, lit, sha2, concat_ws, row_number
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SilverTransformation-Ecom").getOrCreate()

# Load Bronze data
df_bronze = spark.read.table("bronze.raw_transactions")

# Deduplicate based on order_id
window_spec = Window.partitionBy("order_id").orderBy(col("_ingested_at").desc())
df_deduped = df_bronze.withColumn("rn", row_number().over(window_spec)).filter("rn = 1").drop("rn")

# Perform cleanses, name mapping, and data type casting
df_silver = (df_deduped
  .select(
    # Create surrogate key
    sha2(concat_ws("||", col("order_id"), col("cust_id")), 256).alias("sk_transaction"),
    col("order_id").alias("order_id"),
    col("cust_id").alias("customer_id"),
    
    # Cast Varying Date Formats
    coalesce(
      to_timestamp(col("txn_dt"), "yyyy/MM/dd HH:mm:ss"),
      to_timestamp(col("txn_dt"), "dd-MMM-yyyy"),
      to_timestamp(col("txn_dt"), "yyyy-MM-dd HH:mm:ss"),
      to_timestamp(col("txn_dt"), "yyyy-MM-dd")
    ).alias("transaction_timestamp"),
    
    col("item_details").alias("raw_item_details"),
    coalesce(col("gross_amt").cast("decimal(18,2)"), lit(0.00)).alias("gross_amount"),
    coalesce(col("tax_rate").cast("decimal(5,4)"), lit(0.00)).alias("tax_rate"),
    coalesce(col("status"), lit("UNKNOWN")).alias("order_status"),
    
    # Map payment method abbreviation to words
    col("p_method").alias("payment_method_raw"),
    (col("p_method")
      .when(col("p_method") == "cc", "Credit Card")
      .when(col("p_method") == "paypal", "PayPal")
      .when(col("p_method") == "giftcard", "Gift Card")
      .otherwise("Other")
    ).alias("payment_method")
  )
)

# Write to Silver Delta Table
df_silver.write \\
  .format("delta") \\
  .mode("overwrite") \\
  .option("overwriteSchema", "true") \\
  .saveAsTable("silver.conformed_transactions")
""",
        "gold_sql": """-- models/gold/revenue_by_category_daily.sql
{{ config(materialized='table') }}

WITH transactions AS (
    SELECT 
        sk_transaction,
        order_id,
        customer_id,
        CAST(transaction_timestamp AS DATE) AS transaction_date,
        gross_amount,
        tax_rate,
        order_status,
        -- Quick parser for item details (assumed structured list for analytics)
        raw_item_details
    FROM {{ ref('conformed_transactions') }}
    WHERE order_status = 'COMPLETED'
),

daily_base AS (
    SELECT 
        transaction_date,
        COUNT(DISTINCT order_id) AS order_count,
        SUM(gross_amount) AS daily_gross_revenue,
        SUM(gross_amount * tax_rate) AS daily_tax_collected
    FROM transactions
    GROUP BY 1
),

rolling_metrics AS (
    SELECT
        transaction_date,
        order_count,
        daily_gross_revenue,
        daily_tax_collected,
        -- Rolling 7-day average of order counts
        AVG(order_count) OVER (
            ORDER BY transaction_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg_orders
    FROM daily_base
)

SELECT * FROM rolling_metrics""",
        "gold_yml": """# models/gold/revenue_by_category_daily.yml
version: 2

models:
  - name: revenue_by_category_daily
    description: "Daily aggregated e-commerce gross revenue, tax collected, and 7-day rolling order counts."
    columns:
      - name: transaction_date
        description: "The calendar date when the transaction was completed."
        tests:
          - not_null
          - unique
      - name: order_count
        description: "Number of unique orders completed on this date."
        tests:
          - not_null
      - name: daily_gross_revenue
        description: "Total gross amount of completed orders before taxes."
        tests:
          - not_null
      - name: rolling_7d_avg_orders
        description: "7-day rolling average of completed orders."
        tests:
          - not_null
""",
        "mermaid": """graph LR
    subgraph Legacy Layer
        web_server["Web Server File Logs (CSV)"]
    end

    subgraph Bronze Layer [Bronze Layer]
        bronze_table["raw_transactions<br/>(Delta Table)"]
        auto_loader["Auto Loader<br/>Rescue Schema"]
    end

    subgraph Silver Layer [Silver Layer]
        silver_table["conformed_transactions<br/>(Delta Table)"]
        silver_ops["Deduplicate & Parse Date<br/>Hash sk_transaction"]
    end

    subgraph Gold Layer [Gold Layer]
        gold_model["revenue_by_category_daily<br/>(dbt Table Model)"]
    end

    web_server -->|Stream CSVs| auto_loader
    auto_loader -->|Write Raw| bronze_table
    bronze_table -->|Read Stream| silver_ops
    silver_ops -->|Overwrite Clean| silver_table
    silver_table -->|dbt compile| gold_model

    classDef legacy fill:#F8D7DA,stroke:#F5C6CB,stroke-width:2px,color:#721C24;
    classDef bronze fill:#FFF3CD,stroke:#FFEEBA,stroke-width:2px,color:#856404;
    classDef silver fill:#D1ECF1,stroke:#BEE5EB,stroke-width:2px,color:#0C5460;
    classDef gold fill:#D4EDDA,stroke:#C3E6CB,stroke-width:2px,color:#155724;

    class web_server legacy;
    class bronze_table,auto_loader bronze;
    class silver_table,silver_ops silver;
    class gold_model gold;"""
    },
    "mainframe": {
        "name": "Legacy Mainframe Core Banking (COBOL DB2 DDL)",
        "source_name": "Mainframe DB2 DB",
        "source_format": "SQL Server DDL",
        "source_path": "/mnt/mainframe/db2/customer_master/",
        "bronze_table_name": "raw_cust_mast",
        "silver_table_name": "conformed_customer_profile",
        "gold_model_name": "customer_lifetime_value",
        "legacy_schema": """CREATE TABLE CUST_MAST_REC (
    CUST_ID CHAR(10) NOT NULL,
    FIRST_NAME CHAR(25) NOT NULL,
    MID_INIT CHAR(1),
    LAST_NAME CHAR(30) NOT NULL,
    OPEN_DT CHAR(8) NOT NULL, -- Format YYYYMMDD
    ACCT_TYPE CHAR(3) NOT NULL, -- SV = Savings, CH = Checking, CD = Cert of Deposit
    CURR_BAL DECIMAL(15,2) NOT NULL,
    CR_SCORE INT,
    STATUS_CD CHAR(1) NOT NULL -- A = Active, I = Inactive, D = Dormant
);""",
        "silver_transformations": """1. Standardize column names from UPPERCASE with abbreviations to clear snake_case (e.g. CUST_ID -> customer_id, OPEN_DT -> account_open_date, CURR_BAL -> current_balance, CR_SCORE -> credit_score, STATUS_CD -> account_status).
2. Cast OPEN_DT (string YYYYMMDD) to DateType.
3. Clean strings by removing leading/trailing spaces (mainframe CHAR fields pad with spaces).
4. Resolve ACCT_TYPE code names to clear descriptions (SV -> Savings, CH -> Checking, CD -> Certificate of Deposit).
5. Map STATUS_CD (A -> Active, I -> Inactive, D -> Dormant).
6. Create an audit column `_silver_processed_at` and compute surrogate key `sk_customer` using MD5 of customer_id.""",
        "business_requirements": "Join checking, savings, and deposit accounts per customer to compute Customer Lifetime Value (CLV), segment customer tier based on balance and credit scores, and flag dormant profiles with balances above $1,000.",
        "bronze_code": """# Bronze Ingestion Script - Mainframe DB2 DDL
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

spark = SparkSession.builder.appName("BronzeIngestion-Mainframe").getOrCreate()

source_path = "dbfs:/mnt/mainframe/db2/customer_master/"
target_table = "bronze.raw_cust_mast"

# Ingest COBOL/DB2 dump using Auto Loader with schema mapping
df_bronze = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "parquet")
  .option("cloudFiles.schemaLocation", "dbfs:/mnt/schemas/raw_cust_mast")
  .option("cloudFiles.schemaEvolutionMode", "rescue")
  .load(source_path)
)

# Enrich with metadata
df_bronze_enriched = (df_bronze
  .withColumn("_ingested_at", current_timestamp())
  .withColumn("_source_file", input_file_name())
)

# Save Stream to Delta Table
query = (df_bronze_enriched.writeStream
  .format("delta")
  .option("checkpointLocation", "dbfs:/mnt/checkpoints/raw_cust_mast")
  .outputMode("append")
  .toTable(target_table)
)
""",
        "silver_code": """# Silver Transformation Script - Mainframe Customer Profile
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, current_timestamp, md5, when

spark = SparkSession.builder.appName("SilverTransformation-Mainframe").getOrCreate()

# Load raw Bronze data
df_bronze = spark.read.table("bronze.raw_cust_mast")

# Perform cleaning, trimming, code mapping, and castings
df_silver = (df_bronze
  .select(
    # Surrogate key
    md5(trim(col("CUST_ID"))).alias("sk_customer"),
    trim(col("CUST_ID")).alias("customer_id"),
    trim(col("FIRST_NAME")).alias("first_name"),
    trim(col("MID_INIT")).alias("middle_initial"),
    trim(col("LAST_NAME")).alias("last_name"),
    
    # Cast YYYYMMDD string to date
    to_date(col("OPEN_DT"), "yyyyMMdd").alias("account_open_date"),
    
    # Map account types
    when(trim(col("ACCT_TYPE")) == "SV", "Savings")
    .when(trim(col("ACCT_TYPE")) == "CH", "Checking")
    .when(trim(col("ACCT_TYPE")) == "CD", "Certificate of Deposit")
    .otherwise("Unknown").alias("account_type"),
    
    col("CURR_BAL").cast("decimal(15,2)").alias("current_balance"),
    col("CR_SCORE").cast("integer").alias("credit_score"),
    
    # Map account statuses
    when(trim(col("STATUS_CD")) == "A", "Active")
    .when(trim(col("STATUS_CD")) == "I", "Inactive")
    .when(trim(col("STATUS_CD")) == "D", "Dormant")
    .otherwise("Other").alias("account_status"),
    
    # Audit timestamps
    current_timestamp().alias("_silver_processed_at")
  )
)

# Write to Silver Table
df_silver.write \\
  .format("delta") \\
  .mode("overwrite") \\
  .option("overwriteSchema", "true") \\
  .saveAsTable("silver.conformed_customer_profile")
""",
        "gold_sql": """-- models/gold/customer_lifetime_value.sql
{{ config(materialized='table') }}

WITH customer_base AS (
    SELECT 
        sk_customer,
        customer_id,
        first_name,
        last_name,
        account_open_date,
        account_type,
        current_balance,
        credit_score,
        account_status
    FROM {{ ref('conformed_customer_profile') }}
),

aggregates AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        credit_score,
        account_status,
        -- Aggregate balances across account types per customer
        SUM(CASE WHEN account_status = 'Active' THEN current_balance ELSE 0 END) AS active_balance,
        SUM(current_balance) AS customer_lifetime_value,
        COUNT(DISTINCT account_type) AS distinct_accounts
    FROM customer_base
    GROUP BY 1, 2, 3, 4, 5
),

segmented AS (
    SELECT
        *,
        -- Customer tiering segmentation logic
        CASE 
            WHEN customer_lifetime_value >= 100000 OR credit_score >= 750 THEN 'Tier 1 (Platinum)'
            WHEN customer_lifetime_value >= 25000 AND credit_score >= 650 THEN 'Tier 2 (Gold)'
            ELSE 'Tier 3 (Standard)'
        END AS customer_tier,
        
        -- Flag dormant balances above $1,000
        CASE 
            WHEN account_status = 'Dormant' AND customer_lifetime_value > 1000 THEN TRUE
            ELSE FALSE
        END AS is_dormant_risk
    FROM aggregates
)

SELECT * FROM segmented""",
        "gold_yml": """# models/gold/customer_lifetime_value.yml
version: 2

models:
  - name: customer_lifetime_value
    description: "Customer lifetime value aggregates, tiering classifications, and vulnerability risk analysis."
    columns:
      - name: customer_id
        description: "Primary key identifier from mainframe system."
        tests:
          - not_null
          - unique
      - name: customer_lifetime_value
        description: "Total balances across checking, savings, and CD accounts."
        tests:
          - not_null
      - name: customer_tier
        description: "Categorization of customer wealth and credit score tier."
        tests:
          - not_null
          - accepted_values:
              values: ['Tier 1 (Platinum)', 'Tier 2 (Gold)', 'Tier 3 (Standard)']
      - name: is_dormant_risk
        description: "Flag indicating whether a dormant account holds high value and requires remediation."
        tests:
          - not_null
""",
        "mermaid": """graph LR
    subgraph Legacy Layer
        db2_server["Mainframe DB2 DB (Fixed DDL)"]
    end

    subgraph Bronze Layer
        bronze_table["raw_cust_mast<br/>(Delta Table)"]
        auto_loader["Auto Loader Parquet Ingestion"]
    end

    subgraph Silver Layer
        silver_table["conformed_customer_profile<br/>(Delta Table)"]
        silver_ops["Trim CHAR padding<br/>Format Open Date<br/>Map Codes"]
    end

    subgraph Gold Layer
        gold_model["customer_lifetime_value<br/>(dbt Table Model)"]
    end

    db2_server -->|Export Dump| auto_loader
    auto_loader -->|Write Raw| bronze_table
    bronze_table -->|Read Stream| silver_ops
    silver_ops -->|Overwrite Clean| silver_table
    silver_table -->|dbt compile| gold_model

    classDef legacy fill:#F8D7DA,stroke:#F5C6CB,stroke-width:2px,color:#721C24;
    classDef bronze fill:#FFF3CD,stroke:#FFEEBA,stroke-width:2px,color:#856404;
    classDef silver fill:#D1ECF1,stroke:#BEE5EB,stroke-width:2px,color:#0C5460;
    classDef gold fill:#D4EDDA,stroke:#C3E6CB,stroke-width:2px,color:#155724;

    class db2_server legacy;
    class bronze_table,auto_loader bronze;
    class silver_table,silver_ops silver;
    class gold_model gold;"""
    }
}
