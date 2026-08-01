import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lower, count, avg, round as spark_round

def run_lakehouse_pipeline():
    print("Initializing Distributed Databricks Spark Engine Session...")
    
    spark = SparkSession.builder \
        .appName("MultiFacilityHealthcareLakehouse") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .master("local[*]") \
        .getOrCreate()
        
    # 1. BRONZE LAYER
    print("\n[BRONZE] Ingesting landing raw EHR bulk tables into data lake...")
    raw_path = os.path.abspath("data/raw_bulk_ehr_landing.csv")
    bronze_df = spark.read.csv(f"file:///{raw_path}".replace("\\", "/"), header=True, inferSchema=True)
    
    os.makedirs("data/lakehouse/bronze_ehr_table", exist_ok=True)
    bronze_df.write.format("delta").mode("overwrite").save("data/lakehouse/bronze_ehr_table")
    
    # 2. SILVER LAYER
    print("[SILVER] Processing data perimeters and scaling type-safety filters...")
    loaded_bronze = spark.read.format("delta").load("data/lakehouse/bronze_ehr_table")
    
    silver_df = loaded_bronze.withColumn(
        "clean_gender",
        when(lower(col("administrative_gender")).isin("m", "male"), "male")
        .when(lower(col("administrative_gender")).isin("f", "female"), "female")
        .otherwise("unknown")
    ).withColumn(
        "is_compliant",
        when(
            (col("patient_age") >= 0) & (col("patient_age") <= 125) &
            (col("systolic_blood_pressure") >= 30) & (col("systolic_blood_pressure") <= 300) &
            (col("icd10_diagnostic_code").isNotNull()) & (col("icd10_diagnostic_code") != "MALFORMED"),
            1
        ).otherwise(0)
    )
    
    os.makedirs("data/lakehouse/silver_ehr_table", exist_ok=True)
    silver_df.write.format("delta").mode("overwrite").save("data/lakehouse/silver_ehr_table")
    
    # 3. GOLD LAYER
    print("[GOLD] Compiling multi-facility business intelligence aggregates...")
    loaded_silver = spark.read.format("delta").load("data/lakehouse/silver_ehr_table")
    
    gold_facility_metrics = loaded_silver.groupBy("facility_name").agg(
        count("record_id").alias("total_records_processed"),
        spark_round(avg("is_compliant") * 100, 2).alias("facility_data_quality_health_index"),
        spark_round(avg("systolic_blood_pressure"), 2).alias("avg_recorded_systolic_bp")
    )
    
    os.makedirs("data/lakehouse/gold_facility_metrics_table", exist_ok=True)
    gold_facility_metrics.write.format("delta").mode("overwrite").save("data/lakehouse/gold_facility_metrics_table")
    
    # FIXED: Convert to standard Pandas dataframe text formatting to prevent Java crashes
    pandas_report_string = gold_facility_metrics.toPandas().to_string(index=False)
    
    with open("dashboard.txt", "w") as f:
        f.write("=======================================================================\n")
        f.write("      GOLD LAYER EXECUTIVE INTER-FACILITY HEALTH PERFORMANCE REPORT     \n")
        f.write("=======================================================================\n")
        f.write(pandas_report_string + "\n")
        f.write("=======================================================================\n")
    
    print("\nSUCCESS! The final Gold performance report has been compiled and saved to dashboard.txt.")
    spark.stop()

if __name__ == "__main__":
    run_lakehouse_pipeline()

