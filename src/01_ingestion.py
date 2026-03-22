"""Bronze -> Silver ingestion for Home Credit datasets using PySpark."""

from __future__ import annotations

import os
import re
import sys
from pathlib import Path

# Cấp 8GB RAM cho Spark
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 8g pyspark-shell"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession # type: ignore

def create_spark_session() -> SparkSession:
    """Create a Spark session tuned for local Windows execution."""
    hadoop_home = r"C:\hadoop"
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    
    # Đảm bảo Windows tìm thấy hadoop.dll trong luồng chạy này
    if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    spark = (
        SparkSession.builder.appName("HomeCredit_Bronze_To_Silver_Ingestion")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark

def to_snake_case(column_name: str) -> str:
    cleaned = column_name.strip().lower()
    cleaned = re.sub(r"[^a-z0-9]+", "_", cleaned)
    cleaned = re.sub(r"_+", "_", cleaned)
    cleaned = cleaned.strip("_")
    return cleaned if cleaned else "col"

def make_unique_column_names(column_names: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    unique_names: list[str] = []
    for name in column_names:
        if name not in seen:
            seen[name] = 1
            unique_names.append(name)
        else:
            seen[name] += 1
            unique_names.append(f"{name}_{seen[name]}")
    return unique_names

def ingest_and_clean_csv(file_name: str, spark: SparkSession, bronze_dir: Path, silver_dir: Path) -> None:
    source_path = bronze_dir / file_name
    target_name = Path(file_name).stem
    target_path = silver_dir / target_name

    print("=" * 90)
    print(f"[START] Ingest file: {file_name}")

    try:
        if not source_path.exists():
            raise FileNotFoundError(f"Input file not found: {source_path}")

        df = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(source_path))
        )

        normalized_cols = [to_snake_case(col) for col in df.columns]
        normalized_cols = make_unique_column_names(normalized_cols)
        df_cleaned = df.toDF(*normalized_cols)

        row_count = df_cleaned.count()

        # Dùng 2 luồng ghi thay vì 1 để quá trình commit file nhanh hơn
        df_to_write = df_cleaned.coalesce(2)

        (
            df_to_write.write.mode("overwrite")
            .format("parquet")
            .save(str(target_path))
        )

        print(f"[SUCCESS] File       : {file_name}")
        print(f"[SUCCESS] Output path: {target_path}")
        print(f"[SUCCESS] Row count  : {row_count}")

    except Exception as err:
        print(f"[ERROR] Unexpected error while processing {file_name}: {err}")
    finally:
        print(f"[END] Ingest file: {file_name}")

def main() -> None:
    project_root = Path(__file__).resolve().parent.parent
    bronze_dir = project_root / "data" / "bronze"
    silver_dir = project_root / "data" / "silver"
    silver_dir.mkdir(parents=True, exist_ok=True)

    csv_files = [
        "application_train.csv",
        "bureau.csv",
        "bureau_balance.csv",
        "credit_card_balance.csv",
        "installments_payments.csv",
        "POS_CASH_balance.csv",
        "previous_application.csv",
    ]

    spark = create_spark_session()

    try:
        for file_name in csv_files:
            ingest_and_clean_csv(file_name=file_name, spark=spark, bronze_dir=bronze_dir, silver_dir=silver_dir)
    finally:
        spark.stop()
        print("\nPipeline completed. Spark session stopped.")

if __name__ == "__main__":
    main()