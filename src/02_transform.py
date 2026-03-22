"""Silver -> Gold transformation for Home Credit datasets using PySpark."""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Cấp 8GB RAM cho Spark để xử lý Join không bị OOM (Tràn bộ nhớ)
os.environ["PYSPARK_SUBMIT_ARGS"] = "--driver-memory 8g pyspark-shell"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def create_spark_session() -> SparkSession:
    """Khởi tạo Spark Session với cấu hình chuẩn đã test thành công ở luồng Ingestion."""
    hadoop_home = r"C:\hadoop"
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    
    # Đảm bảo Windows tìm thấy hadoop.dll xịn trong luồng chạy này
    if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
        os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ.get("PATH", "")

    spark = (
        SparkSession.builder.appName("HomeCredit_Silver_To_Gold")
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

def main():
    # 1. Định nghĩa đường dẫn
    project_root = Path(__file__).resolve().parent.parent
    silver_dir = project_root / "data" / "silver"
    gold_dir = project_root / "data" / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    spark = create_spark_session()

    print("="*80)
    print("[1/3] Đang đọc dữ liệu từ Silver Layer (Parquet)...")
    
    # Đọc các bảng Parquet
    app_train = spark.read.parquet(str(silver_dir / "application_train"))
    bureau = spark.read.parquet(str(silver_dir / "bureau"))
    prev_app = spark.read.parquet(str(silver_dir / "previous_application"))
    installments = spark.read.parquet(str(silver_dir / "installments_payments"))
    pos_cash = spark.read.parquet(str(silver_dir / "pos_cash_balance"))
    cc_balance = spark.read.parquet(str(silver_dir / "credit_card_balance"))

    print("[2/3] Đang tiến hành Aggregation (Gom cụm) và Join các bảng...")

    # --- AGGREGATION: Tính toán các chỉ số tóm tắt từ bảng phụ ---
    
    # Bảng Bureau: Đếm số khoản vay cũ và tổng nợ
    bureau_agg = bureau.groupBy("sk_id_curr").agg(
        F.count("sk_id_bureau").alias("bureau_loan_count"),
        F.sum("amt_credit_sum").alias("bureau_total_credit")
    )

    # Bảng Previous Application: Đếm số lần nộp đơn trước đó và trung bình số tiền
    prev_app_agg = prev_app.groupBy("sk_id_curr").agg(
        F.count("sk_id_prev").alias("prev_app_count"),
        F.avg("amt_application").alias("prev_app_avg_amt")
    )

    # Bảng Installments: Đếm số lần trả góp và tổng tiền đã trả
    installments_agg = installments.groupBy("sk_id_curr").agg(
        F.count("num_instalment_version").alias("installments_count"),
        F.sum("amt_payment").alias("installments_total_paid")
    )

    # Bảng POS CASH & Credit Card: Đếm số lượng
    pos_cash_agg = pos_cash.groupBy("sk_id_curr").agg(F.count("sk_id_prev").alias("pos_cash_count"))
    cc_agg = cc_balance.groupBy("sk_id_curr").agg(F.sum("amt_balance").alias("cc_total_balance"))

    # --- JOIN: Gắn tất cả vào bảng chính ---
    master_df = app_train \
        .join(bureau_agg, "sk_id_curr", "left") \
        .join(prev_app_agg, "sk_id_curr", "left") \
        .join(installments_agg, "sk_id_curr", "left") \
        .join(pos_cash_agg, "sk_id_curr", "left") \
        .join(cc_agg, "sk_id_curr", "left")

    # Điền giá trị 0 cho những khách hàng không có lịch sử ở bảng phụ
    fill_cols = [
        "bureau_loan_count", "bureau_total_credit", 
        "prev_app_count", "prev_app_avg_amt", 
        "installments_count", "installments_total_paid", 
        "pos_cash_count", "cc_total_balance"
    ]
    master_df = master_df.fillna(0, subset=fill_cols)

    row_count = master_df.count()
    col_count = len(master_df.columns)
    
    print(f"Đã Join xong! Bảng Master có: {row_count} dòng và {col_count} cột.")
    print("[3/3] Đang ghi dữ liệu ra Gold Layer (Parquet)...")

    # 4. Ghi ra thư mục Gold (coalesce để tránh tạo ra quá nhiều file nhỏ)
    target_path = gold_dir / "master_table"
    master_df.coalesce(4).write.mode("overwrite").parquet(str(target_path))

    print(f"[SUCCESS] Hoàn tất Data Pipeline! Dữ liệu tinh lọc (Gold) đã nằm tại: {target_path}")
    
    spark.stop()

if __name__ == "__main__":
    main()