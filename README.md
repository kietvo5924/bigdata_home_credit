#  Home Credit Default Risk - Data Engineering Pipeline

##  Giới thiệu đồ án
Dự án này xây dựng một luồng xử lý dữ liệu lớn (Data Pipeline) tự động bằng **Apache Spark (PySpark)** cho bộ dữ liệu [Home Credit Default Risk từ Kaggle](https://www.kaggle.com/c/home-credit-default-risk).

Mục tiêu của hệ thống là tự động hóa quá trình làm sạch, chuẩn hóa tên cột, chuyển đổi định dạng và gom cụm (Aggregation) hàng chục triệu dòng dữ liệu lịch sử tín dụng để tạo ra một bảng Master duy nhất. Bảng dữ liệu này đã sẵn sàng để phục vụ cho Data Visualization (BI) hoặc huấn luyện mô hình Machine Learning dự đoán rủi ro tín dụng.

---

##  Kiến trúc hệ thống (Medallion Architecture)
Hệ thống được thiết kế theo kiến trúc **Medallion** chuẩn trong Data Engineering:

* ** Bronze Layer:** Dữ liệu thô (Raw Data) định dạng `.csv` giữ nguyên nguyên trạng từ Kaggle.
* ** Silver Layer:** Dữ liệu được chuẩn hóa tên cột sang định dạng `snake_case`, làm sạch và lưu trữ dưới dạng định dạng nén `.parquet` siêu tốc (Script `01_ingestion.py`).
* ** Gold Layer:** Dữ liệu từ 6 bảng lịch sử phụ được Gom cụm (Aggregation) và Left Join vào bảng khách hàng chính (`application_train`), tạo thành 1 bảng Master duy nhất với 130 features (Script `02_transform.py`).

---

##  Yêu cầu hệ thống (Prerequisites)
Dự án này được tối ưu hóa để chạy trên môi trường **Windows**. Để code chạy thành công mà không gặp lỗi NativeIO của Spark, vui lòng cài đặt đúng các phần mềm sau:

**1. Microsoft Visual C++ Redistributable (Bắt buộc trên Windows)**
* Tải và cài đặt bản x64 mới nhất: [VC_redist.x64.exe](https://aka.ms/vs/17/release/vc_redist.x64.exe)

**2. Python 3.10.x**
* PySpark 3.5.0 hoạt động ổn định nhất với Python 3.10 (Tránh dùng Python 3.12+).
* Tải tại: [Python 3.10.11](https://www.python.org/ftp/python/3.10.11/python-3.10.11-amd64.exe) (Nhớ tích chọn `Add Python to PATH` khi cài).

**3. Java Development Kit (JDK 11)**
* Apache Spark yêu cầu Java để chạy engine lõi.
* Tải và cài đặt: [Microsoft OpenJDK 11 (Windows x64 .msi)](https://aka.ms/download-jdk/microsoft-jdk-11.0.22-windows-x64.msi).
* Trong lúc cài, nhớ chọn tính năng `Set JAVA_HOME variable` (Will be installed on local hard drive).

**4. Cấu hình Hadoop Native Binaries cho Windows**
Spark trên Windows cần thư viện C++ của Hadoop để ghi file. Hãy làm đúng 3 bước sau:
1. Tạo một thư mục tại ổ C: `C:\hadoop\bin`
2. Tải file [winutils.exe](https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/winutils.exe) và lưu vào `C:\hadoop\bin`.
3. Tải file [hadoop.dll](https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.6/bin/hadoop.dll) và lưu vào `C:\hadoop\bin`.

---

##  Hướng dẫn cài đặt và chạy dự án

### Bước 1: Khởi tạo môi trường
Mở Terminal tại thư mục gốc của dự án và chạy các lệnh sau:
```bash
# 1. Tạo môi trường ảo
python -m venv venv

# 2. Kích hoạt môi trường ảo (Trên Windows)
.\venv\Scripts\activate

# 3. Cài đặt các thư viện cần thiết
pip install pyspark==3.5.0 pyarrow pandas

## Cấu trúc thư mục dự án
bigdata_home_credit/
│
├── data/
│   ├── bronze/          <- Chứa 7 file CSV thô (Bạn tự thêm vào)
│   ├── silver/          <- Dữ liệu đã làm sạch dạng Parquet
│   └── gold/            <- Bảng Master Parquet hoàn chỉnh
│
├── src/
│   ├── 01_ingestion.py  <- Luồng xử lý thô sang chuẩn
│   └── 02_transform.py  <- Luồng tổng hợp và Join dữ liệu
│
├── .gitignore           <- Loại trừ file rác, venv và thư mục data/ khỏi Git
└── README.md            <- Tài liệu hướng dẫn này
