# FP-Bigdata-A3

## Anggota Kelompok
| No             | Nama | NRP |
| :---------------- | :------ | :---- |
| 1        |   Nafi Firdaus    | 5027231045 |
| 2           |   Tio Axelino   | 5027231065 |
| 3    |  Dionisius Marcel   | 5027231044 |
| 4 |  Fikri Aulia As Sa'adi   | 5027231026 |
| 5 |  Muhammad Andrean Rizq Prasetio    | 5027231052 |

## Latar Belakang Masalah
Google Play Store memiliki lebih dari 3 juta aplikasi, namun hanya sebagian kecil yang mendapatkan visibilitas tinggi. Sebagian besar pengguna:
- Hanya melihat aplikasi populer atau yang ditampilkan di halaman utama
- Tidak menemukan aplikasi yang relevan, unik, atau sesuai kebutuhan pribadi
- Melewatkan aplikasi niche berkualitas tinggi karena kurangnya sistem rekomendasi yang menjangkau long-tail apps

#### Permasalahan utama:
Banyak aplikasi bagus tidak terekspos ke pengguna yang tepat, menyebabkan rendahnya jumlah unduhan dan potensi ekonomi yang tidak termanfaatkan.

## Tujuan Proyek
- Membangun Sistem Rekomendasi Aplikasi Otomatis Menggunakan machine learning untuk mencari aplikasi serupa berdasarkan fitur, nama, dan rating.

- Menerapkan Arsitektur Data Lakehouse Menggunakan Kafka (streaming), MinIO (storage), Hive (metastore), dan Spark (analitik).

- Mengintegrasikan Antarmuka Web Interaktif Website berbasis Streamlit yang memungkinkan pengguna

- Mengatasi Ketimpangan Eksposur Aplikasi Long-Tail Dengan rekomendasi otomatis berdasarkan fitur konten, klasifikasi kategori, dan analisis deskriptif.

### Fitur utama

- Mencari aplikasi

- Melihat aplikasi serupa

- Melihat aplikasi teratas

- Menjelajahi aplikasi per kategori

## Dataset

| Dataset             | Jenis | Link |
| :---------------- | :------ | :---- |
| Google Play Apps and Games (3.4M)        |   Structured    | https://www.kaggle.com/datasets/tapive/google-play-apps-and-games |
| cons-50           |   Unstructured   | https://www.kaggle.com/datasets/danhendrycks/icons50?utm_source=chatgpt.com |

## Teknologi yang Digunakan

| Komponen         | Teknologi              | Deskripsi Singkat                                                                                        |
| :---------------- | :---------------------- | :-------------------------------------------------------------------------------------------------------- |
| Penyimpanan Data | **MinIO**              | Object storage (S3-compatible) untuk menyimpan data mentah, hasil ekstraksi fitur, dan hasil klasifikasi |
| Metadata         | **Apache Hive**        | Menyediakan katalog metadata untuk file `.parquet` / `.csv` di MinIO                                     |
| Streaming Data   | **Apache Kafka**       | Simulasi aliran data aplikasi baru yang masuk ke sistem                                                  |
| Pemrosesan Data  | **Apache Spark**       | Preprocessing, ekstraksi fitur visual (dari Icons-50), dan clustering aplikasi                           |
| Machine Learning | **Scikit-learn**       | Algoritma klasifikasi, clustering, rekomendasi                                                           |
| Frontend Web     |  **HTML**   | Dashboard interaktif dan API untuk mencari dan menampilkan aplikasi                                      |
| Orkestrasi       | **Docker Compose**     | Menjalankan seluruh layanan secara terkoordinasi dalam container                                         |
| Pengujian Upload | **Python + MinIO SDK** | Mengunggah data `.parquet` hasil Spark ke bucket MinIO                                                   |

------------------------------------

## Setup Environment
1. Install required packages:
```bash
# Create a virtual environment (recommended)
python -m venv venv

# Activate virtual environment
# For Windows:
.\venv\Scripts\activate
# For Linux/Mac:
source venv/bin/activate

# Install requirements
pip install -r requirements.txt
```

2. Setup Java and Environment Variables:
   - Install OpenJDK 8 (Temurin) from: https://adoptium.net/
   - Create a `.env` file in the project root directory
   - Copy the contents from `.env.example` to `.env`
   - Update the paths in `.env` with your local paths:
     ```env
     JAVA_HOME=C:\Path\To\Your\Java\JDK
     SPARK_HOME=C:\Path\To\Your\PySpark
     PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip
     PATH=${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}
     ```

3. Setup Apache Kafka:
   - Download Apache Kafka from: https://kafka.apache.org/downloads
   - Extract the downloaded file
   - Start Zookeeper:
     ```bash
     # Windows
     .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
     
     # Linux/Mac
     ./bin/zookeeper-server-start.sh ./config/zookeeper.properties
     ```
   - Start Kafka Server:
     ```bash
     # Windows
     .\bin\windows\kafka-server-start.bat .\config\server.properties
     
     # Linux/Mac
     ./bin/kafka-server-start.sh ./config/server.properties
     ```

3. Verify Installation:
   ```bash
   # Check Java version (should be OpenJDK 8)
   java -version
   
   # Check PySpark installation
   pip show pyspark
   ```

## Cara start
### 1. Download dataset
https://www.kaggle.com/datasets/tapive/google-play-apps-and-games
### 2. Start producer
```py
python producer.py
```
### 3. Start consumer
```py
python consumer.py
```
### 4. Start spark
```py
feature_extraxtor.py
```
```py
spark_script.py
```
### 5. Jalankan App.py
```py
python app.py
```

## Cara jalanin langsung (sudah ada batch data)
### 1. Download dataset
https://www.kaggle.com/datasets/tapive/google-play-apps-and-games
### 2. Taruh di folder yang sama dengan script python
### 3. Jalanin app.py
### 4. Buka index.html
![image](https://github.com/user-attachments/assets/84e30298-b934-45fe-833b-ec5b38d698c5)

