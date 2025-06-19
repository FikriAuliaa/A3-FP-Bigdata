# FP-Bigdata-A3

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

