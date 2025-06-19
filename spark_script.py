# spark_script.py - FINAL with Clustering and Regression

import os
import traceback
from pyspark.sql import SparkSession
# [MODIFIKASI] Menambahkan impor untuk Regresi
from pyspark.ml.feature import StringIndexer, VectorAssembler, Imputer, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, FloatType

# Definisi nama kolom (tetap sama)
NEW_APP_NAME_COL, NEW_APP_ID_COL, NEW_CATEGORY_COL, NEW_RATING_COL, NEW_INSTALLS_COL, NEW_PRICE_COL = "title", "appId", "genre", "score", "minInstalls", "price"

def train_and_save_models(df_apps, df_img_features, stage_name_suffix, spark):
    print(f"\n=== PROCESSING STAGE: {stage_name_suffix} ===")
    
    # Langkah 1: Gabungkan data aplikasi utama dengan fitur gambar
    print("Joining main data with pre-processed image features...")
    if df_img_features:
        df_with_all_features = df_apps.join(df_img_features, "appId", "left_outer")
        print("✓ Join complete.")
    else:
        df_with_all_features = df_apps
        print("INFO: No image features to join.")

    # Langkah 2: Cleaning & Casting tipe data
    print("\n=== DATA CLEANING & TYPE CASTING ===")
    cols_to_cast = {
        NEW_RATING_COL: DoubleType(), NEW_INSTALLS_COL: LongType(), NEW_PRICE_COL: DoubleType(),
        "img_avg_red": FloatType(), "img_avg_green": FloatType(), "img_avg_blue": FloatType(),
        "img_width": IntegerType(), "img_height": IntegerType()
    }
    df_cleaned = df_with_all_features
    for col_name, target_type in cols_to_cast.items():
        if col_name in df_cleaned.columns:
            df_cleaned = df_cleaned.withColumn(col_name, col(col_name).cast(target_type))

    # Langkah 3: Filter data yang valid untuk training (non-null)
    df_valid_for_training = df_cleaned.filter(
        col(NEW_RATING_COL).isNotNull() & col(NEW_INSTALLS_COL).isNotNull() & col(NEW_PRICE_COL).isNotNull()
    ).cache()
    
    training_count = df_valid_for_training.count()
    print(f"Total rows valid for training: {training_count}")
    if training_count < 10: 
        print("ERROR: Not enough data for training after filtering.")
        df_valid_for_training.unpersist()
        return False

    # =========================================================================
    # BAGIAN 1: K-MEANS CLUSTERING (Logika yang sudah ada)
    # =========================================================================
    print(f"\n--- FITTING K-MEANS CLUSTERING MODEL ---")
    category_index_col_kmeans = NEW_CATEGORY_COL + "IndexKMeans"
    
    # Definisikan fitur-fitur untuk clustering
    numeric_features_cluster = [NEW_RATING_COL, NEW_INSTALLS_COL, NEW_PRICE_COL]
    image_features_cluster = ["img_avg_red", "img_avg_green", "img_avg_blue", "img_width", "img_height"]
    imputer_inputs_cluster = [c for c in numeric_features_cluster + image_features_cluster if c in df_valid_for_training.columns]
    imputer_outputs_cluster = [f + "_imputed" for f in imputer_inputs_cluster]
    
    imputer_stage_cluster = Imputer(inputCols=imputer_inputs_cluster, outputCols=imputer_outputs_cluster, strategy="mean")
    indexer_stage_cluster = StringIndexer(inputCol=NEW_CATEGORY_COL, outputCol=category_index_col_kmeans, handleInvalid="skip")
    assembler_inputs_cluster = imputer_outputs_cluster + [category_index_col_kmeans]
    assembler_stage_cluster = VectorAssembler(inputCols=assembler_inputs_cluster, outputCol="unscaled_features")
    scaler_stage_cluster = StandardScaler(inputCol="unscaled_features", outputCol="scaled_features", withStd=True, withMean=False)
    kmeans_stage = KMeans(k=5, seed=1, featuresCol="scaled_features", predictionCol="cluster")
    
    pipeline_kmeans = Pipeline(stages=[imputer_stage_cluster, indexer_stage_cluster, assembler_stage_cluster, scaler_stage_cluster, kmeans_stage])
    
    print("Fitting K-Means pipeline...")
    kmeans_model = pipeline_kmeans.fit(df_valid_for_training)
    df_with_cluster = kmeans_model.transform(df_cleaned)
    
    print("✓ K-Means model fitted. Cluster counts:")
    df_with_cluster.groupBy("cluster").count().orderBy("cluster").show()

    # =========================================================================
    # BAGIAN 2: RANDOM FOREST REGRESSION (Logika Baru)
    # =========================================================================
    print(f"\n--- FITTING RANDOM FOREST REGRESSION MODEL ---")
    category_index_col_rf = NEW_CATEGORY_COL + "IndexRF"
    features_rf_col = "features_rf"

    # Definisikan fitur untuk regresi. PENTING: JANGAN masukkan 'score' sebagai fitur.
    # Kita akan memprediksi 'score', jadi 'score' adalah label, bukan fitur.
    rf_features = [NEW_INSTALLS_COL, NEW_PRICE_COL] # Fitur dasar
    # Menambahkan fitur gambar jika ada
    image_features_rf = ["img_avg_red", "img_avg_green", "img_avg_blue", "img_width", "img_height"]
    for img_feat in image_features_rf:
        if img_feat in df_valid_for_training.columns:
            rf_features.append(img_feat)

    imputer_outputs_rf = [f + "_imputedRF" for f in rf_features]
    
    imputer_stage_rf = Imputer(inputCols=rf_features, outputCols=imputer_outputs_rf, strategy="mean")
    indexer_stage_rf = StringIndexer(inputCol=NEW_CATEGORY_COL, outputCol=category_index_col_rf, handleInvalid="skip")
    assembler_inputs_rf = imputer_outputs_rf + [category_index_col_rf]
    assembler_stage_rf = VectorAssembler(inputCols=assembler_inputs_rf, outputCol=features_rf_col)
    
    # [PERBAIKAN] Definisikan model regressornya dengan maxBins yang lebih besar
    rf_regressor = RandomForestRegressor(featuresCol=features_rf_col, labelCol=NEW_RATING_COL, predictionCol="PredictedRating", seed=42, maxBins=64)
    
    pipeline_rf = Pipeline(stages=[imputer_stage_rf, indexer_stage_rf, assembler_stage_rf, rf_regressor])
    
    print("Fitting Random Forest pipeline...")
    rf_model = pipeline_rf.fit(df_valid_for_training)
    # Transformasi data yang sudah memiliki kolom 'cluster'
    df_final = rf_model.transform(df_with_cluster)

    # Evaluasi model regresi
    print("Evaluating Random Forest model performance...")
    evaluator = RegressionEvaluator(labelCol=NEW_RATING_COL, predictionCol="PredictedRating", metricName="rmse")
    # Evaluasi hanya pada data yang memang punya rating (data training)
    predictions_for_eval = rf_model.transform(df_valid_for_training)
    rmse = evaluator.evaluate(predictions_for_eval)
    print(f"✓ Random Forest - Root Mean Squared Error (RMSE) on training data: {rmse}")

    df_valid_for_training.unpersist()

    # =========================================================================
    # LANGKAH TERAKHIR: SIMPAN SEMUA HASIL
    # =========================================================================
    print(f"\n=== SAVING FINAL DATA AND MODELS (Stage: {stage_name_suffix}) ===")
    
    # Tentukan path penyimpanan
    kmeans_model_save_path = f"app_model_kmeans_{stage_name_suffix}"
    rf_model_save_path = f"app_model_rf_{stage_name_suffix}" # Path untuk model RF
    api_data_save_path = f"api_app_info_{stage_name_suffix}"
    
    # Tentukan kolom-kolom final untuk API, termasuk 'PredictedRating'
    final_cols = [
        NEW_APP_ID_COL, NEW_APP_NAME_COL, NEW_CATEGORY_COL, NEW_RATING_COL, 
        NEW_INSTALLS_COL, NEW_PRICE_COL, "icon_path", "cluster", "PredictedRating",
        "img_avg_red", "img_avg_green", "img_avg_blue"
    ]
    valid_final_cols = [c for c in final_cols if c in df_final.columns]
    api_data = df_final.select(*valid_final_cols)

    # Simpan model K-Means
    kmeans_model.write().overwrite().save(kmeans_model_save_path)
    print(f"✓ K-Means Model saved to '{kmeans_model_save_path}'")
    
    # Simpan model Random Forest
    rf_model.write().overwrite().save(rf_model_save_path)
    print(f"✓ Random Forest Model saved to '{rf_model_save_path}'")
    
    # Simpan data Parquet untuk API
    api_data.write.mode("overwrite").parquet(api_data_save_path)
    print(f"✓ Final API data saved to '{api_data_save_path}'")
    
    return True

# ===================== SCRIPT UTAMA (Tidak ada perubahan) =====================
if __name__ == "__main__":
    spark = SparkSession.builder.appName("ML_Pipeline_with_Preprocessed_Features").config("spark.driver.memory", "3g").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=== SPARK ML TRAINING (WITH PRE-PROCESSED FEATURES) STARTED ===")
    BATCH_DATA_FOLDER = "batch_data"
    IMAGE_FEATURES_FILE = "image_features.parquet"

    try:
        df_full = spark.read.csv(BATCH_DATA_FOLDER, header=True, inferSchema=False, escape="\"")
        print(f"✓ Berhasil memuat {df_full.count()} baris data aplikasi.")
    except Exception as e:
        print(f"FATAL: Gagal memuat data utama dari '{BATCH_DATA_FOLDER}': {e}"); spark.stop(); exit(1)

    df_image_features = None
    if os.path.exists(IMAGE_FEATURES_FILE):
        try:
            df_image_features = spark.read.parquet(IMAGE_FEATURES_FILE)
            print(f"✓ Berhasil memuat {df_image_features.count()} fitur gambar dari '{IMAGE_FEATURES_FILE}'")
        except Exception as e:
            print(f"WARNING: Gagal memuat file fitur gambar '{IMAGE_FEATURES_FILE}': {e}. Melanjutkan tanpa fitur gambar.")
    else:
        print(f"INFO: File '{IMAGE_FEATURES_FILE}' tidak ditemukan. Melanjutkan tanpa fitur gambar.")

    if train_and_save_models(df_full, df_image_features, "final_with_features", spark):
        print(f"\n=== SPARK ML PIPELINE COMPLETED SUCCESSFULLY. ===")
    else:
        print(f"\n=== SPARK ML PIPELINE FAILED. ===")
        
    spark.stop()
