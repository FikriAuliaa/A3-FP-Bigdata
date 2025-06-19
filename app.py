import os
import re
import traceback
from flask import Flask, request, jsonify, send_from_directory # [MODIFIKASI]
from flask_cors import CORS
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vector
from pyspark.ml import PipelineModel
from pyspark.sql.functions import col, desc, lower, abs as spark_abs, udf, lit
from pyspark.sql.types import IntegerType, FloatType, LongType, DoubleType, StringType

app = Flask(__name__)
CORS(app) # Mengizinkan akses dari semua origin

# --- Inisialisasi Spark Session (Global) ---
spark = SparkSession.builder.appName("AppRecommendationAPI_Final_Complete").config("spark.driver.memory", "3g").getOrCreate()
spark.sparkContext.setLogLevel("ERROR") # Mengurangi log agar terminal lebih bersih

# --- [MODIFIKASI] Konfigurasi Path untuk Gambar ---
# Path absolut ke folder yang berisi semua kategori ikon (misal: .../dataset-gambar/Icons-50)
# PENTING: Sesuaikan path ini jika direktori Anda berbeda.
BASE_DATASET_DIRECTORY = os.path.abspath("dataset-gambar")
print(f"INFO: Direktori dasar untuk ikon diatur ke: {BASE_DATASET_DIRECTORY}")

# Daftar stop words (tetap sama)
STOP_WORDS = set([
    "a", "an", "the", "is", "are", "was", "were", "of", "and", "or", "in", "on", "at",
    "to", "for", "with", "by", "from", "as", "it", "this", "that", "app", "apps", "hd",
    "pro", "free", "new", "android", "mobile", "online", "offline", "edition", "version",
    "official", "ultimate", "lite", "plus", "super", "real", "my", "best", "top", "big",
    "small", "all", "very", "just", "full", "no", "not", "yes", "fps", "rpg", "p", "o",
    "s", "vs", "vs.", "x", "2d", "3d", "update", "jan", "feb", "mar", "apr", "may",
    "jun", "jul", "aug", "sep", "oct", "nov", "dec", ":", "-", "&", "+", ",", ".",
    "!", "?", "'", "\"", "(", ")", "–", "™", "®"
])

# --- Helper Functions ---
def extract_keywords(text):
    if not text or not isinstance(text, str): return set()
    text = re.sub(r'[^\w\s]', '', text.lower()); words = text.split()
    return set(word for word in words if word not in STOP_WORDS and len(word) > 2)

def find_latest_data_path(base_name="api_app_info_"):
    # [MODIFIKASI] Logika yang lebih sederhana dan andal untuk menemukan folder data terbaru
    try:
        all_dirs = [d for d in os.listdir('.') if os.path.isdir(d) and d.startswith(base_name)]
        if not all_dirs: return None
        latest_dir = max(all_dirs, key=lambda d: os.path.getmtime(d))
        print(f"✓ Menggunakan data dari direktori terbaru: '{latest_dir}'")
        return latest_dir
    except Exception as e:
        print(f"Error saat mencari path data: {e}")
        return None

def load_api_data():
    DATA_PATH = find_latest_data_path()
    if DATA_PATH and os.path.exists(DATA_PATH):
        try:
            df_loaded = spark.read.parquet(DATA_PATH).cache()
            print(f"✓ Berhasil memuat {df_loaded.count()} baris data dari '{DATA_PATH}'")
            
            # [MODIFIKASI] Menambahkan kolom fitur gambar ke dalam map tipe data
            essential_cols_type_map = {
                "title": StringType(), "appId": StringType(), "genre": StringType(),
                "score": FloatType(), "minInstalls": LongType(), "price": DoubleType(),
                "PredictedRating": FloatType(), "cluster": IntegerType(),
                "img_avg_red": FloatType(), "img_avg_green": FloatType(), "img_avg_blue": FloatType(),
                "img_width": IntegerType(), "img_height": IntegerType()
            }
            for col_name, target_type_obj in essential_cols_type_map.items():
                if col_name in df_loaded.columns:
                    df_loaded = df_loaded.withColumn(col_name, col(col_name).cast(target_type_obj))

            return df_loaded
        except Exception as e:
            print(f"FATAL: Gagal memuat data Parquet. Error: {e}")
            return None
    else:
        print("FATAL: Tidak ada data Parquet yang ditemukan. Jalankan spark_script.py terlebih dahulu.")
        return None

# --- Memuat Data Global ---
df = load_api_data()
if df: df.printSchema()

# ==============================================================================
# [BAGIAN BARU] ENDPOINT UNTUK MENYAJIKAN GAMBAR
# ==============================================================================
@app.route('/icon/<path:relative_path>')
def serve_icon(relative_path):
    """Menyajikan file gambar ikon dari direktori dataset."""
    try:
        return send_from_directory(BASE_DATASET_DIRECTORY, relative_path)
    except FileNotFoundError:
        return jsonify({"error": "Icon not found"}), 404

# --- API Endpoints ---
# Semua endpoint lama Anda dipertahankan dan disesuaikan untuk menyertakan icon_path.

@app.route('/check_data', methods=['GET'])
def check_data_endpoint():
    # Endpoint ini tidak perlu diubah
    if df is None: return jsonify({"status": "error", "message": "Data tidak dimuat."}), 500
    return jsonify({
        "status": "success",
        "message": "Data aplikasi berhasil dimuat.",
        "rows_count": df.count(),
        "columns": df.columns,
        "sample_data": [row.asDict() for row in df.limit(3).collect()]
    })

@app.route('/categories', methods=['GET'])
def get_categories():
    # Endpoint ini tidak perlu diubah
    if df is None: return jsonify({"error": "Data tidak tersedia."}), 500
    categories_list = [row['genre'] for row in df.select('genre').distinct().orderBy('genre').collect() if row['genre']]
    return jsonify({"categories": categories_list})

@app.route('/search_app_suggestions', methods=['GET'])
def search_app_suggestions():
    query = request.args.get('q', '').strip().lower()
    if df is None: return jsonify({"error": "Data aplikasi tidak tersedia."}), 500
    if not query or len(query) < 2: return jsonify({"suggestions": [], "message": "Min 2 karakter."})

    # [MODIFIKASI] Menambahkan icon_path ke suggestions
    suggestions = df.filter(lower(col('title')).contains(query)) \
        .orderBy(desc("minInstalls")) \
        .select("title", "appId", "icon_path", "score") \
        .distinct().limit(15).collect()
        
    formatted = [{"name": row["title"], "app_id": row["appId"], "icon_path": row["icon_path"], "score": row["score"]} for row in suggestions]
    if not formatted: return jsonify({"suggestions": [], "message": f"Tidak ada aplikasi cocok dengan '{query}'."})
    return jsonify({"suggestions": formatted})

@app.route('/app_details_by_id/<app_id_input>', methods=['GET']) 
def get_app_details_by_id(app_id_input):
    if df is None: return jsonify({"error": "Data aplikasi tidak tersedia."}), 500
    app_details_row = df.filter(col('appId') == app_id_input).first()
    if not app_details_row: return jsonify({"error": f"Aplikasi dengan App Id '{app_id_input}' tidak ditemukan."}), 404
    
    app_details = app_details_row.asDict()
    return jsonify({"app_details": app_details})

@app.route('/recommend_apps_by_category/<category_name>', methods=['GET'])
def recommend_apps_by_category(category_name):
    if df is None: return jsonify({"error": "Data tidak tersedia."}), 500
    recommended_df = df.filter(lower(col('genre')) == category_name.lower()) \
                       .orderBy(desc("score"), desc("minInstalls"))
    
    select_cols = ['title', 'appId', 'genre', 'score', 'minInstalls', 'price', 'icon_path']
    valid_cols = [c for c in select_cols if c in recommended_df.columns]
    
    result = [row.asDict() for row in recommended_df.select(*valid_cols).limit(20).collect()]
    return jsonify({"recommendations": result})

@app.route('/top_apps', methods=['GET'])
def get_top_apps():
    if df is None: return jsonify({"error": "Data tidak tersedia."}), 500
    sort_by = request.args.get('sort_by', 'score')
    limit = int(request.args.get('limit', 10))

    if sort_by not in df.columns:
        return jsonify({"error": f"Kolom '{sort_by}' tidak valid untuk pengurutan."}), 400

    select_cols = ['title', 'appId', 'genre', 'score', 'minInstalls', 'price', 'icon_path']
    valid_cols = [c for c in select_cols if c in df.columns]

    top_apps = df.orderBy(desc(sort_by)).select(*valid_cols).limit(limit).collect()
    return jsonify({"apps": [row.asDict() for row in top_apps]})

@app.route('/recommend_similar_app_by_name/<path:app_name_input>', methods=['GET'])
def recommend_similar_apps_by_name(app_name_input):
    if df is None: return jsonify({"error": "Data tidak tersedia."}), 500
    try:
        input_app_row = df.filter(lower(col('title')) == app_name_input.lower()).first()
        if not input_app_row: return jsonify({"error": f"Aplikasi '{app_name_input}' tidak ditemukan."}), 404
        
        input_cluster_id = input_app_row['cluster']
        input_app_id = input_app_row['appId']
        
        if input_cluster_id is None:
            return jsonify({"error": "Aplikasi referensi tidak memiliki informasi cluster."}), 404
            
        candidate_df = df.filter((col('cluster') == input_cluster_id) & (col('appId') != input_app_id))
        
        # [MODIFIKASI] Memastikan icon_path ada di output
        select_cols = ['title', 'appId', 'genre', 'score', 'minInstalls', 'price', 'icon_path']
        valid_cols = [c for c in select_cols if c in candidate_df.columns]
        
        recommendations = [row.asDict() for row in candidate_df.orderBy(desc("score")).select(*valid_cols).limit(10).collect()]
        
        return jsonify({
            "input_app_found": input_app_row.asDict(),
            "recommendations": recommendations
        })
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

# Endpoint ini tetap sama, hanya perlu dipastikan kolom yang dipilih ada
@app.route('/apps_in_cluster/<int:cluster_id>', methods=['GET'])
def get_apps_in_cluster(cluster_id):
    if df is None: return jsonify({"error": "Data tidak tersedia."}), 500
    if 'cluster' not in df.columns: return jsonify({"error": "Informasi cluster tidak tersedia."}), 500
    
    apps_in_cluster_df = df.filter(col('cluster') == cluster_id).orderBy(desc("score"))
    
    # [MODIFIKASI] Memastikan icon_path ada di output
    select_cols = ['title', 'appId', 'genre', 'score', 'minInstalls', 'price', 'icon_path']
    valid_cols = [c for c in select_cols if c in apps_in_cluster_df.columns]

    apps_list = apps_in_cluster_df.select(*valid_cols).limit(20).collect()
    return jsonify({"apps": [row.asDict() for row in apps_list]})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
