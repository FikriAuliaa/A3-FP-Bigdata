# producer.py - VERSI DIPERBARUI

from kafka import KafkaProducer
import pandas as pd
import time
import random
import json
import os

# --- KONFIGURASI UTAMA ---
DATASET_FILE = 'google-play-dataset-by-tapivedotcom.csv'
KAFKA_TOPIC = 'tapive_playstore_dataset'
# Path ke folder utama yang berisi folder-folder kategori ikon
BASE_ICON_PATH = r'dataset-gambar/Icons-50/Icons-50' # Gunakan 'r' untuk raw string

# --- KAMUS PEMETAAN GENRE KE IKON ---
GENRE_TO_ICON_MAP = {
    # Produktivitas & Bisnis
    'Tools': ['blade', 'clock', 'disk'], 'Productivity': ['books', 'building', 'envelope', 'clock'],
    'Business': ['building', 'disk', 'envelope'], 'Finance': ['building', 'disk'],
    # Hiburan & Gaya Hidup
    'Entertainment': ['emotion_face', 'ball', 'biking', 'bunny_ears'], 'Games': ['ball', 'biking', 'cartwheeling', 'blade', 'feline'],
    'Music & Audio': ['disk', 'emotion_face'], 'Health & Fitness': ['biking', 'drinks'], 'Shopping': ['disk', 'flag'],
    # Sosial & Komunikasi
    'Communication': ['envelope', 'emotion_face', 'family'], 'Social': ['family', 'emotion_face'],
    # Pengetahuan & Perjalanan
    'Education': ['books', 'building', 'arrow_directions'], 'Books & Reference': ['books', 'envelope'],
    'Travel & Local': ['airplane', 'boat', 'fast_train', 'flag', 'building'], 'Maps & Navigation': ['arrow_directions', 'flag', 'airplane'],
    # Default untuk genre yang tidak terdaftar
    'default': ['arrow_directions', 'cloud', 'disk', 'flag']
}

# --- FUNGSI HELPER ---
def send_to_kafka(data, producer, topic):
    try:
        producer.send(topic, value=data)
    except Exception as e:
        print(f"Gagal mengirim data: {e}")

# --- INISIALISASI ---
# 1. Inisialisasi Kafka Producer
try:
    producer = KafkaProducer(
        bootstrap_servers='localhost:29092',
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
        retries=3,
        linger_ms=100
    )
    print(f"Kafka Producer berhasil terhubung ke localhost:29092, topik: {KAFKA_TOPIC}")
except Exception as e:
    print(f"Gagal terhubung ke Kafka: {e}"); exit()

# 2. Baca dataset utama
try:
    print(f"Membaca dataset dari file: {DATASET_FILE}...")
    df = pd.read_csv(DATASET_FILE, low_memory=False)
    print(f"Dataset '{DATASET_FILE}' berhasil dimuat. Jumlah baris: {len(df)}")
except Exception as e:
    print(f"ERROR saat membaca file CSV '{DATASET_FILE}': {e}"); exit()

# 3. Pindai dan cache daftar file ikon (OPTIMASI)
# Ini dilakukan sekali saja di awal untuk menghindari scan folder berulang kali di dalam loop
print(f"Memindai direktori ikon dari: {BASE_ICON_PATH}...")
icon_files_by_category = {}
try:
    for category in os.listdir(BASE_ICON_PATH):
        category_path = os.path.join(BASE_ICON_PATH, category)
        if os.path.isdir(category_path):
            files = [f for f in os.listdir(category_path) if f.lower().endswith(('.png', '.jpg', '.jpeg'))]
            if files: # Hanya tambahkan jika kategori tidak kosong
                icon_files_by_category[category] = files
    print(f"Ditemukan {len(icon_files_by_category)} kategori ikon yang valid.")
except Exception as e:
    print(f"ERROR saat memindai folder ikon: {e}"); exit()


# --- PROSES UTAMA PENGIRIMAN DATA ---
print(f"\nMemulai pengiriman data ke topik Kafka: {KAFKA_TOPIC}...")
total_rows_sent = 0
try:
    for index, row in df.iterrows():
        # Konversi baris ke dictionary dan bersihkan nilai NaN
        data_dict = row.to_dict()
        cleaned_data = {key: None if pd.isna(value) else value for key, value in data_dict.items()}

        # --- LOGIKA BARU: Pemetaan dan Penambahan Path Ikon ---
        app_genre = cleaned_data.get('genre')
        icon_path = None
        chosen_icon_category = None

        # 1. Tentukan kategori ikon mana yang akan digunakan berdasarkan genre
        icon_categories_for_genre = GENRE_TO_ICON_MAP.get(app_genre, GENRE_TO_ICON_MAP['default'])
        chosen_icon_category = random.choice(icon_categories_for_genre)

        # 2. Pilih file ikon acak dari kategori yang telah dipilih
        if chosen_icon_category in icon_files_by_category:
            # Pastikan daftar file untuk kategori tersebut tidak kosong
            if icon_files_by_category[chosen_icon_category]:
                chosen_icon_file = random.choice(icon_files_by_category[chosen_icon_category])
                # 3. Buat path lengkap ke file ikon yang dipilih
                icon_path = os.path.join(BASE_ICON_PATH, chosen_icon_category, chosen_icon_file)

        # 4. Tambahkan informasi baru ke pesan JSON yang akan dikirim
        cleaned_data['icon_path'] = icon_path
        cleaned_data['icon_category_assigned'] = chosen_icon_category
        # --------------------------------------------------------

        # Mengirim data yang sudah diperkaya ke Kafka
        send_to_kafka(cleaned_data, producer, KAFKA_TOPIC)
        total_rows_sent += 1

        if (index + 1) % 1000 == 0:
             print(f"{index + 1} baris telah diproses dan dikirim...")

        # Delay kecil untuk simulasi streaming
        time.sleep(random.uniform(0.001, 0.005))

except Exception as e:
    print(f"Terjadi error saat mengirim data: {e}")
    import traceback
    traceback.print_exc()
finally:
    if producer:
        print("\nMenunggu semua pesan terkirim (flushing)...")
        producer.flush()
        producer.close()
        print("Kafka Producer ditutup.")
    print(f"Pengiriman selesai. Total {total_rows_sent} baris data dikirim ke Kafka.")