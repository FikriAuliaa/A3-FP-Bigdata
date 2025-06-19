# consumer.py - VERSI LENGKAP DENGAN SARAN PERBAIKAN

from kafka import KafkaConsumer
import pandas as pd
import time
import json
from datetime import datetime
import os

# Nama folder tempat file batch CSV akan disimpan
BATCH_FOLDER = "batch_data"

# Nama Topik Kafka (HARUS SAMA DENGAN DI PRODUCER.PY)
KAFKA_TOPIC = 'tapive_playstore_dataset'

# ==============================================================================
# Fungsi untuk menyimpan batch ke file CSV (DENGAN PENINGKATAN)
# ==============================================================================
def save_batch_to_file(batch_data, folder_name, file_name_only):
    """
    Menyimpan list of dictionaries ke sebuah file CSV dengan urutan kolom yang konsisten.
    """
    # Buat folder jika belum ada
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Folder '{folder_name}' telah dibuat.")

    full_file_path = os.path.join(folder_name, file_name_only)
    
    df = pd.DataFrame(batch_data)

    # --- PENINGKATAN: Mendefinisikan dan Men enforcing Urutan Kolom ---
    # PENTING: Sesuaikan daftar & urutan kolom ini agar cocok dengan file CSV asli Anda!
    # Ini memastikan setiap file batch CSV memiliki struktur yang sama persis.
    expected_columns = [
        # Daftar kolom utama dari dataset Anda
        'appId', 'title', 'genre', 'score', 'minInstalls', 'price', 'developer', 'developerId',
        'reviews', 'currency', 'genreId', 'icon', 'headerImage', 'screenshots', 'video',
        'videoImage', 'contentRating', 'contentRatingDescription', 'adSupported', 'containsAds',
        'inAppPurchases', 'editorsChoice', 'released', 'lastUpdatedOn', 'version',
        'privacyPolicy', 'summary', 'description', 'minAndroidVersion', 'maxInstalls',
        # Kolom baru yang kita tambahkan dari producer
        'icon_path', 'icon_category_assigned'
    ]

    # Ambil kolom yang ada di DataFrame saat ini
    current_cols = df.columns.tolist()
    # Gabungkan dengan daftar yang diharapkan untuk memastikan semua kolom ter-cover,
    # tapi utamakan urutan dari expected_columns.
    final_ordered_cols = [col for col in expected_columns if col in current_cols]
    # Tambahkan kolom lain yang mungkin ada di df tapi tidak terduga (jika ada)
    for col in current_cols:
        if col not in final_ordered_cols:
            final_ordered_cols.append(col)

    # Re-indeks DataFrame. Ini akan mengatur ulang kolom sesuai urutan final_ordered_cols.
    # Jika ada kolom di `final_ordered_cols` yang tidak ada di `df` (misal batch data error),
    # kolom itu akan dibuat dengan nilai NaN, menjaga konsistensi.
    df = df.reindex(columns=final_ordered_cols)
    # -----------------------------------------------------------------

    # Tentukan apakah header perlu ditulis (hanya untuk file baru)
    write_header = not os.path.exists(full_file_path)
    
    # Simpan ke CSV dengan mode 'append'
    df.to_csv(full_file_path, index=False, mode='a', header=write_header) 
    print(f"Batch data ({len(df)} baris) telah disimpan di {full_file_path}")


# ==============================================================================
# Bagian Utama Skrip Consumer
# ==============================================================================
try:
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers='localhost:29092',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        auto_offset_reset='earliest',
    )
    print(f"Kafka consumer berhasil terhubung, mendengarkan topik: {KAFKA_TOPIC}")
except Exception as e:
    print(f"Gagal terhubung ke Kafka: {e}")
    exit()

# Variabel untuk batch processing
batch_data_list = [] 
time_window = 60  # Jangka waktu (detik) untuk menyimpan batch
last_saved_time = time.time()

print(f"Kafka consumer dimulai. Menyimpan data setiap {time_window} detik. Menunggu pesan...")
try:
    # Loop terus menerus untuk memeriksa waktu dan pesan secara aktif
    while True:
        # Ambil pesan dengan timeout agar loop tidak sepenuhnya blocking
        messages = consumer.poll(timeout_ms=1000, max_records=500)

        if messages: # Jika ada pesan yang diterima
            for tp, msg_list in messages.items():
                for message in msg_list:
                    data = message.value  
                    batch_data_list.append(data)
        
        current_time = time.time()

        # Cek jika jendela waktu telah terlewati
        if current_time - last_saved_time >= time_window:
            if batch_data_list: # Hanya simpan jika ada data di batch
                file_name_only = f"batch_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                save_batch_to_file(batch_data_list, BATCH_FOLDER, file_name_only)
                batch_data_list = [] # Reset batch setelah disimpan
            
            # Reset timer setelah time window tercapai, meskipun batch kosong
            last_saved_time = current_time 
            print(f"Time window tercapai. Timer direset, menunggu pesan berikutnya...")

except KeyboardInterrupt:
    print("\nConsumer dihentikan oleh pengguna (Ctrl+C).")
except Exception as e:
    print(f"Terjadi error pada consumer loop: {e}")
    import traceback
    traceback.print_exc()
finally:
    # Menyimpan sisa data yang mungkin ada di batch sebelum keluar
    if batch_data_list:
        print("Menyimpan sisa data sebelum keluar...")
        file_name_only = f"batch_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}_final.csv"
        save_batch_to_file(batch_data_list, BATCH_FOLDER, file_name_only)
    
    if 'consumer' in locals() and consumer:
        print("Menutup Kafka consumer.")
        consumer.close()