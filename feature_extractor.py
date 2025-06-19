import os
import glob
import pandas as pd
from PIL import Image
import numpy as np
from tqdm import tqdm # Library untuk progress bar, install dengan: pip install tqdm

# --- KONFIGURASI ---
# Path ke folder yang berisi semua file CSV dari consumer
BATCH_DATA_FOLDER = "batch_data"
# Nama file output yang akan berisi fitur-fitur gambar
OUTPUT_PARQUET_FILE = "image_features.parquet"

# --- Fungsi Ekstraksi Fitur (mirip seperti di Spark, tapi versi Python murni) ---
def extract_features_from_image_path(path):
    try:
        if path is None or not isinstance(path, str) or not os.path.exists(path):
            return None
        
        img = Image.open(path).convert('RGB')
        img_array = np.array(img)
        avg_colors = np.mean(img_array, axis=(0, 1))
        width, height = img.size
        
        return {
            "img_avg_red": float(avg_colors[0]),
            "img_avg_green": float(avg_colors[1]),
            "img_avg_blue": float(avg_colors[2]),
            "img_width": int(width),
            "img_height": int(height)
        }
    except Exception as e:
        # print(f"Warning: Gagal memproses gambar {path}: {e}")
        return None

# --- SCRIPT UTAMA ---
if __name__ == "__main__":
    print("Memulai proses pra-pemrosesan dan ekstraksi fitur gambar...")

    # Baca semua file CSV dari folder batch_data
    all_csv_files = glob.glob(os.path.join(BATCH_DATA_FOLDER, "*.csv"))
    if not all_csv_files:
        print(f"Error: Tidak ada file CSV ditemukan di folder '{BATCH_DATA_FOLDER}'.")
        exit()
        
    print(f"Membaca {len(all_csv_files)} file CSV...")
    df = pd.concat((pd.read_csv(f) for f in all_csv_files), ignore_index=True)
    print(f"Total {len(df)} baris data dimuat.")

    # Ambil kolom yang kita butuhkan: appId dan icon_path
    df_images = df[['appId', 'icon_path']].copy()
    # Hapus baris di mana icon_path kosong atau duplikat
    df_images.dropna(subset=['icon_path'], inplace=True)
    df_images.drop_duplicates(subset=['icon_path'], inplace=True)
    
    print(f"Ditemukan {len(df_images)} path ikon unik untuk diproses.")

    results = []
    # Gunakan tqdm untuk melihat progress bar di terminal
    for index, row in tqdm(df_images.iterrows(), total=df_images.shape[0], desc="Mengekstrak Fitur"):
        icon_path = row['icon_path']
        features = extract_features_from_image_path(icon_path)
        
        if features:
            features['appId'] = row['appId'] # Tambahkan appId untuk kunci join
            results.append(features)

    if not results:
        print("Tidak ada fitur gambar yang berhasil diekstrak.")
        exit()

    # Konversi hasil list of dictionaries ke pandas DataFrame
    features_df = pd.DataFrame(results)
    
    # Simpan ke file Parquet untuk efisiensi
    try:
        features_df.to_parquet(OUTPUT_PARQUET_FILE, index=False)
        print(f"\n✓ Ekstraksi fitur selesai. Hasil disimpan di: '{OUTPUT_PARQUET_FILE}'")
        print(f"Total {len(features_df)} fitur gambar berhasil disimpan.")
    except Exception as e:
        print(f"\nError saat menyimpan file Parquet: {e}")