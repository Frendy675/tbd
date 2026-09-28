from minio import Minio
from minio.error import S3Error
import pandas as pd
from io import BytesIO

# =========================
# Koneksi ke MinIO
# =========================
client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="secretsecret",
    secure=False
)

# =========================
# Konfigurasi
# =========================
source_bucket = "bronze"
source_file = "Book1.csv"

target_bucket = "silver"
target_file = "Book1_clean.csv"


try:
    # ==========================================
    # STEP 1 - BACA DATA DARI BUCKET BRONZE
    # ==========================================
    print("Membaca data dari bronze...")

    response = client.get_object(
        source_bucket,
        source_file
    )

    data = response.read()

    response.close()
    response.release_conn()

    # Karena CSV menggunakan ;
    df = pd.read_csv(
        BytesIO(data),
        sep=";"
    )

    print("\nData sebelum cleaning:")
    print(df)

    # ==========================================
    # STEP 2 - PEMBERSIHAN DATA
    # ==========================================

    # Bersihkan spasi pada nama kolom
    df.columns = df.columns.str.strip()

    # Bersihkan spasi pada data string
    df["nama"] = df["nama"].astype(str).str.strip()

    # Ubah kolom numerik menjadi angka
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df["harga"] = pd.to_numeric(df["harga"], errors="coerce")
    df["jumlah"] = pd.to_numeric(df["jumlah"], errors="coerce")

    # Hapus data yang memiliki nilai kosong
    # pada kolom wajib
    df = df.dropna(
        subset=["id", "nama", "harga", "jumlah"]
    )

    # Hapus data duplikat
    df = df.drop_duplicates()

    # Pastikan tipe data integer
    df["id"] = df["id"].astype(int)
    df["harga"] = df["harga"].astype(int)
    df["jumlah"] = df["jumlah"].astype(int)

    print("\nData setelah cleaning:")
    print(df)

    # ==========================================
    # STEP 3 - BUAT BUCKET SILVER
    # ==========================================

    if not client.bucket_exists(target_bucket):
        client.make_bucket(target_bucket)
        print(f"\nBucket '{target_bucket}' berhasil dibuat.")
    else:
        print(f"\nBucket '{target_bucket}' sudah tersedia.")

    # ==========================================
    # STEP 4 - UBAH DATAFRAME KE CSV
    # ==========================================

    csv_buffer = BytesIO()

    df.to_csv(
        csv_buffer,
        index=False,
        sep=";"
    )

    csv_buffer.seek(0)

    # ==========================================
    # STEP 5 - UPLOAD KE SILVER
    # ==========================================

    client.put_object(
        target_bucket,
        target_file,
        csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv"
    )

    print("\nData bersih berhasil disimpan!")
    print(f"Bucket : {target_bucket}")
    print(f"File   : {target_file}")

except S3Error as e:
    print("Error MinIO:", e)

except Exception as e:
    print("Error:", e)
