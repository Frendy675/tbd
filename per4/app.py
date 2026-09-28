from minio import Minio
from minio.error import S3Error
import pandas as pd
from io import BytesIO


# ==========================================
# Koneksi ke MinIO
# ==========================================

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="secretsecret",
    secure=False
)


# ==========================================
# Konfigurasi
# ==========================================

source_bucket = "silver"
source_file = "Book1_clean.csv"

target_bucket = "gold"
target_file = "Book1_aggregate.csv"


try:

    # ==========================================
    # STEP 1 - BACA DATA DARI SILVER
    # ==========================================

    print("Membaca data dari bucket silver...")

    response = client.get_object(
        source_bucket,
        source_file
    )

    data = response.read()

    response.close()
    response.release_conn()

    df = pd.read_csv(
        BytesIO(data),
        sep=";"
    )

    print("\nData dari silver:")
    print(df)


    # ==========================================
    # STEP 2 - HITUNG TOTAL PER BARIS
    # ==========================================

    df["total"] = df["harga"] * df["jumlah"]

    print("\nData dengan total:")
    print(df)


    # ==========================================
    # STEP 3 - AGGREGATE
    # ==========================================

    aggregate_df = pd.DataFrame({
        "total_jumlah": [df["jumlah"].sum()],
        "total": [df["total"].sum()]
    })


    print("\nHasil aggregate:")
    print(aggregate_df)


    # ==========================================
    # STEP 4 - BUAT BUCKET GOLD
    # ==========================================

    if not client.bucket_exists(target_bucket):

        client.make_bucket(target_bucket)

        print(
            f"\nBucket '{target_bucket}' berhasil dibuat."
        )

    else:

        print(
            f"\nBucket '{target_bucket}' sudah tersedia."
        )


    # ==========================================
    # STEP 5 - UBAH HASIL AGGREGATE KE CSV
    # ==========================================

    csv_buffer = BytesIO()

    aggregate_df.to_csv(
        csv_buffer,
        index=False,
        sep=";"
    )

    csv_buffer.seek(0)


    # ==========================================
    # STEP 6 - SIMPAN KE GOLD
    # ==========================================

    client.put_object(
        target_bucket,
        target_file,
        csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv"
    )


    print("\n================================")
    print("Aggregate berhasil disimpan!")
    print("Bucket :", target_bucket)
    print("File   :", target_file)
    print("================================")


except S3Error as e:

    print("Error MinIO:", e)


except Exception as e:

    print("Error:", e)
    