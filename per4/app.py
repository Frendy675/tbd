from minio import Minio
from minio.error import S3Error
import pandas as pd
from io import BytesIO

# Koneksi ke MinIO API
client = Minio(
    "localhost:9000",
    access_key="ACCESS_KEY_KAMU",
    secret_key="SECRET_KEY_KAMU",
    secure=False
)

bucket_name = "bronze"
object_name = "Book1.csv"

try:
    # Ambil file dari MinIO
    response = client.get_object(
        bucket_name,
        object_name
    )

    # Baca isi CSV
    data = response.read()

    # Masukkan ke pandas DataFrame
    df = pd.read_csv(BytesIO(data))

    # Tutup koneksi response
    response.close()
    response.release_conn()

    print("CSV berhasil dibaca!")
    print("\nIsi data:")
    print(df)

except S3Error as e:
    print("Error MinIO:", e)

except Exception as e:
    print("Error:", e)
