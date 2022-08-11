from data_ativizig.dataloader import Dataloader
from src import PROJECT_PATH

from google.cloud import storage
import os


class CombinedDataloader:
    def __init__(self):
        """self.__db_credentials_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database.ini')
        self.db = Dataloader(self.__db_credentials_path)
        self.meta = self.db.meta_data \
            .groupby(["river"]) \
            .get_group("Tisza") \
            .sort_values(by='river_km', ascending=False)
        self.gauges = self.meta.dropna(subset=['h_table']).index.tolist()"""

    @staticmethod
    def get_files_from_bucket():
        """Downloads a blob from the bucket."""
        # The ID of your GCS bucket
        # bucket_name = "your-bucket-name"

        # The ID of your GCS object
        # source_blob_name = "storage-object-name"

        # The path to which the file should be downloaded
        # destination_file_name = "local/path/to/file"
        storage_client = storage.Client()
        bucket = storage_client.bucket("ativizig-phase-3")

        blob = bucket.blob("fwd-data-1951-pre-edited/data-and-meta/fwd-data-edited_1876_1950.csv")
        os.makedirs(os.path.join(PROJECT_PATH, 'data'), exist_ok=True)
        blob.download_to_filename(os.path.join(PROJECT_PATH, 'data', 'data_pre_1951.csv'))

        print(
            "Downloaded storage object {} from bucket {} to local file {}.".format(
                "fwd-data-1951-pre-edited/data-and-meta/fwd-data-edited_1876_1950.csv", "ativizig-phase-3",
                os.path.join(PROJECT_PATH, 'data', 'data_pre_1951.csv')
            )
        )


if __name__ == "__main__":
    comb_dataloader = CombinedDataloader
    comb_dataloader.get_files_from_bucket()
