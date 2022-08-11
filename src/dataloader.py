from data_ativizig.dataloader import Dataloader
from src import PROJECT_PATH

from typing import Union
from google.cloud import storage
import os
import pandas as pd


class CombinedDataloader:
    def __init__(self):
        os.makedirs(os.path.join(PROJECT_PATH, 'data'), exist_ok=True)
        self.get_files_from_bucket()

        self.__db_credentials_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database.ini')
        self.db = Dataloader(self.__db_credentials_path)

        self.meta = self.get_metadata()
        self.pre_data = self.create_pre_data()

    def get_files_from_bucket(self):
        """Downloads a blob from the bucket."""
        storage_client = storage.Client()
        origin_bucket = storage_client.bucket("ativizig-phase-3")

        src_data = "fwd-data-1951-pre-edited/data-and-meta/fwd-data-edited_1876_1950.csv"
        pre_data = origin_bucket.blob(src_data)

        dest_data = os.path.join(PROJECT_PATH, 'data', 'data_pre_1951.csv')
        pre_data.download_to_filename(dest_data)
        self.log(src_data, "ativizig-phase-3", dest_data)
        # TODO: Add meta data download

    def create_pre_data(self):
        raw_data = pd.read_csv(os.path.join(PROJECT_PATH, 'data', 'data_pre_1951.csv'), index_col=[0])[: -1]
        reg_name_pairs = self.meta['station_name'].to_dict()
        reg_name_pairs = dict((v, k) for k, v in reg_name_pairs.items())
        print(reg_name_pairs)
        pre_data = raw_data.rename(columns=reg_name_pairs)
        print(pre_data.columns)
        return pre_data

    def get_daily_time_series(self, reg_number_list: list, threshold: Union[None, int] = None):
        pro_data = self.db.get_daily_time_series(reg_number_list=reg_number_list, threshold=threshold)
        for reg_number in reg_number_list:
            if reg_number in self.pre_data:
                pass
            else:
                pass
        pass

    def get_metadata(self):
        meta = self.db.meta_data \
                    .groupby(["river"]) \
                    .get_group("Tisza") \
                    .sort_values(by='river_km', ascending=False)

        # TODO: Merge with new meta table
        return meta

    @staticmethod
    def log(source: str, bucket: str, destination: Union[str, bytes]):
        print(f"Downloaded storage object {source} from bucket {bucket} to local file {destination}.")


def filter_by_threshold(df, thresh):

    full_dates = df.index
    null_groups = df[df[df.columns[-1]].isnull()].groupby((~df[df.columns[-1]].isnull()).cumsum())
    for k, v in null_groups:
        if len(v) > thresh:
            full_dates = full_dates.difference(v.index)
    df = df.reindex(full_dates)
    return df


if __name__ == "__main__":
    comb_dataloader = CombinedDataloader()
    comb_dataloader.get_daily_time_series(reg_number_list=[2541, 1514, 1515])
