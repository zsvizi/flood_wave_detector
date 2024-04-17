from src import PROJECT_PATH

import os
import gdown
import pandas as pd


class Dataloader:
    def __init__(self, dataset_name: str = None):
        os.makedirs(os.path.join(PROJECT_PATH, 'data'), exist_ok=True)
        if dataset_name is None:
            self.dataset_name = 'adatok_fontos_korrigalt'
        else:
            self.dataset_name = dataset_name
        self.download_data()
        self.meta = self.get_metadata()
        self.data = self.read_data()

    def download_data(self):
        if not self.do_all_files_exist():
            url = "https://drive.google.com/drive/folders/1gCC5gLKBh8NLWt_ham42EGk_WKQk_c_B"
            output = os.path.join(PROJECT_PATH, 'data')
            gdown.download_folder(url=url, output=output)

    @staticmethod
    def do_all_files_exist() -> bool:
        """
        This function checks whether all the files we wish to download already exist

        :return bool: True if all of them exist, False if at least one is missing
        """

        files = ["adatok_fontos.csv", "existing_stations.json", "level_groups_fontos.json", "meta_fontos.csv",
                 "nullpontok_fontos.json", "adatok_fontos_korrigalt.csv", "mederatmetszesek_fontos_korrigalt.csv"]

        for file in files:
            if not os.path.exists(os.path.join(PROJECT_PATH, 'data', file)):
                return False

        return True

    @staticmethod
    def get_metadata():
        meta = pd.read_csv(os.path.join(PROJECT_PATH, 'data', 'meta_fontos.csv'), index_col=0, sep=";") \
            .groupby(["river"]) \
            .get_group("Tisza") \
            .sort_values(by='river_km', ascending=False)
        return meta

    def read_data(self):
        data = pd.read_csv(os.path.join(PROJECT_PATH, 'data', self.dataset_name + '.csv'), index_col=0, sep=";")
        date = pd.to_datetime(data['Date']).dt.strftime('%Y-%m-%d')

        def isnumber(x):
            try:
                float(x)
                return True
            except:
                return False
        df = data[data.applymap(isnumber)]
        df['Date'] = date
        df = df.set_index(df['Date'])
        return df
