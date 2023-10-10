from src import PROJECT_PATH

import os
import pandas as pd
import gdown


class Dataloader:
    def __init__(self, dataset_name: str = None):
        os.makedirs(os.path.join(PROJECT_PATH, 'data'), exist_ok=True)
        if dataset_name is None:
            self.dataset_name = '1951_2020'
        else:
            self.dataset_name = dataset_name
        self.download_data()
        self.meta = self.get_metadata()
        self.data = self.read_data()

    def download_data(self):
        if not os.path.exists(os.path.join(PROJECT_PATH, 'data', self.dataset_name + ".csv")):
            url = 'https://drive.google.com/uc?id=1pDNckq54TQ4-bMOZiXTdHELJf4bKKqGm'
            output = os.path.join(PROJECT_PATH, 'data', self.dataset_name + ".csv")
            gdown.download(url, output, quiet=False)
        if not os.path.exists(os.path.join(PROJECT_PATH, 'data', 'level_groups' + '.json')):
            url = 'https://drive.google.com/uc?id=1Y0M5I8Kehcvgjb7678Wkg_vBp-aVUtGe'
            output = os.path.join(PROJECT_PATH, 'data', 'level_groups' + '.json')
            gdown.download(url, output, quiet=False)
        if not os.path.exists(os.path.join(PROJECT_PATH, 'data', 'meta' + '.csv')):
            url = 'https://drive.google.com/uc?id=1NDqGBvy1plDOG541z_3eBPrRguJGjVa-'
            output = os.path.join(PROJECT_PATH, 'data', 'meta' + '.csv')
            gdown.download(url, output, quiet=False)

    @staticmethod
    def get_metadata():
        meta = pd.read_csv(os.path.join(PROJECT_PATH, 'data', 'meta.csv'), index_col=0) \
            .groupby(["river"]) \
            .get_group("Tisza") \
            .sort_values(by='river_km', ascending=False)
        return meta

    def read_data(self):
        data = pd.read_csv(os.path.join(PROJECT_PATH, 'data', self.dataset_name + '.csv'), index_col=0)
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
