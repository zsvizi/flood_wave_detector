from src import PROJECT_PATH

import os
import pandas as pd


class Dataloader:
    def __init__(self, dataset_name: str = None):
        os.makedirs(os.path.join(PROJECT_PATH, 'data'), exist_ok=True)
        self.meta = self.get_metadata()
        if dataset_name is None:
            self.dataset_name = '1951_2020'
        else:
            self.dataset_name = dataset_name
        self.data = self.read_data()

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
