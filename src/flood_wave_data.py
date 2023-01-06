import os

from data_ativizig.dataloader import Dataloader


class FloodWaveData:
    """This class gathers metadata in one place.

    Stores all the essential metadata and the Dataloader instance.
    """
    def __init__(self):
        """
        Constructor for FloodWaveData class

        """
        self.__db_credentials_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database.ini')
        self.dataloader = Dataloader(self.__db_credentials_path)
        self.meta = self.dataloader.meta_data \
            .groupby(["river"]) \
            .get_group("Tisza") \
            .sort_values(by='river_km', ascending=False)
        self.gauges = self.meta.dropna(subset=['h_table']).index.tolist()
