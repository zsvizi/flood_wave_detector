from src.data.dataloader import Dataloader


class FloodWaveData:
    """This class gathers metadata in one place.

    Stores all the essential metadata and the Dataloader instance.
    """
    def __init__(self, dataset_name: str = None):
        """
        Constructor for FloodWaveData class

        """

        self.dataloader = Dataloader(dataset_name=dataset_name)
        self.data = self.dataloader.data
        self.meta = self.dataloader.meta
        self.gauges = self.meta.dropna(subset=['h_table']).index.tolist()
