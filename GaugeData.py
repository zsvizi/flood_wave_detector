class GaugeData:
    def __init__(self):
        self.value = None
        self.is_peak = False

    def __str__(self):
        return f'({self.value}, {self.is_peak})'
