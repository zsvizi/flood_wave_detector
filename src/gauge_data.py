class GaugeData:
    def __init__(self, value: float, is_peak: bool = False) -> None:
        self.value = value
        self.is_peak = is_peak

    def __str__(self) -> str:
        return f'({self.value}, {self.is_peak})'
