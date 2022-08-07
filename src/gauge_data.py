class GaugeData:
    """This class is specific data structure for our purposes.

    A GaugeData instance contains the water level value in float, and a boolean True or False according to whether it
    is considered as a peak or not.
    """
    def __init__(self, value: float, is_peak: bool = False) -> None:
        """
        Constructor for GaugeData class

        :param value: The water level value in float
        :param is_peak: A boolean flag, if it is a peak then it's True, if it isn't then it's False
        """
        self.value = value
        self.is_peak = is_peak

    def __str__(self) -> str:
        """
        A method to be able to print GaugeData instance as a nicely formatted string

        :return: a nicely formatted string
        """
        return f'({self.value}, {self.is_peak})'
