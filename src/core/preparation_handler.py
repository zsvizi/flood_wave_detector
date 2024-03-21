import os

from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from src import PROJECT_PATH
from src.utils.json_helper import JsonHelper


class PreparationHandler:
    """
    This is a helper class for GraphPreparation
    e.g.: file reading, file sorting, date conversion, graph selection, etc...
    """

    @staticmethod
    def read_vertex_file(gauge: str, folder_name: str) -> pd.DataFrame:
        """
        Reads the generated vertex file of the station with the given ID

        :param str gauge: the ID of the desired station
        :param str folder_name: Name of the folder to use for file handling.
        :return pd.DataFrame: A Dataframe with the peak value and date
        """
        gauge_with_index = JsonHelper.read(os.path.join(PROJECT_PATH, folder_name,
                                                        'find_vertices', f'{gauge}.json'))
        list_with_index = [[i, gauge_with_index[i][0], gauge_with_index[i][1]] for i in list(gauge_with_index.keys())]
        gauge_peaks = pd.DataFrame(data=list_with_index,
                                   columns=['Date', 'Max value', 'Color'])
        gauge_peaks['Date'] = pd.to_datetime(gauge_peaks['Date'])
        return gauge_peaks

    @staticmethod
    def find_dates_for_next_gauge(
            actual_date: datetime,
            backward: int,
            next_gauge_candidate_vertices: pd.DataFrame,
            forward: int
    ) -> pd.DataFrame:
        """
        Searches for continuation of a component

        :param datetime actual_date: The date of the last peak
        :param int backward: The number of days allowed before a node for continuation (at a given gauge).
                            This parameter is also called as alpha.
        :param pd.DataFrame next_gauge_candidate_vertices: The time series of the subsequent station in a DataFrame
        :param int forward: The number of days allowed after a node for continuation (at a given gauge).
                            This parameter is also called as beta.
        :return pd.DataFrame: A DataFrame containing the found dates
        """

        dates = PreparationHandler.filter_for_start_and_length(
            candidate_vertices=next_gauge_candidate_vertices,
            date=actual_date,
            forward_span=forward,
            backward_span=backward
        )
        return dates

    @staticmethod
    def convert_datetime_to_str(
            actual_date: datetime,
            gauge_pair: dict,
            next_gauge_dates: pd.DataFrame,
            slopes
    ) -> None:
        """
        Converts the date(s) to our desired format string. Then the list of converted strings is stored in a dictionary

        :param datetime actual_date: The date to be converted
        :param dict gauge_pair: A dictionary to store the converted list of strings
        :param pd.DataFrame next_gauge_dates: A DataFrame containing the found dates to be converted
        :param slopes: slope or slopes between the two vertices
        """

        if not next_gauge_dates.empty:
            found_next_dates_str = next_gauge_dates['Date'].dt.strftime('%Y-%m-%d').tolist()
            gauge_pair[actual_date.strftime('%Y-%m-%d')] = (found_next_dates_str, slopes)

    @staticmethod
    def get_peak_list(peaks: pd.DataFrame, level_group: float) -> dict:
        """
        Creates a list containing (date, value, color) tuples.
        :param pd.DataFrame peaks: single column DataFrame which to convert
        :param float level_group: level group number of the gauge
        :return dict: dictionary
        """
        peak_tuples = peaks.to_records(index=True)
        peak_list = [
            tuple(x)
            for x in peak_tuples
        ]
        peak_list_new = {}
        for i in range(len(peak_list)):
            if peak_list[i][1] < level_group:
                color = "yellow"
            else:
                color = "red"
            peak_list_new[peak_list[i][0]] = [peak_list[i][1], color]
        return peak_list_new

    @staticmethod
    def clean_dataframe_for_getting_peak_list(
            local_peak_values: np.array,
            gauge_data: pd.DataFrame,
            reg_number: str
    ) -> pd.DataFrame:
        """
        Creates a dataframe containing a given station's peaks with the desired date format and data type

        :param np.array local_peak_values: The flagged time series of the desired station in a numpy array
        :param pd.DataFrame gauge_data: The time series of the desired station in a DataFrame
        :param str reg_number: The ID of the desired station
        :return pd.DataFrame: A DataFrame containing the given station's peaks with date index
        """

        peaks = gauge_data.loc[np.array([x.is_peak for x in local_peak_values])]
        peaks.info()
        peaks.index = pd.to_datetime(peaks.index).strftime('%Y-%m-%d')
        peaks.info()
        peaks = peaks.drop(columns="Date")  # .set_index(peaks.index.strftime('%Y-%m-%d'))
        peaks[reg_number] = peaks[reg_number].astype(float)
        return peaks

    @staticmethod
    def filter_for_start_and_length(
            candidate_vertices: pd.DataFrame,
            date: datetime,
            forward_span: int,
            backward_span: int
    ) -> pd.DataFrame:
        """
        Find possible follow-up dates for the component coming from the previous gauge

        :param pd.DataFrame candidate_vertices: Dataframe to crop
        :param datetime date: start date of the crop
        :param int forward_span: number of days we allow for continuing
        :param int backward_span: number of days we allow for delay
        :return pd.DataFrame: Cropped dataframe with found next dates.
        """

        max_date = date + timedelta(days=forward_span)
        min_date = date - timedelta(days=backward_span)
        possible_dates = candidate_vertices[(candidate_vertices['Date'] >= min_date) &
                                            (candidate_vertices['Date'] <= max_date)]

        return possible_dates

    @staticmethod
    def get_dates_in_between(start_date: str, end_date: str, intervals: dict, gauges: list) -> list:
        """
        This function returns dates from intervals that are in-between start_date and end_date

        :param str start_date: starting date
        :param str end_date: ending date
        :param str intervals: dictionary containing dates regarding the existence of gauges
        :param list gauges: list of gauges
        :return list: list of dates in-between start_date and end_date
        """

        all_dates = [start_date, end_date]
        for gauge in gauges:
            x = intervals[str(gauge)]["start"]
            y = intervals[str(gauge)]["end"]
            all_dates = all_dates + [x] if x not in all_dates else all_dates
            all_dates = all_dates + [y] if y not in all_dates else all_dates
        all_dates.sort(key=lambda d: datetime.strptime(d, '%Y-%m-%d'))
        cut_dates = [date for date in all_dates if start_date <= date <= end_date]

        return cut_dates
