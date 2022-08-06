from typing import Union
import json


class JsonHelper:
    """This class is for shrinking other classes.

    Since reading/writing from/to files is really common in this code base, using this class makes it tidier.
    """
    @staticmethod
    def write(filepath: str, obj: Union[dict, tuple, list], log: bool = True) -> None:
        """
        Writes the given object to a file

        :param filepath: The path to write to
        :param obj: The object containing the contents of the file
        :param log: Boolean whether to print the log or not
        :return:
        """
        if log:
            print(f'Writing to file: {filepath}')
        with open(filepath, 'w') as file:
            json.dump(obj=obj, fp=file, indent=4)

    @staticmethod
    def read(filepath: str, log: bool = True) -> dict:
        """
        Reads the file with the given path

        :param filepath: The path to read from
        :param log: Boolean whether to print the log or not
        :return:  The contents of the file
        """
        if log:
            print(f'Reading from file: {filepath}')
        with open(filepath, 'r') as file:
            return json.load(file)
