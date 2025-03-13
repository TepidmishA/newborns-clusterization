"""
Data Reading Module

Provides abstract and concrete implementations for reading and
parsing annotation data from various sources.

Classes:
    :DataReader: Abstract base class for data reading implementations.
    :CsvReader: CSV parser for reading and parsing data from a CSV file.

Dependencies:
    :csv: for reading CSV files.
    :re: for regular expression operations used in data parsing.
    :abc: for abstract base class support.
"""

import csv
import re
from abc import ABC, abstractmethod


class DataReader(ABC):
    """
    Abstract base class for data reading implementations.
    """

    def __init__(self, filepath: str):
        """
        :param filepath: Path to data source file.
        """
        self.file_path = filepath

    @abstractmethod
    def read(self):
        """Parse and return annotation data."""


class CsvReader(DataReader):
    """
    A utility class for reading and parsing data from a CSV file.

    The CSV file should have the following format:
        location, child_weight, child_height, [list of risk factors]

    - `location` (str): The geographical location where the data was collected.
    - `child_weight` (int): The weight of the child in kilograms.
    - `child_height` (int): The height of the child in centimeters.
    - [list of risk factors] (list[bool]): A list of boolean values indicating
      the presence (True) or absence (False) of various risk factors.
    """

    def __init__(self, filepath: str):
        """
        :param filepath: Path to data source file.
        """
        super().__init__(filepath)
        self.add_header = True      # Whether to include the header row in the output.

    def read(self):
        """
        Parses the CSV file, skipping the header row.

        :return: list of tuples containing parsed data by rows.
        :raises FileNotFoundError, ValueError, csv.Error, OSError: if any error occurs during
                file reading or parsing.
        """
        parsed_data = []
        try:
            with open(self.file_path, mode='r', encoding='ANSI') as file:
                reader = csv.reader(file, delimiter=';')
                if not self.add_header:
                    next(reader, [])  # Skip header

                for row in reader:
                    if len(row) != 77:
                        raise ValueError(f"Incorrect line in the file {self.file_path}: {row}")

                    location, child_weight, child_height, *risk_factors = row

                    # Split by any of the specified delimiters
                    child_weight_parts = [part.strip() for part in
                                          re.split(r"[,/\.]", child_weight)]
                    child_height_parts = [part.strip() for part in
                                          re.split(r"[,/\.]", child_height)]

                    # Check that the number of components between fields is the same and that all
                    # components within a field have the same length
                    if len(child_weight_parts) != len(child_height_parts) or \
                            len(set(map(len, child_weight_parts))) != 1 or \
                            len(set(map(len, child_height_parts))) != 1 or \
                            len(child_weight_parts[0]) == 0 or len(str(location)) == 0:
                        raise ValueError(f"Incorrect line in the file {self.file_path}: {row}")

                    # Process each pair of values
                    for i in range(len(child_height_parts)):
                        row_data = (
                            str(location),
                            int(child_weight_parts[i]),
                            int(child_height_parts[i]),
                            *[bool(data) for data in risk_factors]
                        )
                        parsed_data.append(row_data)

        except FileNotFoundError:
            raise
        except (ValueError, csv.Error) as e:
            raise ValueError(f"Data format error in {self.file_path}: {e}") from e
        except OSError as e:
            raise OSError(f"File system error accessing {self.file_path}: {e}") from e

        return parsed_data
