"""
Data Filtering Module

Provides abstract and concrete implementations for filtering data from CSV files.

Classes:
    :DataFilter: Abstract base class for data filter implementations.
    :SkipFilter: A utility class for reading and filtering incorrect data from a CSV file.

Dependencies:
    :csv: for reading and writing CSV files.
    :re: for regular expression operations used in data parsing.
    :abc: for abstract base class support.
"""

import csv
import re
from abc import ABC, abstractmethod


class DataFilter(ABC):
    """
    Abstract base class for data filter implementations.
    """

    def __init__(self, filepath: str):
        """
        :param filepath: Path to data source file.
        """
        self.file_path = filepath

    @abstractmethod
    def filter(self):
        """Parse and save filtered data."""


class SkipFilter(DataFilter):
    """
    A utility class for reading and filtering incorrect data from a CSV file.

    The CSV file should have the following format:
        location, child_weight, child_height, [list of risk factors]

    - `location` (str): The geographical location where the data was collected.
    - `child_weight` (int): The weight of the child in kilograms.
    - `child_height` (int): The height of the child in centimeters.
    - [list of risk factors] (list[bool]): A list of boolean values indicating
      the presence (True) or absence (False) of various risk factors.

    Filtering criteria:
    - The number of columns in the row must be 77.
    - The `child_weight` and `child_height` fields must contain the same number of components after
      splitting by delimiters (`,`, `/`, `.`).
    - All components within the `child_weight` and `child_height` fields must have the same length
      (no mixed-length components).
    - The `child_weight` and `child_height` values must not be empty.
    - The `location` field must not be empty.
    """
    def filter(self):
        """
        Reads the CSV file, filters rows based on specific conditions,
        and writes the filtered data into a new CSV file (filtered_data.csv).

        :return: None
        :raises FileNotFoundError, ValueError, csv.Error, OSError: if any error occurs during
                file reading or writing.
        """
        try:
            # Open the input file for reading and the output file for writing
            with open(self.file_path, mode='r', encoding='ANSI') as input_file, \
                    open('filtered_data.csv', mode='w', newline='', encoding='ANSI') as output_file:

                reader = csv.reader(input_file, delimiter=';')
                writer = csv.writer(output_file, delimiter=';')

                # Read the header and write it to the output file
                header = next(reader)
                writer.writerow(header)

                for row in reader:
                    if len(row) != 77:
                        continue  # Skip rows with incorrect number of columns

                    location, child_weight, child_height, *_ = row

                    # Split values by the specified delimiters and remove leading/trailing spaces
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
                        continue  # Skip the row if conditions are not met

                    # Write the valid row to the output file
                    writer.writerow(row)

        except FileNotFoundError:
            raise
        except (ValueError, csv.Error) as e:
            raise ValueError(f"Data format error in {self.file_path}: {e}") from e
        except OSError as e:
            raise OSError(f"File system error accessing {self.file_path}: {e}") from e
