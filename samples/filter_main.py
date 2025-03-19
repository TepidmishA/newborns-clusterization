"""
Main module for filtering data from CSV files.
"""

import os
from src.utils.data_filter import SkipFilter


if __name__ == '__main__':
    input_filepath = os.path.abspath("../data/clusterization.csv")
    output_filepath = os.path.abspath("../data/filtered_data.csv")

    SkipFilter(input_filepath, output_filepath).filter()
