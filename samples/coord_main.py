"""
Main module for adding geographic coordinates to CSV data based on location.

This script reads a CSV file containing location data, fetches geographic coordinates
for each location using the Yandex Maps API, and writes the enriched data (with coordinates)
into a new CSV file.
"""

import os
from src.geo_coordinates.geo_coordinates import GeoCheckOSM


if __name__ == '__main__':
    input_filepath = os.path.abspath("../data/filtered_data.csv")
    output_filepath = os.path.abspath("../data/output_with_coordinates.csv")

    geo_adder = GeoCheckOSM(input_filepath, output_filepath)
    geo_adder.add_coordinates_and_save()
