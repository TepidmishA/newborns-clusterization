"""
Main module for adding geographic coordinates to CSV data based on location.

This script reads a CSV file containing location data, fetches geographic coordinates
for each location using the Yandex Maps API, and writes the enriched data (with coordinates)
into a new CSV file.

Dependencies:
    - Yandex Maps API: for converting location names into geographic coordinates.
    - GeoCoordinatesAdder: custom class for handling the coordinate addition process.

Usage:
    1. Set your Yandex Maps API key by replacing 'your_yandex_api_key' in the code.
    2. Provide the path to the input CSV file containing location data.
    3. Define the path to the output CSV file where the enriched data will be saved.
    4. Run the script to add coordinates to the data.
"""

import os
from src.geo_coordinates.geo_coordinates import NominatimOSM


if __name__ == '__main__':
    input_filepath = os.path.abspath("data/filtered_data.csv")
    output_filepath = os.path.abspath("data/output_with_coordinates.csv")

    geo_adder = NominatimOSM(input_filepath, output_filepath)
    geo_adder.add_coordinates_and_save()
