"""
Module for adding geographic coordinates to CSV data based on location addresses.

This module reads location data from a CSV file, fetches latitude and longitude coordinates
for each location using the Yandex Geocoding API, and writes the enriched data into a new CSV file.

Classes:
    :GeoCoordinatesAdder: Handles fetching coordinates and writing the updated data.

Dependencies:
    :csv: for reading and writing CSV files.
    :requests: For making API calls to Yandex Geocoding.
    :CsvReader: Utility class for reading CSV data.
"""

import csv
import requests
from src.utils.data_reader import CsvReader


class GeoCoordinatesAdder:
    """
    A utility class for adding coordinates (latitude and longitude) to the data from a CSV file
    based on the location addresses provided in the first column of the CSV file.

    The class will use the Yandex Geocoding API to obtain coordinates for the addresses.
    """

    def __init__(self, api_key: str, input_filepath: str, output_filepath: str):
        """
        :param api_key: API key for Yandex Geocoding API.
        :param input_filepath: Path to the CSV file containing the data.
        :param output_filepath: Path to the output CSV file where the data
                                with coordinates will be saved.
        """
        self.api_key = api_key
        self.input_filepath = input_filepath
        self.output_filepath = output_filepath

    def get_coordinates(self, address: str):
        """
        Gets latitude and longitude from Yandex Geocoding API based on the address.

        :param address: The address of the location.
        :return: tuple of (latitude, longitude) if successful, otherwise (None, None).
        """
        url = "https://geocode-maps.yandex.ru/1.x/"
        params = {
            "geocode": address,
            "format": "json",
            "apikey": self.api_key
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()

            # Check if there are any results
            if data["response"]["GeoObjectCollection"]["featureMember"]:
                # Get the coordinates of the first feature in the response
                coordinates = \
                    data["response"]["GeoObjectCollection"]["featureMember"][0]["GeoObject"][
                        "Point"][
                        "pos"]
                latitude, longitude = coordinates.split()
                return float(latitude), float(longitude)
            return None, None
        except requests.RequestException as e:
            print(f"Error fetching coordinates for {address}: {e}")
            return None, None

    def add_coordinates_and_save(self):
        """
        Reads the data from the CSV file, adds coordinates to each location,
        and writes the data with coordinates to a new CSV file.

        :return: None
        """
        csv_reader = CsvReader(self.input_filepath)
        data = csv_reader.read()  # Get data from the CSV file

        # Open the output file for writing
        with open(self.output_filepath, mode='w', newline='', encoding='ANSI') as output_file:
            writer = csv.writer(output_file, delimiter=';')

            for row in data:
                location = row[0]  # First column contains the location address
                latitude, longitude = self.get_coordinates(location)

                # Add coordinates to the beginning of the row
                new_row = [latitude, longitude] + row
                writer.writerow(new_row)
