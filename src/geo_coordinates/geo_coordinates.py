"""
Module for adding geographic coordinates to CSV data based on location addresses.

This module reads location data from a CSV file, fetches latitude and longitude coordinates
for each location using either the Yandex Geocoding API or the Nominatim API (OpenStreetMap),
and writes the enriched data into a new CSV file.

Classes:
    :CoordinatesAdder: Abstract base class for handling coordinate fetching and CSV processing.
    :YandexMap: Handles fetching coordinates using the Yandex Geocoding API.
    :NominatimOSM: Handles fetching coordinates using the Nominatim API (OpenStreetMap).

Dependencies:
    :csv: For reading and writing CSV files.
    :requests: For making API calls to geocoding services.
    :CsvReader: Utility class for reading CSV data.
"""

import csv
from abc import ABC, abstractmethod
import requests
from src.utils.data_reader import CsvReader


class CoordinatesAdder(ABC):
    """
    Abstract base class for adding coordinates to .csv file.
    """

    def __init__(self, input_filepath: str, output_filepath: str):
        """
        :param input_filepath: Path to the CSV file containing the data.
        :param output_filepath: Path to the output CSV file where the data
                                with coordinates will be saved.
        """
        self.input_filepath = input_filepath
        self.output_filepath = output_filepath

    @abstractmethod
    def get_coordinates(self, address: str) -> (float, float):
        """
        Gets latitude and longitude from Yandex Geocoding API based on the address.

        :param address: The address of the location.
        :return: tuple of (latitude, longitude) if successful, otherwise (None, None).
        """

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


class YandexMap(CoordinatesAdder):
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
        super().__init__(input_filepath, output_filepath)
        self.api_key = api_key

    def get_coordinates(self, address: str) -> (float, float):
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


class NominatimOSM(CoordinatesAdder):
    """
    A utility class for adding coordinates (latitude and longitude) to the data from a CSV file
    based on the location addresses provided in the first column of the CSV file.

    The class will use the Nominatim API (OpenStreetMap) to obtain coordinates for the addresses.
    """

    def __init__(self, input_filepath: str, output_filepath: str):
        """
        :param input_filepath: Path to the CSV file containing the data.
        :param output_filepath: Path to the output CSV file where the data
                                with coordinates will be saved.
        """
        super().__init__(input_filepath, output_filepath)
        self.cache = {}  # Cache to store already fetched coordinates

    def get_coordinates(self, address: str) -> (float, float):
        """
        Gets latitude and longitude from Nominatim API based on the address.

        :param address: The address of the location.
        :return: tuple of (latitude, longitude) if successful, otherwise (None, None).
        """
        if address in self.cache:
            return self.cache[address]

        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": address,
            "format": "json",
            "limit": 1
        }

        try:
            response = requests.get(url, params=params,
                                    headers={"User-Agent": "geo-coordinates-adder"}, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes
            data = response.json()

            if data:
                latitude = float(data[0]["lat"])
                longitude = float(data[0]["lon"])
                self.cache[address] = (latitude, longitude)
                return latitude, longitude
            return None, None
        except requests.RequestException as e:
            print(f"Error fetching coordinates for {address}: {e}")
            return None, None
