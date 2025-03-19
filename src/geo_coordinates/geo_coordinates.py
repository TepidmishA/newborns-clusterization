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
import time
from tqdm import tqdm  # Импортируем tqdm для прогресс-бара


class CoordinatesAdder(ABC):
    """
    Abstract base class for adding coordinates to .csv file.

    The CSV file should have the following format:
        hospital_num, history_num, location, ...
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
        # Open the output file for writing
        with (open(self.input_filepath, mode='r', encoding='ANSI') as input_file, \
              open(self.output_filepath, mode='w', newline='', encoding='ANSI')
              as output_file):
            reader = csv.reader(input_file, delimiter=';')
            writer = csv.writer(output_file, delimiter=';')

            # Read the header and write it to the output file
            header = next(reader)
            writer.writerow(["latitude", "longitude"] + header)

            for row in tqdm(reader, desc="Processing", unit=" row"):
                hospital_num, history_num,  location, *_ = row
                latitude, longitude = self.get_coordinates(location)

                # Add coordinates to the beginning of the row
                new_row = [latitude, longitude] + row
                writer.writerow(new_row)
                print(f"\n{hospital_num}, {history_num}: {new_row}")
                time.sleep(1)


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

    def get_coordinates(self, address: str) -> (float, float):
        url = "https://nominatim.openstreetmap.org/search"
        params = {
            "q": address,
            "format": "json",
            "limit": 2
        }

        for attempt in range(3):
            try:
                response = requests.get(url, params=params,
                                        headers={"User-Agent": "geo-coordinates-adder"}, timeout=6)
                response.raise_for_status()
                data = response.json()

                if data:
                    latitude = float(data[0]["lat"])
                    longitude = float(data[0]["lon"])
                    return latitude, longitude

            except requests.RequestException as e:
                print(f"Attempt {attempt + 1}: Error fetching coordinates for {address}: {e}")
                time.sleep(1)

        return None, None


class PhotonOSM(CoordinatesAdder):
    """
    A utility class for adding coordinates (latitude and longitude) to the data from a CSV file
    based on the location addresses provided in the first column of the CSV file.

    The class will use the Photon API (OpenStreetMap) to obtain coordinates for the addresses.
    """

    def __init__(self, input_filepath: str, output_filepath: str):
        """
        :param input_filepath: Path to the CSV file containing the data.
        :param output_filepath: Path to the output CSV file where the data
                                with coordinates will be saved.
        """
        super().__init__(input_filepath, output_filepath)

    def get_coordinates(self, address: str) -> (float, float):
        url = f"https://geocode.xyz/{address}"
        params = {"json": 1}

        for attempt in range(3):
            try:
                response = requests.get(url, params=params, timeout=6)
                response.raise_for_status()
                data = response.json()

                if "latt" in data and "longt" in data:
                    return float(data["latt"]), float(data["longt"])

            except requests.RequestException as e:
                print(f"Error fetching coordinates for {address}: {e}")
                time.sleep(1)

        return None, None


class MultiServiceGeocoder(CoordinatesAdder):
    """
    A geocoder that attempts to get coordinates using multiple services.
    It first tries Yandex, then Nominatim, and finally Photon.
    """

    def __init__(self, input_filepath: str, output_filepath: str):
        """
        :param yandex_api_key: API key for Yandex Geocoding API.
        :param input_filepath: Path to the CSV file containing the data.
        :param output_filepath: Path to the output CSV file where the data
                                with coordinates will be saved.
        """
        super().__init__(input_filepath, output_filepath)

    @staticmethod
    def get_coordinates_nominatim(address: str) -> (float, float):
        """Gets coordinates using Nominatim OSM."""
        url = "https://nominatim.openstreetmap.org/search"
        params = {"q": address, "format": "json", "limit": 1}

        try:
            response = requests.get(url, params=params, headers={"User-Agent": "geo-coordinates-adder"}, timeout=6)
            response.raise_for_status()
            data = response.json()

            if data:
                return float(data[0]["lat"]), float(data[0]["lon"])
        except requests.RequestException as e:
            print(f"Nominatim error for {address}: {e}")
        except ValueError as e:
            print(e)

        return None, None

    @staticmethod
    def get_coordinates_photon(address: str) -> (float, float):
        """Gets coordinates using Photon API."""
        url = f"https://geocode.xyz/{address}"
        params = {"json": 1}

        try:
            response = requests.get(url, params=params, timeout=6)
            response.raise_for_status()
            data = response.json()

            if "latt" in data and "longt" in data:
                return float(data["latt"]), float(data["longt"])
        except requests.RequestException as e:
            print(f"Photon error for {address}: {e}")
        except ValueError as e:
            print(e)

        return None, None

    @staticmethod
    def get_coordinates_GeocodeMapsCo(address: str) -> (float, float):
        """
        Получает широту и долготу из Geocode.maps.co на основе адреса.

        :param address: Адрес местоположения.
        :return: Кортеж (широта, долгота) при успешном получении, иначе (None, None).
        """
        url = "https://geocode.maps.co/search"
        params = {
            "q": address,
            "api_key": "api_key"
        }
        global currGeocodeMapsCo
        if currGeocodeMapsCo > 4800:
            print(f"GeocodeMapsCo error for {address}: {e}")
            return
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            currGeocodeMapsCo = currGeocodeMapsCo + 1
            data = response.json()

            if data:
                latitude = float(data[0]["lat"])
                longitude = float(data[0]["lon"])
                return latitude, longitude

        except requests.RequestException as e:
            print(f"GeocodeMapsCo error for {address}: {e}")
        except ValueError as e:
            print(e)

        return None, None

    def get_coordinates(self, address: str) -> (float, float):
        """Tries multiple services to get coordinates."""
        for method in [self.get_coordinates_nominatim, self.get_coordinates_photon, self.get_coordinates_GeocodeMapsCo]:
            latitude, longitude = method(address)
            if latitude and longitude:
                return latitude, longitude

            time.sleep(1.5)  # Pause to avoid being blocked

        return None, None


class GeoCheckOSM(CoordinatesAdder):
    """
    A utility class for adding coordinates (latitude and longitude) to the data from a CSV file
    based on the location addresses provided in the first column of the CSV file.

    The class will use the Photon API (OpenStreetMap) to obtain coordinates for the addresses.
    """

    def __init__(self, input_filepath: str, output_filepath: str):
        """
        :param input_filepath: Path to the CSV file containing the data.
        :param output_filepath: Path to the output CSV file where the data
                                with coordinates will be saved.
        """
        super().__init__(input_filepath, output_filepath)

    def get_coordinates(self, address: str) -> (float, float):
        url = "https://www.ideeslibres.org/GeoCheck/geocoder.php"
        params = {"q": address, "geocoder": "photon"}
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            if data:
                latitude = float(data["firstlat"])
                longitude = float(data["firstlng"])
                return latitude, longitude
        except requests.RequestException as e:
            print(f"Error fetching {address}: {e}")
        except ValueError as e:
            print(f"Value error {address}: {e}")
        except TypeError as e:
            print(f"Type error {address}: {e}")
        return None, None
