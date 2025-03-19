import requests
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from abc import ABC, abstractmethod


class CoordinatesAdderMultiWorker(ABC):
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

    def add_coordinates_and_save(self, max_workers=5):
        """
        Reads the data from the CSV file, adds coordinates to each location in parallel,
        and writes the data with coordinates to a new CSV file.
        """
        with (open(self.input_filepath, mode='r', encoding='ANSI') as input_file,
              open(self.output_filepath, mode='w', newline='', encoding='ANSI') as output_file):
            reader = csv.reader(input_file, delimiter=';')
            writer = csv.writer(output_file, delimiter=';')

            header = next(reader)
            writer.writerow(["latitude", "longitude"] + header)

            rows = list(reader)
            addresses = [row[2] for row in rows]  # Третий столбец — это адрес

            results = {}
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_row = {executor.submit(self.get_coordinates, addr): row for row, addr in
                                 zip(rows, addresses)}

                with tqdm(total=len(rows), desc="Fetching coordinates", unit=" row") as pbar:
                    for future in as_completed(future_to_row):
                        row = future_to_row[future]
                        hospital_num, history_num, location, *_ = row
                        try:
                            latitude, longitude = future.result()
                        except Exception as e:
                            print(f"Error processing {location}: {e}")
                            latitude, longitude = None, None

                        new_row = [latitude, longitude] + row
                        writer.writerow(new_row)
                        print(f"{hospital_num}, {history_num}: {new_row}")
                        pbar.update(1)


class GeoCheckOSMMulti(CoordinatesAdderMultiWorker):
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
        return None, None
