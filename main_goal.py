import enum
from datetime import datetime
from typing import Optional
import json
import requests
from airflow.decorators import dag, task


class State(enum.Enum):
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"


class FlightDetails:
    icao24: str
    first_seen: int
    est_departure_airport: Optional[str]
    last_seen: int
    est_arrival_airport: str
    callsign: str
    est_departure_airport_horiz_distance: Optional[int]
    est_departure_airport_vert_distance: Optional[int]
    est_arrival_airport_horiz_distance: int
    est_arrival_airport_vert_distance: int
    departure_airport_candidates_count: int
    arrival_airport_candidates_count: int

class ReadingTheFile:
    json_of_flight_details = ""

    def read_api(self):
        response = requests.get(
            "https://opensky-network.org/api/flights/arrival?airport=LFPG&begin=1669908851&end=1670081651")
        for i in response.json():
            # flight_details = FlightDetails()
            # flight_details.icao24 = i['icao24']
            # flight_details.first_seen = i['firstSeen']
            # flight_details.est_departure_airport = i['estDepartureAirport']
            # flight_details.last_seen = i['lastSeen']
            # flight_details.est_arrival_airport = i['estArrivalAirport']
            # flight_details.callsign = i['callsign']
            # flight_details.est_departure_airport_horiz_distance = i['estDepartureAirportHorizDistance']
            # flight_details.est_departure_airport_vert_distance = i['estDepartureAirportVertDistance']
            # flight_details.est_arrival_airport_horiz_distance = i['estArrivalAirportHorizDistance']
            # flight_details.est_arrival_airport_vert_distance = i['estArrivalAirportVertDistance']
            # flight_details.departure_airport_candidates_count = i['departureAirportCandidatesCount']
            # flight_details.arrival_airport_candidates_count = i['arrivalAirportCandidatesCount']
            #
            # self.list_of_flights_details.append(flight_details)
            self.json_of_flight_details += i

    def get_the_json(self):
        return self.json_of_flight_details


@dag(
    schedule=None,
    start_date=datetime(2022, 1, 12),
    catchup=False
)
def main_goal():


    @task
    def read_from_opensky_api() -> str:
        ReadingTheFile.read_api()
        return ReadingTheFile.get_the_json()

    @task
    def write_to_json_file(js_file: str):
        with open('flight_details.json', 'w') as outfile:
            json.dump(js_file, outfile)

    json_file = read_from_opensky_api()
    write_to_json_file(json_file)


_ = main_goal()
