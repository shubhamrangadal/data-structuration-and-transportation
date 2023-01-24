import enum
from typing import Optional

import requests


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
    list_of_flights_details = []
    state_of_the_function: State

    def read_api(self):
        response = requests.get(
            "https://opensky-network.org/api/flights/arrival?airport=LFPG&begin=1669908851&end=1670081651")
        for i in response.json():
            flight_details = FlightDetails()
            flight_details.icao24 = i['icao24']
            flight_details.first_seen = i['firstSeen']
            flight_details.est_departure_airport = i['estDepartureAirport']
            flight_details.last_seen = i['lastSeen']
            flight_details.est_arrival_airport = i['estArrivalAirport']
            flight_details.callsign = i['callsign']
            flight_details.est_departure_airport_horiz_distance = i['estDepartureAirportHorizDistance']
            flight_details.est_departure_airport_vert_distance = i['estDepartureAirportVertDistance']
            flight_details.est_arrival_airport_horiz_distance = i['estArrivalAirportHorizDistance']
            flight_details.est_arrival_airport_vert_distance = i['estArrivalAirportVertDistance']
            flight_details.departure_airport_candidates_count = i['departureAirportCandidatesCount']
            flight_details.arrival_airport_candidates_count = i['arrivalAirportCandidatesCount']

            self.list_of_flights_details.append(flight_details)


    if len(list_of_flights_details) > 0:
        state_of_the_function = State.SUCCESS
    else:
        state_of_the_function = State.FAIL

    def get_the_list(self):
        return self.list_of_flights_details



