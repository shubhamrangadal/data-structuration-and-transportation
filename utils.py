import requests


def read_weather_data(city: str, date: str) -> dict:
    # Get weather data from OpenWeatherMap API for the given city and date
    api_key = '1cf51456fc78575f038084e0f8573bf8'
    response = requests.get(f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric')
    weather_data = response.json()

    # Return weather data as dictionary
    return {'weather_data': weather_data}