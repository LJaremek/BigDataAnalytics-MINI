from datetime import datetime, timedelta
import json

import requests


DAILY_PARAMETERS = (
    "temperature_2m_min",
    "temperature_2m_max",
    "rain_sum",
    "sunshine_duration"
)

COCOA_LOCATIONS = {
    "Ivory Coast": {
        "regions": [
            {
                "name": "Soubre",
                "latitude": 5.46,
                "longitude": 6.36,
                "weight": 0.4
            },
        ],
        "weight": 0.4
    }
}

RESOURCES = {
    "cocoa": COCOA_LOCATIONS
}


def generate_forecast_url(
        latitude: float,
        longitude: float,
        hourly_parameters: list[str],
        start_date: str | None = None,
        end_date: str | None = None,
        ) -> str:

    url = "https://api.open-meteo.com/v1/forecast?"
    url += f"latitude={latitude}&longitude={longitude}"
    url += f"&daily={','.join(hourly_parameters)}"

    if start_date is not None:
        url += f"&start_date={start_date}"
    if end_date is not None:
        url += f"&end_date={end_date}"

    return url


def get_weather(
        start_date: str,
        end_date: str,
        resource: str
        ) -> dict[str, float]:

    assert len(start_date) == len("YYYY-MM-DD")
    assert len(end_date) == len("YYYY-MM-DD")

    results = {
        "temperature": [],
        "rain": [],
        "sun": []
    }

    for _, country in RESOURCES[resource].items():
        cw = country["weight"]  # country_weight
        country_results = {
            "temperature": [],
            "rain": [],
            "sun": []
        }

        for region in country["regions"]:
            rw = region["weight"]  # region_weight

            latitude = region["latitude"]
            longitude = region["longitude"]
            url = generate_forecast_url(
                latitude, longitude, DAILY_PARAMETERS, start_date, end_date
                )
            response = requests.get(url)
            data = json.loads(response.text)["daily"]

            min_temp = data["temperature_2m_min"][0]
            max_temp = data["temperature_2m_max"][0]

            country_results["temperature"].append((min_temp + max_temp)/2 * rw)
            country_results["rain"].append(data["rain_sum"][0] * rw)
            country_results["sun"].append(data["sunshine_duration"][0] * rw)

        temp = country_results["temperature"]
        results["temperature"].append(sum(temp)/len(temp)*cw)
        rain = country_results["rain"]
        results["rain"].append(sum(rain)/len(rain)*cw)
        sun = country_results["sun"]
        results["sun"].append(sum(sun)/len(sun)*cw)

    temp = results["temperature"]
    results["temperature"] = sum(temp)/len(temp)
    rain = results["rain"]
    results["rain"] = sum(rain)/len(rain)
    sun = results["sun"]
    results["sun"] = sum(sun)/len(sun)

    return results


def generate_date_range(start_date: str, end_date: str) -> list[str]:
    """
    Generate a list of dates between start_date and end_date (inclusive).

    :param start_date: The starting date in "YYYY-MM-DD" format.
    :param end_date: The ending date in "YYYY-MM-DD" format.
    :return: A list of dates in "YYYY-MM-DD" format.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    if start > end:
        raise ValueError("'start_date' must not be after 'end_date'.")

    date_list = [
        (start + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end - start).days + 1)
        ]

    return date_list


def get_weathers(
        start_date: str,
        end_date: str,
        resource: str
        ) -> list[dict[str, float]]:
    dates = generate_date_range(start_date, end_date)[1:]

    results = []
    for end_date in dates:
        results.append(get_weather(start_date, end_date, resource))
        start_date = end_date

    return results


if __name__ == "__main__":
    start_date = "2024-11-10"
    end_date = "2024-11-25"

    print(get_weathers(start_date, end_date, "cocoa"))
