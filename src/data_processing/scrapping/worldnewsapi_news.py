import json
import os

import requests


def worldnewsapi_generate_url(
        key_word: str, api_key: str,
        start_date: str, end_date: str,
        latitude: float | None = None,
        longitude: float | None = None,
        radius: int | None = None
        ) -> str:

    url = "https://api.worldnewsapi.com/search-news"
    url += f"?text={key_word}"
    url += f"&earliest-publish-date={start_date}"
    url += f"&latest-publish-date={end_date}"

    if latitude is not None and longitude is not None and radius is not None:
        url += f"&location-filter={latitude},{longitude},{radius}"

    url += f"&api-key={api_key}"

    return url


def worldnewsapi_parse_news(news: list[dict]) -> list[dict[str, str]]:
    parsed_news = []
    for record in news:
        parsed_news.append({
            "data": record["text"],
            "date": record["publish_date"]
        })
    return parsed_news


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    newsapi_key = os.getenv("WORLDNEWSAPI_API")
    key_word = "cocoa"
    start_date = "2024-10-15"
    end_date = "2024-10-18"

    url = worldnewsapi_generate_url(
        key_word, newsapi_key,
        start_date, end_date,
        5.46, 6.36, 100
        )

    response = requests.get(url)
    articles = json.loads(response.text)["news"]
    parsed_articles = worldnewsapi_parse_news(articles)
    print(parsed_articles)
