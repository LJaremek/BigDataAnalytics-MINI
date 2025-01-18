import time
import json
import os

from bs4 import BeautifulSoup
import requests


def newsapi_generate_url(
        key_words: list[str],
        start_date: str,
        end_date: str,
        api_key: str
        ) -> str:
    """
    Generates a News API URL with specified keywords and API key.

    Input:
     * key_words: list[str] - List of keywords to search for
     * start_date: str - Date like `2024-12-11T19:31:56`
     * end_date: str - Date like `2024-12-11T19:31:56`
     * api_key: str - API key for authentication

    Output:
     * url: str
    """
    key_words = [
        word.replace(" ", "%20")
        for word in key_words
    ]

    url = "https://newsapi.org/v2/everything?q="
    url += "%20AND%20".join(key_words)
    url += f"&from={start_date}&to={end_date}"
    url += "&searchIn=content&language=en&apiKey="
    url += api_key

    return url


def newsapi_fetch_article_text(url: str) -> str:
    """
    Fetches article text from a given URL.

    Input:
     * url: str - The URL of the article

    Output:
     * text: str
    """
    response = requests.get(url)
    time.sleep(1)
    if response.status_code != 200:
        return None

    soup = BeautifulSoup(response.content, "html.parser")

    paragraphs = soup.find_all("p")
    text = "\n".join([
        p.get_text()
        for p in paragraphs
        if p.get_text().strip()
        ])

    return text if text else "No content found"


def newsapi_parse_articles(articles: list[dict]) -> dict[str, str]:
    """
    Parses a list of articles, filtering and fetching article content.

    Input:
     * articles: list[dict] - List of article data dictionaries

    Output:
     * parsed_articles: dict[str, str]
    """
    parsed_articles = []
    for article in articles:
        if article["url"] == "https://removed.com":
            continue

        try:
            text = newsapi_fetch_article_text(article["url"])
            if text is None:
                continue
        except requests.exceptions.ConnectionError:
            continue

        parsed_articles.append({
            "date": article["publishedAt"],
            "data": text
        })

    return parsed_articles


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    newsapi_key = os.getenv("news_api")
    key_words = ["cocoa", "Ivory Coast"]

    start_date = "2024-11-18T00:00:00"
    end_date = "2024-11-20T00:00:00"

    url = newsapi_generate_url(key_words, start_date, end_date, newsapi_key)

    response = requests.get(url)
    articles = json.loads(response.text)["articles"]
    parsed_articles = newsapi_parse_articles(articles)

    print(parsed_articles)
