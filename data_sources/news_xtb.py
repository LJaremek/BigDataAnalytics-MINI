from datetime import datetime
import os

from xtb import XTB


def xtb_parse_news(key_words: list[str], news: list[dict]) -> dict:
    parsed_news = []
    for record in news:
        tmp_record = record["body"].lower()
        good_record = True

        for key_word in key_words:
            if key_word not in tmp_record:
                good_record = False
                break

        if good_record:
            date = datetime.strptime(
                record["timeString"],
                "%b %d, %Y, %I:%M:%S %p"
                ).strftime("%Y-%m-%dT%H:%M:%SZ")

            parsed_news.append({
                "data": record["body"],
                "date": date
            })

    return parsed_news


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()

    xtb = XTB(
        os.getenv("XTB_USER_ID"),
        os.getenv("XTB_PASSWORD")
    )
    xtb.login()

    date = "2024-10-01"
    date2 = "2024-10-25"
    news = xtb.get_news(date, date2)["returnData"]
    key_words = ["kakao"]

    print(xtb_parse_news(key_words, news))
