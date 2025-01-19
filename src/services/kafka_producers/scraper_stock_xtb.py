import time
import os

from dotenv import load_dotenv
from xtb import XTB

from mongodb_logging import get_last_scraper_end_date, add_new_scraper_log
from tools import get_kafka_producer, get_date_one_month_ago, add_n_days
from tools import current_date, compare_dates


SCRAPER_NAME = "scraper_stock_xtb"
DATE_FORMAT = "%Y-%m-%d"
MINUTES = 5
DAYS = 1


if __name__ == "__main__":
    load_dotenv()

    DEBUG_MODE = bool(int(os.getenv("DEBUG_MODE")))
    print("[START] Mode:", DEBUG_MODE)

    producer = get_kafka_producer()

    last_end_date = get_last_scraper_end_date(SCRAPER_NAME)

    if last_end_date is None:
        date_start = get_date_one_month_ago(DATE_FORMAT)
    else:
        date_start = last_end_date
    date_end = add_n_days(date_start, DAYS, DATE_FORMAT)

    running = compare_dates(
        current_date(DATE_FORMAT), date_start, DATE_FORMAT, ">="
        )

    while running:
        print("Time:", date_start, date_end)
        if not DEBUG_MODE:
            xtb = XTB(
                os.getenv("XTB_USER_ID"),
                os.getenv("XTB_PASSWORD")
            )
            xtb.login()

            candlesticks = xtb.get_chart_range_request(
                date_start, date_end, "COCOA", "H1"
                )["returnData"]["rateInfos"]

            del xtb
        else:
            print("[WANR] Mock data")
            candlesticks = [
                {'ctm': 1732266000000, 'ctmString': 'Nov 22, 2024, 10:00:00 AM', 'open': 8714.0, 'close': -17.0, 'high': 0.0, 'low': -35.0, 'vol': 1157.0},  # noqa: E501
                {'ctm': 1732269600000, 'ctmString': 'Nov 22, 2024, 11:00:00 AM', 'open': 8698.0, 'close': 21.0, 'high': 59.0, 'low': -5.0, 'vol': 3904.0},  # noqa: E501
                {'ctm': 1732273200000, 'ctmString': 'Nov 22, 2024, 12:00:00 PM', 'open': 8721.0, 'close': 112.0, 'high': 117.0, 'low': -7.0, 'vol': 4241.0},  # noqa: E501
                {'ctm': 1732276800000, 'ctmString': 'Nov 22, 2024, 1:00:00 PM', 'open': 8833.0, 'close': 162.0, 'high': 220.0, 'low': -18.0, 'vol': 7331.0},  # noqa: E501
                {'ctm': 1732280400000, 'ctmString': 'Nov 22, 2024, 2:00:00 PM', 'open': 8992.0, 'close': 111.0, 'high': 130.0, 'low': -86.0, 'vol': 7601.0},  # noqa: E501
                {'ctm': 1732284000000, 'ctmString': 'Nov 22, 2024, 3:00:00 PM', 'open': 9099.0, 'close': 6.0, 'high': 26.0, 'low': -61.0, 'vol': 5894.0},  # noqa: E501
                {'ctm': 1732287600000, 'ctmString': 'Nov 22, 2024, 4:00:00 PM', 'open': 9107.0, 'close': 86.0, 'high': 121.0, 'low': 0.0, 'vol': 6942.0},  # noqa: E501
                {'ctm': 1732291200000, 'ctmString': 'Nov 22, 2024, 5:00:00 PM', 'open': 9190.0, 'close': -100.0, 'high': 96.0, 'low': -166.0, 'vol': 9747.0},  # noqa: E501
                {'ctm': 1732294800000, 'ctmString': 'Nov 22, 2024, 6:00:00 PM', 'open': 9084.0, 'close': -153.0, 'high': 6.0, 'low': -160.0, 'vol': 4417.0},  # noqa: E501
                {'ctm': 1732298400000, 'ctmString': 'Nov 22, 2024, 7:00:00 PM', 'open': 8930.0, 'close': 27.0, 'high': 29.0, 'low': -115.0, 'vol': 4383.0}  # noqa: E501
                ]

        data = {
            "source": "scraper_stock_xtb",
            "candlesticks": candlesticks,
            "time": time.strftime(DATE_FORMAT),
            "date_start": date_start,
            "date_end": date_end,
            "date_format": DATE_FORMAT
        }

        producer.send("scraped_data", value=data)

        count = len(data["candlesticks"])
        add_new_scraper_log(SCRAPER_NAME, date_start, date_end, count)

        date_start = date_end
        date_end = add_n_days(date_start, DAYS, DATE_FORMAT)

        if compare_dates(
                current_date(DATE_FORMAT), date_start, DATE_FORMAT, "<="
                ):
            running = False
            print("[INFO] running = False")

        time.sleep(60*MINUTES)
