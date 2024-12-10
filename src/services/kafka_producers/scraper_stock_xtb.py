import time

from dotenv import load_dotenv
from xtb import XTB

from tools import get_kafka_producer


if __name__ == "__main__":
    load_dotenv()

    # xtb = XTB(
    #     os.getenv("XTB_USER_ID"),
    #     os.getenv("XTB_PASSWORD")
    # )
    # xtb.login()

    producer = get_kafka_producer()

    while True:
        date_start = "2024-11-22"
        date_end = "2024-11-25"
        # candlesticks = xtb.get_chart_range_request(date_start, date_end, "COCOA", "H1")["returnData"]["rateInfos"]
        candlesticks = [
            {'ctm': 1732266000000, 'ctmString': 'Nov 22, 2024, 10:00:00 AM', 'open': 8714.0, 'close': -17.0, 'high': 0.0, 'low': -35.0, 'vol': 1157.0},
            {'ctm': 1732269600000, 'ctmString': 'Nov 22, 2024, 11:00:00 AM', 'open': 8698.0, 'close': 21.0, 'high': 59.0, 'low': -5.0, 'vol': 3904.0},
            {'ctm': 1732273200000, 'ctmString': 'Nov 22, 2024, 12:00:00 PM', 'open': 8721.0, 'close': 112.0, 'high': 117.0, 'low': -7.0, 'vol': 4241.0},
            {'ctm': 1732276800000, 'ctmString': 'Nov 22, 2024, 1:00:00 PM', 'open': 8833.0, 'close': 162.0, 'high': 220.0, 'low': -18.0, 'vol': 7331.0},
            {'ctm': 1732280400000, 'ctmString': 'Nov 22, 2024, 2:00:00 PM', 'open': 8992.0, 'close': 111.0, 'high': 130.0, 'low': -86.0, 'vol': 7601.0},
            {'ctm': 1732284000000, 'ctmString': 'Nov 22, 2024, 3:00:00 PM', 'open': 9099.0, 'close': 6.0, 'high': 26.0, 'low': -61.0, 'vol': 5894.0},
            {'ctm': 1732287600000, 'ctmString': 'Nov 22, 2024, 4:00:00 PM', 'open': 9107.0, 'close': 86.0, 'high': 121.0, 'low': 0.0, 'vol': 6942.0},
            {'ctm': 1732291200000, 'ctmString': 'Nov 22, 2024, 5:00:00 PM', 'open': 9190.0, 'close': -100.0, 'high': 96.0, 'low': -166.0, 'vol': 9747.0},
            {'ctm': 1732294800000, 'ctmString': 'Nov 22, 2024, 6:00:00 PM', 'open': 9084.0, 'close': -153.0, 'high': 6.0, 'low': -160.0, 'vol': 4417.0},
            {'ctm': 1732298400000, 'ctmString': 'Nov 22, 2024, 7:00:00 PM', 'open': 8930.0, 'close': 27.0, 'high': 29.0, 'low': -115.0, 'vol': 4383.0}
            ]

        data = {
            "source": "scraper_stock_xtb",
            "candlesticks": candlesticks,
            "time": time.strftime("%Y-%d-%m %I:%M:%S")
        }

        producer.send("scraped_data", value=data)

        # time.sleep(15*60)
        time.sleep(10)
