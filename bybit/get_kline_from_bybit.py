import ccxt
import datetime
import pandas as pd
from datetime import datetime, timezone, timedelta

exchange = ccxt.bybit()

start_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
end_date = datetime(2024, 3, 20, tzinfo=timezone.utc)

start_timestamp = int(start_date.timestamp()) * 1000
end_timestamp = int(end_date.timestamp()) * 1000

df_list = []
while True:

    kline = exchange.publicGetV5MarketKline(
        {"symbol": "BTCUSDT", "end": end_timestamp, "interval": "15", "limit": 200}
    )["result"]["list"]

    df = pd.DataFrame(
        kline, columns=["timestamp", "op", "hi", "lo", "cl", "volume", "turnover"]
    )
    df["timestamp"] = pd.to_datetime(df["timestamp"].astype(int), unit="ms", utc=True)
    df_list.append(df)

    # print(kline)

    # 最後の時間を取得
    last_time = int(kline[-1][0])
    end_timestamp = last_time - 1

    if last_time < start_timestamp:
        break
    if len(kline) < 10:
        break

final_df = pd.concat(df_list, ignore_index=True)

final_df = final_df.iloc[::-1].reset_index(drop=True)
