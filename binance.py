import datetime
from typing import Optional
import requests
from requests import RequestException
import csv
import json
from pydantic import BaseModel
import re


class BinanceObject(BaseModel):
    aggregate_trade_id: int
    price: float
    quantity: float
    first_trade_id: int
    last_trade_id: int
    time_stamp: int
    was_the_buyer: bool
    was_the_trade: bool


class BinanceAnalyzer:
    def __init__(
        self, 
        symbol: str,
        start_date: str,
        end_date: str,
        interval: int
    ):
        self.symbol = symbol
        self.delta = self.find_delta_timezone(data=start_date)
        self.start_time = self.get_timestamp(date_str=start_date) + self.delta
        self.end_time = self.get_timestamp(date_str=end_date) + self.delta
        self.interval = self.get_timestamp(interval=interval)
        self.file_name = f'{self.symbol}_{datetime.datetime.now().strftime("%d-%m-%Y_%H:%M:%S")}.csv'
        

    def find_delta_timezone(self, data: str) -> int:
        diff_time = datetime.datetime.astimezone(self.convert_str_to_datetime(data))
        date_tz = str(diff_time.timetz())
        value_symbol = True if date_tz.find("+") else False
        value = int(date_tz.split("+")[-1].split(':')[0]) \
            if value_symbol else int(date_tz.split("-")[-1].split(':')[0])

        return value * 3600 * 1000 if value_symbol else -1 * (value * 3600 * 1000) 


    def convert_str_to_datetime(self, date: str) -> datetime:
        return datetime.datetime.strptime(date, '%d-%m-%Y %H:%M:%S')


    def get_timestamp(self, date_str: Optional[str] = None, interval: Optional[int] = None) -> int:
        """
        Convert str(date) to int(timestamp)
        """
        if interval:
            return interval * 1000
        return int(round(self.convert_str_to_datetime(date_str).timestamp() * 1000))


    def create_csv_file(self):
        with open(f'./csv_data/{self.file_name}', 'w', newline='') as csvfile:
            fieldnames = [
                "aggregate_trade_id",
                "price",
                "quantity",
                "first_trade_id",
                "last_trade_id",
                "time_stamp",
                "was_the_buyer",
                "was_the_trade",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()


    def add_row_to_csv(self, transaction_data: BinanceObject):
        with open(f'./csv_data/{self.file_name}', 'a', newline='') as csvfile:
            fieldnames = [
                "aggregate_trade_id",
                "price",
                "quantity",
                "first_trade_id",
                "last_trade_id",
                "time_stamp",
                "was_the_buyer",
                "was_the_trade",
            ]

            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writerow(
                {
                    "aggregate_trade_id": transaction_data.aggregate_trade_id, 
                    "price": transaction_data.price,
                    "quantity": transaction_data.quantity,
                    "first_trade_id": transaction_data.first_trade_id,
                    "last_trade_id": transaction_data.last_trade_id,
                    "time_stamp": transaction_data.time_stamp,
                    "was_the_buyer": transaction_data.was_the_buyer,
                    "was_the_trade": transaction_data.was_the_trade,
                }
            )


    def get_transactions_to_json(self, start_time: int, end_time: int) -> json:
        """
        param:

        start_time: int (timestamp value)
        end_time: int (timestamp value)

        return:

        json object with transactions on interval (start_time -> end_time)
        """
        link = f"https://api.binance.com/api/v3/aggTrades?symbol={self.symbol}&startTime={start_time}&endTime={end_time}"

        try:
            req = requests.get(
                url=link
            )
        except RequestException:
            return None

        return req.json()


    def run(self):
        self.create_csv_file()
        
        for start in range(self.start_time, self.end_time, self.interval):
            json_data = self.get_transactions_to_json(start_time=start, end_time=start + self.interval) 
            
            if not json_data:
                print('Warning! Not valid data')
                continue
            print(json_data)
            for transaction_item in json_data:
                print(transaction_item)

                transaction_object = BinanceObject(
                    aggregate_trade_id=transaction_item.get('a'),
                    price=transaction_item.get('p'),
                    quantity=transaction_item.get('q'),
                    first_trade_id=transaction_item.get('f'),
                    last_trade_id=transaction_item.get('l'),
                    time_stamp=transaction_item.get("T"),
                    was_the_buyer=transaction_item.get("m"),

                    was_the_trade=transaction_item.get("M"),
                )
                self.add_row_to_csv(transaction_data=transaction_object)
        

def check_data(date: str):
    pattern = r"^[0-3][\d]-[0-1][\d]-[\d]{4} [0-2][\d]:[0-6][\d]:[0-6][\d]"

    if not re.match(pattern, date):
        raise ValueError(f"Date {date} is not valid\n\
            Try this format for your date value (\"22-12-2021 12:11:40\")")


def main():

    symbol = input('Enter symbol (ex. "BTCUSDT"): ')
    start_date = input('Enter start datetime (ex. "22-10-2021 06:00:00"): ').strip()
    check_data(start_date)
    end_date = input('Enter finish datetime (ex. "22-10-2021 12:00:00"): ').strip()
    check_data(end_date)
    interval = input('Enter int value of interval (10 < interval <= 180) or skip (default 30 seconds): ')
    interval = int(interval) if isinstance(interval, int) else 30

    obj = BinanceAnalyzer(
        symbol=symbol,
        start_date=start_date.strip(),
        end_date=end_date.strip(),
        interval=interval if interval and (interval in range(10, 181)) else 30  
    )

    obj.run()
     
        
if __name__ == '__main__':
    main()