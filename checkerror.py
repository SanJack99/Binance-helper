import requests
import time
import datetime


def get_status_code_200(url: str = "https://api.binance.com/api/v3/exchangeInfo") -> bool:
    return requests.get(url=url).status_code == 200
    

def main():
    start = datetime.datetime.now()
    sleep = 1

    while True:
        if (datetime.datetime.now() - start).seconds > 300:
            start = datetime.datetime.now() 
            if sleep > 0:
                sleep -= 0.1
                sleep = round(sleep, 1)
            else:
                print(f'{sleep = } break loop')
                break 
            print(f"Sleep interval = {sleep} seconds")
            time.sleep(60)
        
        if not get_status_code_200():
            print(f"Lower sleep interval equal {sleep = } seconds")
            break
        
        time.sleep(sleep)


if __name__ == "__main__":
    main()