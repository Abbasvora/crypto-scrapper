import os
from dotenv import load_dotenv 
import requests
import pandas as pd
import websocket, json
from datetime import datetime
from multiprocessing import Pool

load_dotenv()

coins= ['btcusdt', 'ethusdt', 'ltcusdt', 'xrpusdt', 'xmrusdt', 'beamusdt']
INTERVAL = '1m'

def main(coin):
    data = get_data(coin)
    file_path = generate_df(data[0]['s'], data)
    send_message(file_path)
    os.remove(file_path)


def get_data(coin):
    
    """
    Gathers Data for given coin in specified interval
    
    Calls generate_df() and send_message()
    """
    
    SOCKET = f"wss://stream.binance.com:9443/ws/{coin}@kline_{INTERVAL}"
    x = []
    ws1 = websocket.WebSocket()
    ws1.connect(SOCKET)
    while True:
        json_message = json.loads(ws1.recv())
        if json_message['k']['x'] == True:
            temp = json_message.popitem()
            temp = temp[1]
            for k in temp.keys():
                json_message[k] = temp[k]
            x.append(json_message)
            
        if len(x) == 2:
            ws1.close(websocket.STATUS_PROTOCOL_ERROR)
            break
    data = x.copy()
    x.clear()
    return data
    
    

def generate_df(name, x):
    
    """Generates csv file from given dict.

    Parameters
    ----------
    
    `name`: Symbol name(used for filename)

    `x`: reference to list contaning dictionarys
    
    
    `Returns`: File Name
    """
    
    now = datetime.now()
    filename = now.strftime("%d-%m-%Y-%H-%M") 
    path = f'{name}_{INTERVAL}_{filename}.csv'
    df = pd.DataFrame()
    for i in x:
        i['e'] = [i['e']]
        df = df.append(pd.DataFrame.from_dict(i))
    
    x.clear()
    df.pop('e')
    df.rename(columns={'E': 'Time', 's': 'Symbol',
                     'c': 'Close', 'o': 'Open', 'h': 'High', 'l': 'Low', 'v': 'Volume',
                     'q': 'Quote Volume', 't':'Kline start time', 'T':'Kline close time',
                     'n':'Number of trades', 'q':'Quote asset volume', 'V':'Taker buy base asset volume',
                     'Q':'Taker buy quote asset volume'}, inplace=True)
    COL_NAME= 'Time'
    first_col = df.pop(COL_NAME)
    first_col = [datetime.utcfromtimestamp((int(i)/1000)).strftime('%Y-%m-%d %H:%M:%S') for i in first_col]
    df.insert(0, COL_NAME, first_col)
    df.to_csv(f'{name}_{INTERVAL}_{filename}.csv', index=False)
    
    return path
    

def send_message(file_path):
    
    """Sends Message via Telegram api.

    Parameters
    ----------

    file_path: Path of file """
    
    files = {'document': open(file_path, 'rb')}
    url = f'https://api.telegram.org/bot{os.getenv("API_KEY")}/sendDocument?chat_id={os.getenv("ID")}&caption={file_path}'
    requests.post(url, files=files)
    
    


def run_parallel():
    
    # list of ranges
    list_ranges = [i for i in coins]
  
    # pool object with number of elements in the list
    pool = Pool(processes=len(list_ranges))
  
    # map the function to the list and pass 
    # function and list_ranges as arguments
    pool.map(main, list_ranges)

# Driver code
if __name__ == '__main__':
    while True:
        run_parallel()
        
    
       