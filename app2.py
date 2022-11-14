from flask import Flask, render_template, Response
from flask_sqlalchemy import SQLAlchemy
from celery import Celery
from datetime import datetime
from binance.client import Client
from binance.enums import *
import time
from waiting import wait
import requests
import json
from concurrent.futures import ThreadPoolExecutor
import pytz
import os
import threading
# import redis# Connect to a local redis instance
# r = redis.Redis(host = 'localhost', port = 6379, db = 0)
app = Flask(__name__)

# def make_celery(app):
#     celery = Celery(
#         app.import_name,
#         backend=app.config['CELERY_RESULT_BACKEND'],
#         broker=app.config['CELERY_BROKER_URL']
#     )
#     # celery.conf.update(app.config)

#     class ContextTask(celery.Task):
#         def __call__(self, *args, **kwargs):
#             with app.app_context():
#                 return self.run(*args, **kwargs)

#     celery.Task = ContextTask
#     return celery

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
# app.config['CELERY_BROKER_URL']= 'amqps://owdjdkgy:HZ246WsJZdN9NpxW87q2yIMwp7CtEwLU@gerbil.rmq.cloudamqp.com/owdjdkgy'
# app.config['CELERY_RESULT_BACKEND']='db+sqlite:///data.db'
# os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')
db = SQLAlchemy(app)
# celery = make_celery(app)

api_key = "your-api-key"
api_secret = "your-api-secret"

tickers_spot = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'NEOUSDT', 'LTCUSDT', 'QTUMUSDT', 'ADAUSDT', 'XRPUSDT', 'EOSUSDT', 'TUSDUSDT',
                'IOTAUSDT', 'XLMUSDT', 'ONTUSDT', 'TRXUSDT', 'ETCUSDT', 'ICXUSDT', 'NULSUSDT', 'VETUSDT', 'LINKUSDT', 'WAVESUSDT',
                'BTTUSDT', 'ONGUSDT', 'HOTUSDT', 'ZILUSDT', 'ZRXUSDT', 'FETUSDT', 'BATUSDT', 'XMRUSDT', 'ZECUSDT',
                'IOSTUSDT', 'CELRUSDT', 'DASHUSDT', 'NANOUSDT', 'OMGUSDT', 'THETAUSDT', 'ENJUSDT', 'MITHUSDT', 'MATICUSDT',
                'ATOMUSDT', 'TFUELUSDT', 'ONEUSDT', 'FTMUSDT', 'ALGOUSDT', 'GTOUSDT', 'DOGEUSDT',
                'DUSKUSDT', 'ANKRUSDT', 'WINUSDT', 'COSUSDT', 'COCOSUSDT', 'MTLUSDT', 'TOMOUSDT', 'PERLUSDT',
                'DENTUSDT', 'MFTUSDT', 'KEYUSDT', 'DOCKUSDT', 'WANUSDT', 'FUNUSDT', 'CVCUSDT', 'CHZUSDT',
                'BANDUSDT', 'BEAMUSDT', 'XTZUSDT', 'RENUSDT', 'RVNUSDT', 'HBARUSDT', 'NKNUSDT',
                'STXUSDT', 'KAVAUSDT', 'ARPAUSDT', 'IOTXUSDT', 'RLCUSDT', 'CTXCUSDT', 'BCHUSDT', 'TROYUSDT',
                'VITEUSDT', 'FTTUSDT', 'EURUSDT', 'OGNUSDT', 'DREPUSDT', 'TCTUSDT', 'WRXUSDT',
                'BTSUSDT', 'LSKUSDT', 'BNTUSDT', 'LTOUSDT', 'AIONUSDT', 'MBLUSDT',
                'COTIUSDT', 'STPTUSDT', 'WTCUSDT', 'DATAUSDT', 'SOLUSDT',
                'CTSIUSDT', 'HIVEUSDT', 'CHRUSDT', 'GXSUSDT', 'ARDRUSDT', 'MDTUSDT',
                'STMXUSDT', 'KNCUSDT', 'LRCUSDT', 'PNTUSDT', 'COMPUSDT', 'SCUSDT',
                'ZENUSDT', 'SNXUSDT', 'VTHOUSDT', 'DGBUSDT', 'GBPUSDT', 'SXPUSDT', 'MKRUSDT', 'DCRUSDT',
                'STORJUSDT', 'MANAUSDT', 'AUDUSDT', 'YFIUSDT', 'BALUSDT', 'BLZUSDT',
                'IRISUSDT', 'KMDUSDT', 'JSTUSDT', 'SRMUSDT', 'ANTUSDT', 'CRVUSDT', 'SANDUSDT', 'OCEANUSDT', 'NMRUSDT',
                'DOTUSDT', 'LUNAUSDT', 'RSRUSDT', 'PAXGUSDT', 'WNXMUSDT', 'TRBUSDT', 'BZRXUSDT', 'SUSHIUSDT', 'YFIIUSDT',
                'KSMUSDT', 'EGLDUSDT', 'DIAUSDT', 'RUNEUSDT', 'FIOUSDT', 'UMAUSDT', 'BELUSDT', 'WINGUSDT', 'UNIUSDT', 'NBSUSDT',
                'OXTUSDT', 'SUNUSDT', 'AVAXUSDT', 'HNTUSDT', 'FLMUSDT', 'ORNUSDT', 'UTKUSDT', 'XVSUSDT', 'ALPHAUSDT',
                'AAVEUSDT', 'NEARUSDT', 'FILUSDT', 'INJUSDT', 'AUDIOUSDT', 'CTKUSDT', 'AKROUSDT', 'AXSUSDT', 'HARDUSDT', 'DNTUSDT',
                'STRAXUSDT', 'UNFIUSDT', 'ROSEUSDT', 'AVAUSDT', 'XEMUSDT', 'SKLUSDT', 'SUSDUSDT', 'GRTUSDT', 'JUVUSDT', 'PSGUSDT',
                '1INCHUSDT', 'REEFUSDT', 'OGUSDT', 'ATMUSDT', 'ASRUSDT', 'CELOUSDT', 'RIFUSDT', 'BTCSTUSDT', 'TRUUSDT', 'CKBUSDT',
                'LITUSDT', 'SFPUSDT', 'DODOUSDT', 'CAKEUSDT', 'ACMUSDT', 'BADGERUSDT', 'FISUSDT', 'OMUSDT', 'PONDUSDT', 'DEGOUSDT',
                'ALICEUSDT', 'LINAUSDT', 'PERPUSDT', 'RAMPUSDT', 'SUPERUSDT', 'CFXUSDT', 'EPSUSDT', 'AUTOUSDT', 'TKOUSDT',
                'PUNDIXUSDT', 'TLMUSDT', 'BTGUSDT', 'MIRUSDT', 'BARUSDT', 'FORTHUSDT', 'BAKEUSDT', 'TWTUSDT', 'FIROUSDT',
                'BURGERUSDT', 'SLPUSDT', 'SHIBUSDT', 'ICPUSDT', 'ARUSDT', 'POLSUSDT', 'MDXUSDT', 'MASKUSDT', 'LPTUSDT',
                'NUUSDT', 'XVGUSDT', 'ATAUSDT', 'GTCUSDT', 'TORNUSDT', 'KEEPUSDT', 'ERNUSDT', 'KLAYUSDT', 'PHAUSDT', 'BONDUSDT',
                'MLNUSDT', 'DEXEUSDT', 'C98USDT', 'CLVUSDT', 'QNTUSDT', 'FLOWUSDT', 'TVKUSDT', 'MINAUSDT', 'RAYUSDT', 'FARMUSDT',
                'ALPACAUSDT', 'QUICKUSDT', 'MBOXUSDT', 'FORUSDT', 'REQUSDT', 'GHSTUSDT', 'WAXPUSDT', 'TRIBEUSDT', 'GNOUSDT',
                'XECUSDT', 'ELFUSDT', 'DYDXUSDT', 'POLYUSDT', 'IDEXUSDT', 'VIDTUSDT', 'GALAUSDT',
                'ILVUSDT', 'YGGUSDT',  'SYSUSDT', 'DFUSDT', 'FIDAUSDT', 'FRONTUSDT', 'CVPUSDT', 'AGLDUSDT', 'RADUSDT', 'BETAUSDT', 'RAREUSDT']


class Symbol(db.Model):
    sno = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(15), nullable=False)
    avg_vol = db.Column(db.String(40), nullable=False)
    latest_vol = db.Column(db.String(40), nullable=False)
    date_time = db.Column(db.DateTime, default=datetime.now())

    def __repr__(self) -> str:
        return f'{self.symbol}'

def sma(series, timeperiod):
    sum = 0
    for i in range(timeperiod):
        sum = sum + float(series[i])
    return sum / timeperiod

def pushbullet_noti(title, body):
    TOKEN = "o.x3Rw2TCGlPTRgt8sctbARyqFRqvPMFaW"
    # TOKEN = 'Your Access Token' # Pass your Access Token here
    # Make a dictionary that includes, title and body
    msg = {"type": "note", "title": title,
           "body": body}

    resp = requests.post('https://api.pushbullet.com/v2/pushes',
                         data=json.dumps(msg),
                         headers={'Authorization': 'Bearer ' + TOKEN,
                                  'Content-Type': 'application/json'})
    if resp.status_code != 200:  # Check if fort message send with the help of status code
        raise Exception('Error', resp.status_code)
    else:
        # print('Message sent')
        pass

def is_something_ready():
    if  (int(str(datetime.now())[14:16]) % 5) == 0:
    #  int(str(datetime.now())[17:19])%59 == 0:
   
        return True
    else:
        return False

def newscan():
    pairs=[]
		
    def breakoutcheck(symbol):
        volume = []
        k_lines = client.get_historical_klines(
			symbol, Client.KLINE_INTERVAL_5MINUTE, "1 day ago UTC")
        time.sleep(4)
        if len(k_lines) == 0:
            k_lines = client.get_historical_klines(
                symbol, Client.KLINE_INTERVAL_5MINUTE, "1 day ago UTC")
            time.sleep(10)
        i = 0
        while i < 30:
            volume.append(k_lines[-i-1][5])
            i = i+1
        # print(" volume list :   ", volume[::-1])
        avg_volume = sma(volume[::-1], timeperiod=20)
        vol = volume[::-1]
        # print(avg_volume)
        if float(vol[-1]) > (10.0*avg_volume):
            pairs.append(Symbol(symbol,str(avg_volume),str(vol[-1]),datetime.now()))
            print(symbol, "  avg_volume : ", avg_volume,'  last candle volume: ', vol[-1])
            return Symbol(symbol,str(avg_volume),str(vol[-1]),datetime.now())
        else:
            return 0

    with ThreadPoolExecutor(max_workers=100) as exe:
        exe.map(breakoutcheck, tickers_spot)

    print(f"scan done at  {datetime.now(pytz.timezone('Asia/Kolkata'))}")

    if len(pairs)!=0: 
        pushbullet_noti("5m breakout", str(pairs))
        # print(pairs)
        return (pairs)
    if len(pairs)==0:
        # print('Nothing found')
        return 0

def time_loop():
    while 1:
        wait(lambda: is_something_ready(), timeout_seconds=3700,waiting_for="current candle to close")
        time.sleep(0.5)
        sym=newscan()
        # s1=Symbol(symbol='BTCUSDT',avg_vol='500',latest_vol='1000',date_time=datetime.now())
        # sym = [s1]
        if sym != 0:
            for i in sym :
               db.session.add(i)
            db.session.commit()
            pushbullet_noti("5m breakout", str(sym))
            print(sym)
        else: print('Nothing found')
        time.sleep(50)

@app.route('/')
def hello_world():
    # sym = Symbol(symbol='ETHUSDT', avg_vol='586',
    #              latest_vol='70', date_time=datetime.now())
    # db.session.add(sym)
    # db.session.commit()
    allsym = Symbol.query.all()
    return render_template("index.html", allsym=allsym)


if __name__ == '__main__':
    try:
        client = Client(api_key, api_secret)
        print('connection established with binance api')
    except:
        print("Please check your internet connection!")
    threading.Thread(target=time_loop).start()
    print('time loop thread started')
    app.run(debug=True, port=5000)
