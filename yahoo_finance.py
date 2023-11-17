import yfinance as yf
import requests
import pandas as pd
import concurrent.futures
import utils
from io import BytesIO

def get_tickers_data_stacked(tickers, period="max", interval="1d"):
    data = yf.download(
        tickers=tickers,
        period=period,
        interval=interval,
        group_by="ticker",
        auto_adjust=True,
        prepost=True,
        threads=True,
        proxy=None,
    )
    return data.stack(level=0).rename_axis(["Date", "Ticker"]).reset_index(level=1)


def get_tickers_data(tickers, period="max", interval="1d"):
    bar, c = utils.progress(tickers, message=" -> Downloading tickers"), 0
    x = lambda ticker: get_data(ticker, period=period, interval=interval)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(x, ticker) for ticker in tickers]
        for _ in concurrent.futures.as_completed(results):
            c += 1
            bar.update(c)
        return [dataset.result() for dataset in results]


def get_data(ticker_name, period="max", interval="1d"):
    ticker = yf.Ticker(ticker_name)

    data = ticker.history(
        period=period, interval=interval, auto_adjust=True, debug=True
    )
    data["Ticker"] = ticker.ticker
    data["Interval"] = interval

    return data[["Ticker", "Interval", "Close", "High", "Low", "Open", "Volume"]]


def ticker_exists(ticker):
    """Check if constituent exists in yfinance API"""
    ticker = yf.Ticker(ticker)
    if ticker.history(debug=False).empty is True:
        return False
    if ticker.info is None and ticker.info == {}:
        return False

    return ticker.ticker


def get_info(ticker_name):
    info = yf.Ticker(ticker_name).info
    return {
        "currency": info.get("currency"),
        "quote_type": info.get("quoteType"),
    }


def get_b3_stock_constituents():
    """Get B3 stock constituents only"""
    req = requests.get(
        "https://www.fundamentus.com.br/resultado.php",
        headers={"User-Agent": "Mozilla/5.0"},
    )

    if req.status_code != 200:
        raise Exception("Error getting B3 stock constituents")

    return set(map(lambda x: x + ".SA", pd.read_html(req.text)[0]["Papel"].to_list()))


def get_sp500_stock_constituents():
    req = requests.get(
        "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv",
        headers={"User-Agent": "Mozilla/5.0"},
    )

    if req.status_code != 200:
        raise Exception("Error getting S&P500 stock constituents")

    return set(pd.read_csv(BytesIO(req.content))['Symbol'].tolist())


def get_crypto_constituents_binance():
    def get_ticker_from_string(string):
        if type(string) == str:
            return string.split(" ")[2].replace("USDT", "USD").replace("/", "-")

    constituents = []
    c = 1
    while True:
        req = requests.get(
            "https://coinranking.com/exchange/-zdvbieRdZ%2Bbinance/markets?search=usdt"
            + f"&page={c}",
            headers={"User-Agent": "Mozilla/5.0"},
        )

        if req.status_code != 200:
            raise Exception("Error getting Binance USDT constituents")

        data = pd.read_html(req.text)[0]["Markets"]
        if len(data) <= 1:
            break
        else:
            constituents += [
                get_ticker_from_string(string)
                for string in list(data)
                if type(string) == str
            ]
            c += 1

    return set(constituents)


def get_usa_market_constituents(exchange="all"):
    """Get USA market constituents by exchange. Avaliable exchanges: NASDAQ, NYSE and AMEX"""
    base_url = "https://api.nasdaq.com/api/screener/stocks?tableonly=true&limit=25&download=true"
    url = base_url if exchange == "all" else base_url + "&exchange=" + exchange

    req = requests.get(url, headers={"User-Agent": "Mozilla/5.0"})

    if req.status_code != 200:
        raise Exception("Error getting USA market constituents")

    return set(pd.DataFrame(req.json()["data"]["rows"])["symbol"].tolist())


def get_crypto_constituents():
    """Get crypto constituents"""
    crypto_constituents = []
    for i in range(0, 5000, 25):
        req = requests.get(
            "https://finance.yahoo.com/cryptocurrencies/"
            + f"?_device=desktop&count=25&device=desktop&failsafe=1&offset={i}&ynet=0",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        crypto_constituents += pd.read_html(req.text)[0]["Symbol"].to_list()
    return set(crypto_constituents)


def check_yfinance(constituents):
    """Check if constituents exist in yfinance API"""
    tickers = []
    bar, c = utils.progress(constituents), 0

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = [executor.submit(ticker_exists, ticker) for ticker in constituents]
        for result in concurrent.futures.as_completed(results):
            res = result.result()
            if res:
                tickers.append(res)
            c += 1
            bar.update(c)
    return tickers
