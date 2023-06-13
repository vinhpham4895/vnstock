# Copyright 2022 Thinh Vu @ GitHub
# See LICENSE for details.

# from .utils import *
import pandas as pd
import requests
import http.client
import json
from pandas import json_normalize
from io import BytesIO
import time
from datetime import datetime, timedelta

# API request config for SSI API endpoints
headers = {
    'Connection': 'keep-alive',
    'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
    'DNT': '1',
    'sec-ch-ua-mobile': '?0',
    'X-Fiin-Key': 'KEY',
    'Content-Type': 'application/json',
    'Accept': 'application/json',
    'X-Fiin-User-ID': 'ID',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
    'X-Fiin-Seed': 'SEED',
    'sec-ch-ua-platform': 'Windows',
    'Origin': 'https://iboard.ssi.com.vn',
    'Sec-Fetch-Site': 'same-site',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Dest': 'empty',
    'Referer': 'https://iboard.ssi.com.vn/',
    'Accept-Language': 'en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7'
}


def api_request(url, headers=headers):
    r = requests.get(url, headers).json()
    return r


def http_request(domain, api_endpoint, data={}, headers=headers):
    conn = http.client.HTTPSConnection(domain)
    conn.request("GET", api_endpoint, headers=headers)
    res = conn.getresponse()
    return res

# STOCK LISTING


def listing_companies():
    """
    Returns the list of all available stock symbols.
    """
    domain = 'fiin-core.ssi.com.vn'
    api_endpoint = '/Master/GetListOrganization?language=vi'
    res = http_request(domain, api_endpoint)
    r = json.loads(res.read().decode('utf-8'))
    df = pd.DataFrame(r['items']).drop(columns=['organCode', 'icbCode', 'organTypeCode', 'comTypeCode']).rename(
        columns={'comGroupCode': 'group_code', 'organName': 'company_name', 'organShortName': 'company_short_name'})
    return df


def ticker_overview(symbol):
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/ticker/{}/overview'.format(symbol)).json()
    df = json_normalize(data)
    return df

# STOCK TRADING HISTORICAL DATA


def stock_historical_data(symbol, start_date, end_date):
    """
    This function returns the stock historical daily data.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
        start_date (:obj:`str`, required): the start date to get data (YYYY-mm-dd).
        end_date (:obj:`str`, required): the end date to get data (YYYY-mm-dd).
    Returns:
        :obj:`pandas.DataFrame`:
        | tradingDate | open | high | low | close | volume |
        | ----------- | ---- | ---- | --- | ----- | ------ |
        | YYYY-mm-dd  | xxxx | xxxx | xxx | xxxxx | xxxxxx |

    Raises:
        ValueError: raised whenever any of the introduced arguments is not valid.
    """
    fd = int(time.mktime(time.strptime(start_date, "%Y-%m-%d")))
    td = int(time.mktime(time.strptime(end_date, "%Y-%m-%d")))
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={}&type=stock&resolution=D&from={}&to={}'.format(symbol, fd, td)).json()
    df = json_normalize(data['data'])
    df['tradingDate'] = pd.to_datetime(
        df.tradingDate.str.split("T", expand=True)[0])
    df.columns = df.columns.str.title()
    df.rename(columns={'Tradingdate': 'TradingDate'}, inplace=True)
    return df

# TCBS TRADING PRICE TABLE


def price_board(symbol_ls):
    """
    This function returns the trading price board of a target stocks list.
    Args:
        symbol_ls (:obj:`str`, required): STRING list of symbols separated by "," without any space. Ex: "TCB,SSI,BID"
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/second-tc-price?tickers={}'.format(symbol_ls)).json()
    df = json_normalize(data['data'])
    df = df[['t', 'cp', 'fv', 'mav', 'nstv', 'nstp', 'rsi', 'macdv', 'macdsignal', 'tsignal', 'avgsignal', 'ma20', 'ma50', 'ma100', 'session', 'mscore', 'pe', 'pb', 'roe', 'oscore', 'ev', 'mw3d', 'mw1m', 'mw3m', 'mw1y',
             'rs3d', 'rs1m', 'rs3m', 'rs1y', 'rsavg', 'hp1m', 'hp3m', 'hp1y', 'lp1m', 'lp3m', 'lp1y', 'hp1yp', 'lp1yp', 'delta1m', 'delta1y', 'bv', 'av', 'hmp', 'seq', 'vnid3d', 'vnid1m', 'vnid3m', 'vnid1y', 'vnipe', 'vnipb']]
    df = df.rename(columns={'t': 'Mã CP', 'cp': 'Giá Khớp Lệnh', 'fv': 'KLBD/TB5D', 'mav': 'T.độ GD', 'nstv': 'KLGD ròng(CM)', 'nstp': '%KLGD ròng (CM)', 'rsi': 'RSI', '': 'MACD Histogram', 'macdv': 'MACD Volume', 'macdsignal': 'MACD Signal', 'tsignal': 'Tín hiệu KT', 'avgsignal': 'Tín hiệu TB động', 'ma20': 'MA20', 'ma50': 'MA50', 'ma100': 'MA100', 'session': 'Phiên +/- ', 'mscore': 'Đ.góp VNI', 'pe': 'P/E', 'pb': 'P/B', 'roe': 'ROE', 'oscore': 'TCRating', 'ev': 'TCBS định giá',
                   'mw3d': '% thay đổi giá 3D', 'mw1m': '% thay đổi giá 1M', 'mw3m': '% thay đổi giá 3M', 'mw1y': '% thay đổi giá 1Y', 'rs3d': 'RS 3D', 'rs1m': 'RS 1M', 'rs3m': 'RS 3M', 'rs1y': 'RS 1Y', 'rsavg': 'RS TB', 'hp1m': 'Đỉnh 1M', 'hp3m': 'Đỉnh 3M', 'hp1y': 'Đỉnh 1Y', 'lp1m': 'Đáy 1M', 'lp3m': 'Đáy 3M', 'lp1y': 'Đáy 1Y', 'hp1yp': '%Đỉnh 1Y', 'lp1yp': '%Đáy 1Y', 'delta1m': '%Giá - %VNI (1M)', 'delta1y': '%Giá - %VNI (1Y)', 'bv': 'Khối lượng Dư mua', 'av': 'Khối lượng Dư bán', 'hmp': 'Khớp nhiều nhất'})
    return df


# TRADING INTELLIGENT
today_val = datetime.now()


def today():
    today = today_val.strftime('%Y-%m-%d')
    return today


def last_xd(day_num):  # return the date of last x days
    """
    This function returns the date that X days ago from today in the format of YYYY-MM-DD.
    Args:
        day_num (:obj:`int`, required): numer of days.
    Returns:
        :obj:`str`:
            2022-02-22
    Raises:
        ValueError: raised whenever any of the introduced arguments is not valid.
    """
    last_xd = (today_val - timedelta(day_num)).strftime('%Y-%m-%d')
    return last_xd


def start_xm(period):  # return the start date of x months
    """
    This function returns the start date of X months ago from today in the format of YYYY-MM-DD.
    Args:
        period (:obj:`int`, required): numer of months (period).
    Returns:
        :obj:`str`:
            2022-01-01
    Raises:
        ValueError: raised whenever any of the introduced arguments is not valid.
    """
    date = pd.date_range(end=today, periods=period+1,
                         freq='MS')[0].strftime('%Y-%m-%d')
    return date


def stock_intraday_data(symbol, page_num, page_size):
    """
    This function returns the stock realtime intraday data (or data of the last working day = Friday) as a pandas dataframe.
    Parameters: 
        symbol (str): The 3-digit name of the desired stock. Example: `TCB`. 
        page_num (int): The page index starting from 0. Example: 0. 
        page_size (int): The number of rows in a page to be returned by this query, maximum of 100.
    Returns: 
        df (DataFrame): A pandas dataframe containing the price, volume, time, percentage of changes, etc of the stock intraday data.
    Raises: 
        ValueError: If any of the arguments is not valid or the request fails. 
    """
    d = datetime.now()
    base_url = f'https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{symbol}/his/paging?page={page_num}&size={page_size}'
    print(base_url)
    if d.weekday() > 4:  # today is weekend
        data = requests.get(f'{base_url}&headIndex=-1').json()
    else:  # today is weekday
        data = requests.get(base_url).json()
    df = json_normalize(data['data']).rename(
        columns={'p': 'price', 'v': 'volume', 't': 'time'})
    return df

# COMPANY OVERVIEW


def company_overview(symbol):
    """
    This function returns the company overview of a target stock symbol
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/ticker/{}/overview'.format(symbol)).json()
    df = json_normalize(data)
    return df


# FINANCIAL REPORT
def financial_report(symbol, report_type, frequency, headers=headers):  # Quarterly, Yearly
    """
    This function returns the balance sheet of a stock symbol by a Quarterly or Yearly range.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
        report_type (:obj:`str`, required): BalanceSheet, IncomeStatement, CashFlow
        report_range (:obj:`str`, required): Yearly or Quarterly.
    """
    domain = 'fiin-fundamental.ssi.com.vn'
    api_endpoint = f'/FinancialStatement/Download{report_type}?language=vi&OrganCode={symbol}&Skip=0&Frequency={frequency}'
    r = http_request(domain, api_endpoint, headers=headers)
    df = pd.read_excel(BytesIO(r.read()), skiprows=7).dropna()
    return df


def financial_ratio_compare(symbol_ls, industry_comparison, frequency, start_year, headers=headers):
    """
    This function returns the balance sheet of a stock symbol by a Quarterly or Yearly range.
    Args:
        symbol (:obj:`str`, required): ["CTG", "TCB", "ACB"].
        industry_comparison (:obj: `str`, required): "true" or "false"
        report_range (:obj:`str`, required): Yearly or Quarterly.
    """
    global timeline
    if frequency == 'Yearly':
        timeline = str(start_year) + '_5'
    elif frequency == 'Quarterly':
        timeline = str(start_year) + '_4'

    domain = "fiin-fundamental.ssi.com.vn"

    for i in range(len(symbol_ls)):
        if i == 0:
            company_join = '&CompareToCompanies={}'.format(symbol_ls[i])
            api_endpoint = '/FinancialAnalysis/DownloadFinancialRatio2?language=vi&OrganCode={}&CompareToIndustry={}{}&Frequency={}&Ratios=ryd21&Ratios=ryd25&Ratios=ryd14&Ratios=ryd7&Ratios=rev&Ratios=isa22&Ratios=ryq44&Ratios=ryq14&Ratios=ryq12&Ratios=rtq51&Ratios=rtq50&Ratios=ryq48&Ratios=ryq47&Ratios=ryq45&Ratios=ryq46&Ratios=ryq54&Ratios=ryq55&Ratios=ryq56&Ratios=ryq57&Ratios=nob151&Ratios=casa&Ratios=ryq58&Ratios=ryq59&Ratios=ryq60&Ratios=ryq61&Ratios=ryd11&Ratios=ryd3&TimeLineFrom={}'.format(symbol_ls[
                i], industry_comparison, '', frequency, timeline)
        elif i > 0:
            company_join = '&'.join(
                [company_join, 'CompareToCompanies={}'.format(symbol_ls[i])])
            api_endpoint = '/FinancialAnalysis/DownloadFinancialRatio2?language=vi&OrganCode={}&CompareToIndustry={}{}&Frequency={}&Ratios=ryd21&Ratios=ryd25&Ratios=ryd14&Ratios=ryd7&Ratios=rev&Ratios=isa22&Ratios=ryq44&Ratios=ryq14&Ratios=ryq12&Ratios=rtq51&Ratios=rtq50&Ratios=ryq48&Ratios=ryq47&Ratios=ryq45&Ratios=ryq46&Ratios=ryq54&Ratios=ryq55&Ratios=ryq56&Ratios=ryq57&Ratios=nob151&Ratios=casa&Ratios=ryq58&Ratios=ryq59&Ratios=ryq60&Ratios=ryq61&Ratios=ryd11&Ratios=ryd3&TimeLineFrom=2017_5'.format(symbol_ls[
                i], industry_comparison, company_join, frequency, timeline)
    r = http_request(domain, api_endpoint, headers=headers)
    df = pd.read_excel(BytesIO(r.read()), skiprows=7)
    return df


# STOCK FILTERING

def financial_ratio(symbol, report_range, is_all):  # TCBS source
    """
    This function returns the quarterly financial ratios of a stock symbol. Some of expected ratios are: priceToEarning, priceToBook, roe, roa, bookValuePerShare, etc
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
        report_range (:obj:`str`, required): 'yearly' or 'quarterly'.
        is_all (:obj:`boo`, required): True or False
    """
    if report_range == 'yearly':
        x = 1
    elif report_range == 'quarterly':
        x = 0

    if is_all == True:
        y = 'true'
    else:
        y = 'false'

    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{}/financialratio?yearly={}&isAll={}'.format(symbol, x, y)).json()
    df = json_normalize(data)
    return df


# incomestatement, balancesheet, cashflow | report_range: 0 for quarterly, 1 for yearly
def financial_flow(symbol, report_type, report_range):
    """
    This function returns the quarterly financial ratios of a stock symbol. Some of expected ratios are: priceToEarning, priceToBook, roe, roa, bookValuePerShare, etc
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
        report_type (:obj:`str`, required): select one of 3 reports: incomestatement, balancesheet, cashflow.
        report_range (:obj:`str`, required): yearly or quarterly.
    """
    if report_range == 'yearly':
        x = 1
    elif report_range == 'quarterly':
        x = 0
    data = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{}/{}'.format(
        symbol, report_type), params={'yearly': x, 'isAll': 'true'}).json()
    df = json_normalize(data)
    df[['year', 'quarter']] = df[['year', 'quarter']].astype(str)
    df['index'] = df['year'].str.cat('-Q' + df['quarter'])
    df = df.set_index('index').drop(columns={'year', 'quarter'})
    return df


def dividend_history(symbol):
    """
    This function returns the dividend historical data of the seed stock symbol.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/company/{}/dividend-payment-histories?page=0&size=20'.format(symbol)).json()
    df = json_normalize(data['listDividendPaymentHis']
                        ).drop(columns=['no', 'ticker'])
    return df


# STOCK RATING
def general_rating(symbol):
    """
    This function returns a dataframe with all rating aspects for the desired stock symbol.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/general?fType=TICKER'.format(symbol)).json()
    df = json_normalize(data).drop(columns='stockRecommend')
    return df


def biz_model_rating(symbol):
    """
    This function returns the business model rating for the desired stock symbol.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/business-model?fType=TICKER'.format(symbol)).json()
    df = json_normalize(data)
    return df


def biz_operation_rating(symbol):
    """
    This function returns the business operation rating for the desired stock symbol.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/business-operation?fType=TICKER'.format(symbol)).json()
    df = json_normalize(data)
    return df


def financial_health_rating(symbol):
    """
    This function returns the financial health rating for the desired stock symbol.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/financial-health?fType=TICKER'.format(symbol)).json()
    df = json_normalize(data)
    return df


def valuation_rating(symbol):
    """
    This function returns the valuation rating for the desired stock symbol.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/valuation?fType=TICKER'.format(symbol)).json()
    df = json_normalize(data)
    return df


def industry_financial_health(symbol):
    """
    This function returns the industry financial health rating for the seed stock symbol.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/financial-health?fType=INDUSTRY'.format(symbol)).json()
    df = json_normalize(data)
    return df

# STOCK COMPARISON


def industry_analysis(symbol):
    """
    This function returns an overview of rating for companies at the same industry with the desired stock symbol.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/detail/council?tickers={}&fType=INDUSTRIES'.format(symbol)).json()
    df = json_normalize(data)
    data1 = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/detail/single?ticker={}&fType=TICKER'.format(symbol)).json()
    df1 = json_normalize(data1)
    df = pd.concat([df1, df]).reset_index(drop=True)
    return df


def stock_ls_analysis(symbol_ls):
    """
    This function returns an overview of rating for a list of companies by entering list of stock symbols.
    Args:
        symbol (:obj:`str`, required): 3 digits name of the desired stock.
    """
    data = requests.get(
        'https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/detail/council?tickers={}&fType=TICKERS'.format(symbol_ls)).json()
    df = json_normalize(data).dropna(axis=1)
    return df

# MARKET WATCH


# Value, Losers, Gainers, Volume, ForeignTrading, NewLow, Breakout, NewHigh
def market_top_mover(report_name):
    """
    This function returns the list of Top Stocks by one of criteria: 'Value', 'Losers', 'Gainers', 'Volume', 'ForeignTrading', 'NewLow', 'Breakout', 'NewHigh'.
    Args:
        report_name(:obj:`str`, required): name of the report
    """
    domain = 'fiin-market.ssi.com.vn'
    ls1 = ['Gainers', 'Losers', 'Value', 'Volume']
    # ls2 = ['ForeignTrading', 'NewLow', 'Breakout', 'NewHigh']
    if report_name in ls1:
        api_endpoint = f'/TopMover/GetTop{report_name}?language=vi&ComGroupCode=All'
    elif report_name == 'ForeignTrading':
        api_endpoint = '/TopMover/GetTopForeignTrading?language=vi&ComGroupCode=All&Option=NetBuyVol'
    elif report_name == 'NewLow':
        api_endpoint = '/TopMover/GetTopNewLow?language=vi&ComGroupCode=All&TimeRange=ThreeMonths'
    elif report_name == 'Breakout':
        api_endpoint = '/TopMover/GetTopBreakout?language=vi&ComGroupCode=All&TimeRange=OneWeek&Rate=OnePointFive'
    elif report_name == 'NewHigh':
        api_endpoint = '/TopMover/GetTopNewHigh?language=vi&ComGroupCode=All&TimeRange=ThreeMonths'
    response = http_request(domain, api_endpoint)
    r = json.loads(response.read())
    df = pd.DataFrame(r['items'])
    return df


def fr_trade_heatmap(exchange, report_type):
    """
    This function returns the foreign investors trading insights which is being rendered as the heatmap on SSI iBoard
    Args:
        exchange (:obj:`str`, required): Choose All, HOSE, HNX, or UPCOM.
        report_type (:obj:`str`, required): choose one of these report types: FrBuyVal, FrSellVal, FrBuyVol, FrSellVol, Volume, Value, MarketCap
    """
    domain = 'fiin-market.ssi.com.vn'
    api_endpoint = f'/HeatMap/GetHeatMap?language=vi&Exchange={exchange}&Criteria={report_type}'
    response = http_request(domain, api_endpoint)
    r = json.loads(response.read())
    concat_ls = []
    for i in range(len(r['items'])):
        for j in range(len(r['items'][i]['sectors'])):
            name = r['items'][i]['sectors'][j]['name']
            rate = r['items'][i]['sectors'][j]['rate']
            df = json_normalize(r['items'][i]['sectors'][j]['tickers'])
            df['industry_name'] = name
            df['rate'] = rate
            concat_ls.append(df)
    combine_df = pd.concat(concat_ls)
    return combine_df

# GET MARKET IN DEPT DATA - INDEX SERIES


def get_index_series(index_code='VNINDEX', time_range='OneYear', headers=headers):
    """
    Retrieve the Stock market index series, maximum in 5 years
    Args:
        index_code (:obj:`str`, required): Use one of the following code'VNINDEX', 'VN30', 'HNXIndex', 'HNX30', 'UpcomIndex', 'VNXALL',
                                        'VN100','VNALL', 'VNCOND', 'VNCONS','VNDIAMOND', 'VNENE', 'VNFIN',
                                        'VNFINLEAD', 'VNFINSELECT', 'VNHEAL', 'VNIND', 'VNIT', 'VNMAT', 'VNMID',
                                        'VNREAL', 'VNSI', 'VNSML', 'VNUTI', 'VNX50'. You can get the complete list of the latest indices from `get_latest_indices()` function
        time_range (:obj: `str`, required): Use one of the following values 'OneDay', 'OneWeek', 'OneMonth', 'ThreeMonth', 'SixMonths', 'YearToDate', 'OneYear', 'ThreeYears', 'FiveYears'
    """
    domain = 'fiin-market.ssi.com.vn'
    api_endpoint = f'/MarketInDepth/GetIndexSeries?language=vi&ComGroupCode={index_code}&TimeRange={time_range}&id=1'
    payload = {}
    response = http_request(domain, api_endpoint,
                            data=payload, headers=headers)
    result = json_normalize(json.loads(response.read())['items'])
    return result


def get_latest_indices(headers=headers):
    """
    Retrieve the latest indices values
    """
    domain = 'fiin-market.ssi.com.vn'
    api_endpoint = '/MarketInDepth/GetLatestIndices?language=vi&pageSize=999999&status=1'
    response = http_request(domain, api_endpoint, headers=headers)
    result = json_normalize(json.loads(response.read())['items'])
    return result
