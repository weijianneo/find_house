import time
import urllib
from typing import Tuple

import backoff
import requests
import xmltodict
import numpy as np
import pandas as pd
import scipy.spatial


HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'}


def _current_ms() -> int:
    return int(time.time() * 1000)


@backoff.on_exception(backoff.fibo,
                      requests.exceptions.RequestException,
                      max_tries=8,
                      jitter=backoff.full_jitter(10))
def get_lat_lon(address: str) -> Tuple[float, float]:
    address = urllib.parse.quote_plus(address)
    url = f'https://developers.onemap.sg/commonapi/search?searchVal={address}&returnGeom=Y&getAddrDetails=Y'
    r = requests.get(url)
    r.raise_for_status()
    j = r.json()
    j = list(filter(lambda x: x['POSTAL'] != 'NIL', j['results']))
    lat = 0
    lon = 0
    for x in j:
        lat += float(x['LATITUDE'])
        lon += float(x['LONGITUDE'])
    c = len(j)
    return (lat/c, lon/c) if c else (0, 0)


def get_nearst_mrt(address: str) -> Tuple[str, Tuple[float, float]]:
    """Returns name of nearest MRT and its lat/lon."""
    address_geo = get_lat_lon(address)
    if address_geo == (0, 0):
        return 'NIL', address_geo
    address_geo = np.array([address_geo])
    i = np.argmin(scipy.spatial.distance.cdist(address_geo, get_nearst_mrt.stations_geo)[0])
    result = get_nearst_mrt.stations_pdf.iloc[i]
    return result['location'], tuple(result[['latitude', 'longitude']])
get_nearst_mrt.stations_pdf = pd.read_csv('mrt_stations.csv')
get_nearst_mrt.stations_geo = get_nearst_mrt.stations_pdf[['latitude', 'longitude']].values


@backoff.on_exception(backoff.fibo,
                      requests.exceptions.RequestException,
                      max_tries=8,
                      jitter=backoff.full_jitter(10))
def get_postal(address: str) -> str:
    """Returns postal code of address or NIL if not found."""
    address = urllib.parse.quote_plus(address)
    url = f'https://developers.onemap.sg/commonapi/search?searchVal={address}&returnGeom=N&getAddrDetails=Y'
    r = requests.get(url)
    r.raise_for_status()
    j = r.json()
    for x in j['results']:
        if x['POSTAL'] != 'NIL':
            return x['POSTAL']
    return 'NIL'


@backoff.on_exception(backoff.fibo,
                      requests.exceptions.RequestException,
                      max_tries=8,
                      jitter=backoff.full_jitter(10))
def get_lease_remaining_years(address: str) -> int:
    postal = get_postal(address)
    if postal == 'NIL':
        return 0
    r = requests.get(
        f'https://services2.hdb.gov.sg/webapp/BB14ALeaseInfo/BB14SGenerateLeaseInfoXML?postalCode={postal}&_={_current_ms()}',
        headers=HEADERS)
    r.raise_for_status()
    try:
        d = xmltodict.parse(r.text)
        return int(d['LeaseInformation']['LeaseRemaining'])
    except KeyError:
        # HDB service did not return remaining lease
        return 0
