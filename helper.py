import urllib
from typing import Tuple

import backoff
import requests
import numpy as np
import pandas as pd
import scipy.spatial


@backoff.on_exception(backoff.fibo,
                      requests.exceptions.RequestException,
                      max_tries=8,
                      jitter=backoff.full_jitter(10))
def getLatLon(address: str) -> Tuple[float, float]:
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


def getNearestMRT(address: str) -> Tuple[str, Tuple[float, float]]:
    """Returns name of nearest MRT and its lat/lon."""
    address_geo = getLatLon(address)
    if address_geo == (0, 0):
        return 'NIL', address_geo
    address_geo = np.array([address_geo])
    i = np.argmin(scipy.spatial.distance.cdist(address_geo, getNearestMRT.stations_geo)[0])
    result = getNearestMRT.stations_pdf.iloc[i]
    return result['location'], tuple(result[['latitude', 'longitude']])
getNearestMRT.stations_pdf = pd.read_csv('mrt_stations.csv')
getNearestMRT.stations_geo = getNearestMRT.stations_pdf[['latitude', 'longitude']].values
