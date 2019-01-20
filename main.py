import os
import functools
from multiprocessing import Pool, cpu_count

import arrow
import googlemaps
import pandas as pd
from tqdm import tqdm
from sqlobject import *

import helper


NUM_THREADS = 2 * cpu_count()
SAT_11AM = arrow.now('Asia/Singapore').shift(weekday=5).replace(hour=11, minute=0, second=0).timestamp
MON_9AM = arrow.now('Asia/Singapore').shift(weekday=0).replace(hour=9, minute=0, second=0).timestamp
WORK_LOCATION = 'GOOGLE SINGAPORE'

### SQL Object Setup ###
db_filename = os.path.abspath('timings.sqlite')
connection_string = 'sqlite:' + db_filename
connection = connectionForURI(connection_string)
sqlhub.processConnection = connection


class Address(SQLObject):
    location = StringCol(notNone=True, unique=True)
    mrt = StringCol(notNone=True)
    min_walk_to_mrt = IntCol(notNone=True)
    min_to_work = IntCol(notNone=True)
Address.createTable(ifNotExists=True)
#######################


def _populate_single_record(address, gmaps):
    if Address.selectBy(location=address).count():
        return
    mrt, mrt_geo = helper.getNearestMRT(address)
    if mrt == 'NIL':
        return
    try:
        r = gmaps.distance_matrix(address, mrt_geo, mode='walking', departure_time=SAT_11AM,
                                  region='SG', units='metric')
        min_walk_to_mrt = r['rows'][0]['elements'][0]['duration']['value'] // 60
        r = gmaps.distance_matrix(address, WORK_LOCATION, mode='transit', arrival_time=MON_9AM,
                                  region='SG', units='metric')
        min_to_work = r['rows'][0]['elements'][0]['duration']['value'] // 60
    except KeyError:
        # Google maps could not find anything
        return

    Address(location=address, mrt=mrt, min_walk_to_mrt=min_walk_to_mrt, min_to_work=min_to_work)


def populate_timings_db():

    with open('.google_api_key', 'r') as f:
        gmaps = googlemaps.Client(f.read().strip())

    df = pd.read_csv('hdb_listings.csv')
    addresses = df['listing-location'].dropna().str.upper().unique()

    with Pool(NUM_THREADS) as p:
        list(tqdm(p.imap(functools.partial(_populate_single_record, gmaps=gmaps), addresses), total=len(addresses)))


if __name__ == '__main__':
    populate_timings_db()
