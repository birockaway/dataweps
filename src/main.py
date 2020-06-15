# get info on material we have on a product card / exists in sortiment rep
import concurrent.futures
import csv
import datetime as dt
import logging
import os
import queue
import threading
from datetime import datetime
from datetime import timedelta
from time import sleep

import pandas as pd
import pytz
import requests
from logstash_formatter import LogstashFormatterV1
from keboola import docker
from extractors_writer import Writer


class Producer(object):
    def __init__(self, datadir, pipeline):
        self.datadir = datadir
        cfg = docker.Config(self.datadir)
        params = cfg.get_parameters()
        self.user = params.get('#user')
        self.password = params.get('#password')
        self.url = params.get('url')
        self.bulk_size = params.get('bulk_size') or 500
        self.out_cols = params.get('out_cols')
        self.source = params.get('source')
        new_timestamp = datetime.now()
        self.ts = new_timestamp.strftime('%Y-%m-%d %H:%M:%S')
        self.source_id = new_timestamp.strftime('%Y%m%d%H%M%S')
        self.task_queue = pipeline

    # parser
    def parse(self, i):
        product = dict()
        product['TS'] = self.ts
        product['COUNTRY'] = 'CZ'
        product['DISTRCHAN'] = 'CZC'
        product['MATERIAL'] = i['internal_code']
        # product['cse_name'] = i['name']
        product['ESHOP'] = i['shop']
        product['SOURCE'] = self.source
        product['FREQ'] = 'd'
        product['URL'] = i['url']
        product['STOCK'] = 1 if i['stock'] is True else 0
        product['PRICE'] = i['price_vat']
        product['SOURCE_ID'] = self.source_id
        if 'availability' in i:
            # prefer global/eshop availability to regional
            availability = i['availability'].get('global') or i['availability'].get('eshop')
            # as a last resort, take first regions availability
            if availability is None and i['availability'].values():
                availability = list(i['availability'].values())[0]
            product['AVAILABILITY'] = availability

        return product

    def produce(self):
        url = self.url
        user = self.user
        pasw = self.password
        out_cols = self.out_cols

        # params
        per_page = self.bulk_size
        time_from = (dt.datetime.now(pytz.timezone('Europe/Prague')) + timedelta(hours=-7)).strftime(
            '%Y-%m-%dT%H:%M:%S+01:00')
        time_to = (dt.datetime.now(pytz.timezone('Europe/Prague')) + timedelta(hours=1)).strftime(
            '%Y-%m-%dT%H:%M:%S+01:00')

        payload = {'time_from': time_from, 'time_to': time_to, 'per_page': per_page}

        # get and parse response
        last_page = False
        lst = []

        scroll_limit = 0
        try:
            while last_page is False:
                r = requests.get(url, auth=(user, pasw), params=payload)
                resp = r.json()

                if resp.get('code') == 429:
                    # scroll request limit reached, try waiting and rerun last url request
                    scroll_limit += 1
                    if scroll_limit >= 25:
                        raise Exception('Scroll limit reached repeatedly')

                    sleep(1)
                    continue

                for i in resp['results']:
                    lst.append(self.parse(i))

                df = pd.DataFrame(lst, columns=out_cols)
                df['STOCK'] = df['STOCK'].fillna(0).astype(int)
                df.fillna('', inplace=True)
                # output to queue for writing
                self.task_queue.put(df.to_dict('records'))
                if resp['pagination']['last_page'] is False:
                    url = resp['pagination']['next_page']
                else:
                    last_page = True
        except Exception as e:
            logging.error(f'Error occured {str(e)}')
        finally:
            # let writer know extracting is finished
            self.task_queue.put('DONE')


if __name__ == '__main__':
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = LogstashFormatterV1()

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level=logging.INFO)
    colnames = ['AVAILABILITY',
                'COUNTRY',
                'CSE_ID',
                'CSE_URL',
                'DISTRCHAN',
                'ESHOP',
                'FREQ',
                'HIGHLIGHTED_POSITION',
                'MATERIAL',
                'POSITION',
                'PRICE',
                'RATING',
                'REVIEW_COUNT',
                'SOURCE',
                'SOURCE_ID',
                'STOCK',
                'TOP',
                'TS',
                'URL']

    datadir = os.getenv('KBC_DATADIR', '/data/')
    path = f'{os.getenv("KBC_DATADIR")}out/tables/results.csv'
    pipeline = queue.Queue(maxsize=1000)
    event = threading.Event()
    producer = Producer(datadir, pipeline)
    writer = Writer(pipeline, colnames, path)
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.submit(producer.produce)
        executor.submit(writer)
