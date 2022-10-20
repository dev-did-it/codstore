# this python module searches for cod store bundles and captures info about them into a .csv
import logging
import os
import concurrent.futures
import requests as rq
import pandas as pd
import json
from bs4 import BeautifulSoup
from tqdm import tqdm


# write dataframe to .csv
def write_data(titles, urls, skus, costs, file_name):
    try:
        # define dict for bundle data
        bundle_dict = {'title': titles, 'url': urls, 'sku': skus, 'cost': costs}

        # write dataframe to .csv
        bundle_df = pd.DataFrame.from_dict(data=bundle_dict)

        # if file does not exist write header
        if not os.path.exists(file_name):
            bundle_df.to_csv(file_name, index=False)
        else:  # else it exists so append without writing the header
            bundle_df.to_csv(path_or_buf=file_name, mode='a', index=False, header=False)
    except Exception as ex:
        logging.exception('Exception occurred in write_data(): %s' % ex)


# capture bundle cost from alternate url
def get_bundle_cost(sku, game, headers):
    try:
        # build url with sku and game code
        url = 'https://my.callofduty.com/api/papi-client/inventory/v1/title/{game}/bundle/{sku}/en'.format(sku=sku, game=game)

        # perform HTTP GET request
        with rq.get(url=url, headers=headers, timeout=1, allow_redirects=False) as req:
            req_content = req.content

        # parse html response content
        soup = BeautifulSoup(req_content, 'html.parser')

        # parse json response
        site_json = json.loads(soup.text)

        # determine if bundle status is valid
        if site_json['status'] == 'success':
            # capture bundle cost
            bundle_cost = site_json['data']['cost']
            return bundle_cost
        else:
            return None
    except Exception as ex:
        logging.exception('Exception occurred in get_bundle_cost(): %s' % ex)


# backfill cost for existing bundles
def backfill_cost(game):
    try:
        file_name = 'C:/repos/codstore/data/bundles_{game}_test.csv'.format(game=game)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
        }
        cost_dict = {}
        cost_list = []

        # read bundle .csv
        df = pd.read_csv(filepath_or_buffer=file_name)

        # determine if cost column exists in dataframe
        if 'cost' not in df.columns:
            # create cost column
            df['cost'] = None

        # exclude skus with an existing cost
        skus = df['sku'].where(df['cost'].isna())
        # todo: uncomment below if you want to overwrite cost for all skus
        # skus = df['sku']

        for sku in skus:
            # invoke function to get bundle cost for each sku
            bundle_cost = get_bundle_cost(sku, game, headers)
            # append bundle cost to cost list
            cost_list.append(bundle_cost)

        # populate cost dict
        cost_dict['sku'] = skus
        cost_dict['cost'] = cost_list
        # create df from cost dict
        cost_df = pd.DataFrame(data=cost_dict, dtype='int64')


        # merge costs to bundle dataframe
        df_new = df.merge(right=cost_df, on='sku', how='left')[['title', 'url', 'sku', 'cost_y']]
        df_new.rename(columns={'cost_y': 'cost'}, inplace=True)

        # write new dataframe to .csv
        df_new.to_csv(path_or_buf=file_name, mode='w', index=False, header=True)
    except Exception as ex:
        logging.exception('Exception occurred in backfill_cost(): %s' % ex)


# perform an HTTP GET request to cod store with sku code
def make_request(sku, game, file_name):
    try:
        # define variables
        title_list = []
        url_list = []
        sku_list = []
        cost_list = []

        # set url
        url = 'https://my.callofduty.com/store/sku/{sku}/title/{game}'.format(sku=sku, game=game)

        # set headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
        }

        # perform HTTP GET request
        with rq.get(url=url, headers=headers, timeout=1, allow_redirects=False) as req:
            req_content = req.content

        # parse html response content
        soup = BeautifulSoup(req_content, 'html.parser')
        bundle_title = soup.title.string
        # bundle_url = soup.find(name='meta', attrs={'property': 'og:url'}).get('content')

        # determine if bundle exists
        if bundle_title not in ('My Call of Duty® Bundles', 'Access Denied'):
        # if 'Vintage Vanguard' in bundle_title:
            bundle_title = bundle_title.replace(' | My Call of Duty® Bundles', '')

            title_list.append(bundle_title)
            url_list.append(url)
            sku_list.append(sku)

            # invoke function to get bundle cost
            bundle_cost = get_bundle_cost(sku, game, headers)
            cost_list.append(bundle_cost)

            # invoke function to write data to file
            write_data(title_list, url_list, sku_list, cost_list, file_name)

            return bundle_title
        else:
            return None
    except Exception as ex:
        logging.exception('Exception occurred in make_request(): %s' % ex)


# main running function
def main():
    try:
        # define variables
        logging.basicConfig(filename='codstore.log', encoding='utf-8', level=logging.DEBUG, filemode='w')

        # mw bundles
        # start = 400512 # 400003
        # stop = 400003 # 400512
        # game = 'mw'

        # todo: figure out urls for cw bundles as they do not seem to be on website
        # cw bundles
        # start = 29490000
        # stop = 29492000
        # game = 'cw'

        # vg bundles
        start = 33954800 # 33954000
        stop = 33954000 # 33955000
        game = 'vg'

        initial_skus = range(start, stop, -1)

        # read skus from file for comparison with sku list
        file_name = 'C:/repos/codstore/data/bundles_{game}.csv'.format(game=game)

        if os.path.exists(file_name):
            file_skus = pd.read_csv(filepath_or_buffer=file_name, usecols=['sku'])['sku'].to_list()
            # exclude skus already present in file
            skus = [s for s in initial_skus if s not in file_skus]
        else:
            skus = initial_skus

        skus_len = len(skus)

        # iterate through SKUs concurrently
        with tqdm(total=skus_len) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(make_request, sku, game, file_name): sku for sku in skus}
                results = {}
                for future in concurrent.futures.as_completed(futures):
                    soup = futures[future]
                    results[soup] = future.result()
                    pbar.update(1)
    except Exception as ex:
        logging.exception('Exception occurred in main(): %s' % ex)


# test running function for single skus
def test():
    try:
        # test
        start = 33954595
        stop = 33954594
        game = 'vg'

        initial_skus = range(start, stop, -1)

        # read skus from file for comparison with sku list
        file_name = 'C:/repos/codstore/data/bundles_{game}_test.csv'.format(game=game)

        if os.path.exists(file_name):
            file_skus = pd.read_csv(filepath_or_buffer=file_name, usecols=['sku'])['sku'].to_list()
            # exclude skus already present in file
            skus = [s for s in initial_skus if s not in file_skus]
        else:
            skus = initial_skus

        skus_len = len(skus)

        # iterate through SKUs concurrently
        with tqdm(total=skus_len) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(make_request, sku, game, file_name): sku for sku in skus}
                results = {}
                for future in concurrent.futures.as_completed(futures):
                    soup = futures[future]
                    results[soup] = future.result()
                    pbar.update(1)
    except Exception as ex:
        logging.exception('Exception occurred in test(): %s' % ex)


if __name__ == '__main__':
    # main()
    # test()
    backfill_cost(game='vg')
