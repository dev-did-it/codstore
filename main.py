# this python module searches for cod store bundles and captures info about them into a .csv
import logging
import os
import concurrent.futures
import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm


# write dataframe to .csv
def write_data(titles, urls, skus, game):
    try:
        # define dict for bundle data
        bundle_dict = {'title': titles, 'url': urls, 'sku': skus}

        # write dataframe to .csv
        bundle_df = pd.DataFrame.from_dict(data=bundle_dict)

        # set file name and path
        file_name = 'C:/repos/codstore/data/bundles_{game}.csv'.format(game=game)

        # if file does not exist write header
        if not os.path.exists(file_name):
            bundle_df.to_csv(file_name, index=False)
        else:  # else it exists so append without writing the header
            bundle_df.to_csv(path_or_buf=file_name, mode='a', index=False, header=False)
    except Exception as ex:
        logging.exception('Exception occurred in write_data(): %s' % ex)


# perform an HTTP GET request to cod store with sku code
def make_request(sku, game):
    try:
        # define variables
        title_list = []
        url_list = []
        sku_list = []

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
        bundle_url = soup.find(name='meta', attrs={'property': 'og:url'}).get('content')

        # determine if bundle exists
        if bundle_title != 'My Call of Duty® Bundles':
        # if 'Vintage Vanguard' in bundle_title:
            bundle_title = bundle_title.replace(' | My Call of Duty® Bundles', '')

            title_list.append(bundle_title)
            url_list.append(bundle_url)
            sku_list.append(sku)

            # invoke function to write data to file
            write_data(title_list, url_list, sku_list, game)

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
        # start = 400003
        # stop = 400512
        # game = 'mw'

        # cw bundles
        # start = 29490000
        # stop = 29492000
        # game = 'cw'

        # vg bundles
        start = 33954444
        stop = 33954664
        game = 'vg'

        initial_skus = range(start, stop)

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
                futures = {executor.submit(make_request, sku, game): sku for sku in skus}
                results = {}
                for future in concurrent.futures.as_completed(futures):
                    soup = futures[future]
                    results[soup] = future.result()
                    pbar.update(1)
    except Exception as ex:
        logging.exception('Exception occurred in main(): %s' % ex)


if __name__ == '__main__':
    main()