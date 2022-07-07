# this python module searches for cod store bundles and captures info about them into a .csv
import requests as rq
import pandas as pd
from bs4 import BeautifulSoup
from tqdm import tqdm
import concurrent.futures


# write dataframe to .csv
def write_data(titles, urls, skus):
    try:
        # define dict for bundle data
        bundle_dict = {}

        # create columns for dataframe
        bundle_dict['title'] = titles
        bundle_dict['url'] = urls
        bundle_dict['sku'] = skus

        # write dataframe to .csv
        bundle_df = pd.DataFrame.from_dict(data=bundle_dict)
        bundle_df.to_csv(path_or_buf='C:/repos/codstore/data/bundles_vg.csv', mode='a')
    except Exception as ex:
        print('Exception occurred in write_data(): %s' % ex)


# perform an HTTP GET request to cod store with sku code
def make_request(sku):
    try:
        # define variables
        title_list = []
        url_list = []
        sku_list = []

        # set url
        url = 'https://my.callofduty.com/store/sku/{sku}/title/vg'.format(sku=sku)
        # sample bundle that does not exist
        # url = 'https://my.callofduty.com/store/sku/33954689/title/vg'

        # set headers
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
        }

        # perform HTTP GET request
        with rq.get(url=url, headers=headers) as req:
            req_content = req.content

        # parse html response content
        soup = BeautifulSoup(req_content, 'html.parser')
        bundle_title = soup.title.string
        bundle_url = soup.find(name='meta', attrs={'property': 'og:url'}).get('content')

        # determine if bundle exists
        # if bundle_title != 'My Call of Duty® Bundles':
        if 'Vintage Vanguard' in bundle_title:
            bundle_title = bundle_title.replace(' | My Call of Duty® Bundles', '')

            title_list.append(bundle_title)
            url_list.append(bundle_url)
            sku_list.append(sku)

            write_data(title_list, url_list, sku_list)

            return bundle_title
        else:
            return None
    except Exception as ex:
        print('Exception occurred in get_request(): %s' % ex)


# main running function
def main():
    try:
        # define variables
        start = 33954500
        stop = 33954700
        skus = range(start, stop)
        skus_len = len(skus)
        sku = 0

        # iterate through SKUs concurrently
        with tqdm(total=skus_len) as pbar:
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(make_request, sku): sku for sku in skus}
                results = {}
                for future in concurrent.futures.as_completed(futures):
                    soup = futures[future]
                    results[soup] = future.result()
                    pbar.update(1)
    except Exception as ex:
        print('Exception occurred in make_request() for sku %i: %s' % (sku, ex))


if __name__ == '__main__':
    main()