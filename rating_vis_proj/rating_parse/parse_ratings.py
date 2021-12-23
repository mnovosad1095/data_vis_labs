import pprint
import time
import typing
import pandas as pd 
import requests
from datetime import datetime
import pycountry

TOUCHRETOUCHID = '373311252'

def is_error_response(http_response, seconds_to_sleep: float = 1) -> bool:
    """
    Returns False if status_code is 503 (system unavailable) or 200 (success),
    otherwise it will return True (failed). This function should be used
    after calling the commands requests.post() and requests.get().

    :param http_response:
        The response object returned from requests.post or requests.get.
    :param seconds_to_sleep:
        The sleep time used if the status_code is 503. This is used to not
        overwhelm the service since it is unavailable.
    """
    if http_response.status_code == 503:
        time.sleep(seconds_to_sleep)
        return False

    return http_response.status_code != 200

def getAllCountries() -> typing.List[tuple]:
    cntrs = pd.read_csv("./app-store-countries.csv")
    return [tuple(x) for x in cntrs.to_numpy()]


def get_json(url) -> typing.Union[dict, None]:
    """
    Returns json response if any. Returns None if no json found.

    :param url:
        The url go get the json from.
    """
    response = requests.get(url)
    if is_error_response(response):
        return None
    json_response = response.json()
    return json_response


def get_reviews(app_id, country, page=1) -> typing.List[dict]:
    """
    Returns a list of dictionaries with each dictionary being one review. 
    
    :param app_id:
        The app_id you are searching. 
    :param page:
        The page id to start the loop. Once it reaches the final page + 1, the 
        app will return a non valid json, thus it will exit with the current 
        reviews. 
    """
    print(f'STARTED {page}')
    reviews: typing.List[dict] = [{}]
    try:
        country_code = pycountry.countries.lookup(country).alpha_3
    except LookupError:
        country_code = None
    print(country_code)
    while True:
        url = (f'https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/json')
        json = get_json(url)
        
        if not json:
            return reviews

        data_feed = json.get('feed')

        try:
            if not data_feed.get('entry'):
                get_reviews(app_id, page + 1)

            
            reviews += [
                {
                    'review_id': entry.get('id').get('label'),
                    'title': entry.get('title').get('label'),
                    'date':  entry.get('updated').get('label'),
                    'country': country_code,
                    # 'date':  datetime.strftime(entry.get('updated').get('label'), "%Y-%m-%d"),
                    'author': entry.get('author').get('name').get('label'),
                    'author_url': entry.get('author').get('uri').get('label'),
                    'version': entry.get('im:version').get('label'),
                    'rating': entry.get('im:rating').get('label'),
                    'review': entry.get('content').get('label'),
                    'vote_count': entry.get('im:voteCount').get('label'),
                    'page': page
                }
                for entry in data_feed.get('entry')
                if not entry.get('im:name')
            ]
            page += 1
        except Exception:
            return reviews

def create_reviews_df() -> pd.DataFrame:
    
    countries = getAllCountries()
    data: typing.List[dict] = [{}]

    for country in countries:
        print(f'Parsing {country[0]}')
        data += get_reviews(TOUCHRETOUCHID, country[1])

    df = pd.DataFrame(data)
    return df



if __name__ == '__main__':
    # reviews = get_reviews('373311252', 'us')
    df = create_reviews_df()
    # print(len(reviews))
    # pprint.pprint(reviews) 
    # df.head()   
    
    df.to_csv("./reviews.csv")
    
