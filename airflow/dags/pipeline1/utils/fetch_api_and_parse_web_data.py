import json
import pandas
import requests
from bs4 import BeautifulSoup
import logging

logger = logging.getLogger(__name__)
def fetch_backend_api_get_parsed_data(**kwargs):
    # Keywords to search in finshots and yourstory website
    COMPANIES_TO_SEARCH=["HDFC", "Tata Motors"]

    # Make /get request to finshots api and get parsed data
    FINSHOTS_BACKEND_API = f"https://backend.finshots.in/backend/search/"
    MAX_ARTICLE_PER_COMPANY = 5
    """
    Example finshots API respone
    > GET /backend/search/?q=TATA%20MOTORS
    > URI: https://backend.finshots.in/backend/search/?q=TATA%20MOTORS
    > Response
    {
    "total_hits": 10,
    "matches": [
        {
            "post_url": "https://finshots.in/markets/reviewing-tata-motors/",
            "cover_image": "https://d3jlwjv6gmyigl.cloudfront.net/images/2021/07/tata-motors.jpg",
            "title": "Reviewing Tata Motors",
            "excerpt": "An explainer on why Tata Motors' recent performance isn't dissuading analysts",
            "published_date": "2021-07-30T14:29:14+00:00"
        },
        {
            "post_url": "https://finshots.in/markets/what-went-wrong-at-tata-motors/",
            "cover_image": "https://d3jlwjv6gmyigl.cloudfront.net/images/2020/09/tat6.jpg",
            "title": "What went wrong at Tata Motors?",
            "excerpt": "An explainer on the Tata Motors debt problem",
            "published_date": "2020-09-04T15:33:27+00:00"
        },
        {
            "post_url": "https://finshots.in/archive/tata-motors-is-killing-its-dvr-shares/",
            "cover_image": "https://cdn.finshots.app/images/2023/07/design-124-tata-motors.jpg",
            "title": "Tata Motors is killing its DVR shares!",
            "excerpt": "An explainer on shares with differential voting rights and Tata Motors' decision to delist them.",
            "published_date": "2023-07-27T01:30:00+00:00"
        }]
    }
    """

    dataframe = pandas.DataFrame(columns=['title', 'post_url', 'created_at', 'short_desc', 'article', 'company', 'source'])

    for company in COMPANIES_TO_SEARCH:
        query_url = f"{FINSHOTS_BACKEND_API}?q={company}"
        
        http_response = requests.get(query_url)
        http_response.raise_for_status() 

        json_formatted_data_list=json.loads(http_response.content)["matches"]
        fetched_article_count = 0
        for json_formatted_data in json_formatted_data_list:
            if fetched_article_count >= MAX_ARTICLE_PER_COMPANY:
                break
            post_title = json_formatted_data['title']
            post_url=json_formatted_data['post_url']
            created_at=json_formatted_data['published_date']
            short_desc = json_formatted_data['excerpt']
            # GET DETAILED DATA AROUND THE POST
            req = requests.get(url=post_url)
            soup_object = BeautifulSoup(req.content, 'lxml')
            article_details = soup_object.find('div',{"class":"post-content"}).text.strip()
            row=pandas.DataFrame({
                "title": [post_title],
                "post_url": [post_url],
                "created_at": [created_at],
                "short_desc": [short_desc],
                "article": [article_details],
                "company" : [company],
                "source": ["FINSHOTS"]
            })
            dataframe = pandas.concat([dataframe, row], ignore_index=True)
            fetched_article_count += 1
        
    parse_json_data = json.loads(dataframe.to_json(orient='records'))
    logger.info(f"Parsed output data for company: {company} -> {parse_json_data}")

    task_instance = kwargs['task_instance']
    task_instance.xcom_push(key='parsed_data', value=parse_json_data)
    
    