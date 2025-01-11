import requests
from bs4 import BeautifulSoup
import pandas as pd
from typing import List, Optional
from random import choice
import re
import os

# Get the absolute path to the project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
PROXY_LIST_PATH = os.path.join(PROJECT_ROOT, 'config', 'proxy_list.txt')

def create_proxy_list():
    PROXY_URL = 'https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all'
    proxy_list = requests.get(PROXY_URL)
    os.makedirs(os.path.dirname(PROXY_LIST_PATH), exist_ok=True)
    with open(PROXY_LIST_PATH, 'w') as f:
        f.write(proxy_list.text.replace('\r\n', '\n'))

def choice_proxy() -> Optional[str]:
    if not os.path.exists(PROXY_LIST_PATH):
        create_proxy_list()
    
    with open(PROXY_LIST_PATH, 'r') as f:
        proxies = f.read().split('\n')
        return choice(proxies)

def get_id_from_url(url: str) -> str:
    regex = re.search('(/[0-9]+/)|(-[0-9]+.aspx)', url)
    id_index = list(regex.span())
    result = url[id_index[0]:id_index[1]]
    return re.sub(".aspx|/|-", "", result)

def load_url(url: str, return_content: bool = False) -> Optional[str]:
    """Load URL content with error handling."""
    proxy = choice_proxy()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    response = requests.get(url, headers=headers, proxies={'http':proxy})
    try:
        response.raise_for_status()
        if not return_content:
            return response
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
    except Exception as e: 
        print(e)

def load_url_luocdo(url, url_luocdo, return_content=False):
    proxy = choice_proxy()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Referer": url
    }
    response = requests.get(url_luocdo, headers=headers, proxies={'http':proxy})
    try:
        response.raise_for_status()
        if not return_content:
            return response
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
    except Exception as e: 
        print(e)
        
def get_all_sitemaps_url(sitemap_url: str) -> List[str]:
    """Get all sitemap URLs from the main sitemap."""
    sitemap_content = load_url(sitemap_url, return_content=True)
    if not sitemap_content:
        return []
    
    sitemap_tags = sitemap_content.find_all('loc')
    return [tag.text for tag in sitemap_tags]

# Helper function for crawl Q&A
def get_type_of_law(url):
    text = url.split("/")[4]
    return text.split("?")[0]