from typing import List
import pandas as pd 
from src.utils.url_utils import load_url, get_type_of_law
import json
import requests
from bs4 import BeautifulSoup
import os
import time
from random import choice
from typing import List, Optional
import pandas as pd 
from concurrent.futures import ThreadPoolExecutor

def get_all_sub_qa_url(url: str) -> List[str]:
    """Get all sub-URLs from the main QA URL."""
    soup = load_url(url, return_content=True)
    # Extract article links
    articles = soup.select('article')
    keywords = []
    time = []
    date = []
    links = []
    type_qa = []

    type_url = get_type_of_law(url)
    for article in articles:
        tag_a = article.find('a', class_='title-link')
        links.append("https://thuvienphapluat.vn" + tag_a.get("href") if tag_a else None)
        type_qa.append(type_url)
        # Keyword
        keyword_find = article.select('.d-block.sub-item-head-keyword')
        if keyword_find:
            keywords.append([kw.get_text(strip=True) for kw in keyword_find])
        else:
            keywords.append([None])
            
         # Get sub-time
        sub_time_tag = article.find('span', class_='sub-time')
        if sub_time_tag:
            time.append(sub_time_tag.get_text(strip=True).replace(" ","").split("|")[0])
            date.append(sub_time_tag.get_text(strip=True).replace(" ","").split("|")[1])
        else:
            time.append(None)
            date.append(None)
        
    df = pd.DataFrame(list(zip(links, keywords, date, time, type_qa)), columns =['link', 'keyword', "date", "time", "type"])
    return df

def process_url(url: str):
    try:
        df = get_all_sub_qa_url(url)
        return df
    except Exception as e:
        with open("failed_links.txt", "a") as log_file:
            log_file.write(f"{url}\t{str(e)}\n")
        print(f"Error processing {url}: {e}")
        return None
    
def process_urls_multithreaded(urls, max_workers=10, num_page_urls=500):
    dfs = []
    failed_urls = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(process_url, urls)
        for url, result in zip(urls, results):
            if result is not None:
                dfs.append(result)
            else:
                failed_urls.append(url)
    
    if failed_urls:
        with open("failed_links_summary.txt", "w") as f:
            for url in failed_urls:
                f.write(url + "\n")
    
    if dfs:
        final_df = pd.concat(dfs, ignore_index=True)
        return final_df
    else:
        return pd.DataFrame()  

