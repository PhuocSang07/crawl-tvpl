from src.crawler.document_crawler import DocumentCrawler
from src.crawler.qa_crawler import QACrawler
from src.crawler.sitemap_crawler import get_all_document_url, load_record_to_list
from src.extractor.qa_extractor import get_all_sub_qa_url, get_type_of_law, process_urls_multithreaded
from src.utils.url_utils import load_url
import logging
from typing import List
from itertools import zip_longest
import os
import pandas as pd
import json
import requests
import os
import time

base_url = [ 
    "https://thuvienphapluat.vn/phap-luat/cong-nghe-thong-tin?page={}",
    "https://thuvienphapluat.vn/phap-luat/doanh-nghiep?page={}",
    "https://thuvienphapluat.vn/phap-luat/lao-dong-tien-luong?page={}",
    "https://thuvienphapluat.vn/phap-luat/bat-dong-san?page={}",
    "https://thuvienphapluat.vn/phap-luat/vi-pham-hanh-chinh?page={}",
    "https://thuvienphapluat.vn/phap-luat/bao-hiem?page={}",
    "https://thuvienphapluat.vn/phap-luat/quyen-dan-su?page={}",
    "https://thuvienphapluat.vn/phap-luat/van-hoa-xa-hoi?page={}",
    "https://thuvienphapluat.vn/phap-luat/thuong-mai?page={}",
    "https://thuvienphapluat.vn/phap-luat/trach-nhiem-hinh-su?page={}",
    "https://thuvienphapluat.vn/phap-luat/xay-dung-do-thi?page={}",
    "https://thuvienphapluat.vn/phap-luat/chung-khoan?page={}",
    "https://thuvienphapluat.vn/phap-luat/ke-toan-kiem-toan?page={}",
    "https://thuvienphapluat.vn/phap-luat/thue-phi-le-phi?page={}",
    "https://thuvienphapluat.vn/phap-luat/xuat-nhap-khau?page={}",
    "https://thuvienphapluat.vn/phap-luat/tien-te-ngan-hang?page={}",
    "https://thuvienphapluat.vn/phap-luat/dau-tu?page={}",
    "https://thuvienphapluat.vn/phap-luat/so-huu-tri-tue?page={}",
    "https://thuvienphapluat.vn/phap-luat/thu-tuc-to-tung?page={}",
    "https://thuvienphapluat.vn/phap-luat/tai-chinh-nha-nuoc?page={}",
    "https://thuvienphapluat.vn/phap-luat/the-thao-y-te?page={}",
    "https://thuvienphapluat.vn/phap-luat/dich-vu-phap-ly?page={}",
    "https://thuvienphapluat.vn/phap-luat/tai-nguyen-moi-truong?page={}",
    "https://thuvienphapluat.vn/phap-luat/cong-nghe-thong-tin?page={}",
    "https://thuvienphapluat.vn/phap-luat/giao-duc?page={}",
    "https://thuvienphapluat.vn/phap-luat/giao-thong-van-tai?page={}",
    "https://thuvienphapluat.vn/phap-luat/hanh-chinh?page={}",
    "https://thuvienphapluat.vn/phap-luat/linh-vuc-khac?page={}"
    ]

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def split_urls_into_batches(df: pd.DataFrame, batch_size: int) -> List[pd.DataFrame]:
    """Split URLs into batches of specified size."""
    return [df.iloc[i:i + batch_size] for i in range(0, len(df), batch_size)]

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Get URLs
    num_page = 500
    num_page_urls = [url.format(i) for url in base_url for i in range(1, num_page)]

    if not os.path.exists('./data/data.csv'):
        df = process_urls_multithreaded(num_page_urls=num_page_urls, max_workers=1024)
        df = df.dropna()
        df.to_csv('./data/data.csv', index=False)
    else:
        df = pd.read_csv('./data/data.csv').iloc[:2]
    
    logger.info(f'Number of sitemap URLs: {len(df)}')
        
    # Initialize crawler with 4 threads
    crawler = QACrawler(num_threads=4)
    
    # Split URLs into batches (50 URLs per batch)
    batch_size = 100
    url_batches = split_urls_into_batches(df=df, batch_size=batch_size)
    total_batches = len(url_batches)
    logger.info(f'Total batches: {total_batches}')

    # Process URL batches
    for i, batch in enumerate(url_batches, 1):
        logger.info(f'Processing batch {i}/{total_batches} ({len(batch)} URLs)')
        crawler.crawl_batch(batch)
        # Save after each batch
        crawler.save_documents()

if __name__ == "__main__":
    main()
