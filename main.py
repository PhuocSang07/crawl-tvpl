from src.utils.url_utils import get_all_sitemaps_url
from src.crawler.document_crawler import DocumentCrawler
from src.crawler.sitemap_crawler import get_all_document_url, load_record_to_list
import logging
from typing import List
from itertools import zip_longest

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def split_urls_into_batches(urls: List[str], batch_size: int) -> List[List[str]]:
    """Split URLs into batches of specified size."""
    return [urls[i:i + batch_size] for i in range(0, len(urls), batch_size)]

def main():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    
    # Get sitemap URLs
    sitemap_url = 'https://thuvienphapluat.vn/sitemap.xml'
    sitemap_urls = get_all_sitemaps_url(sitemap_url)
    logger.info(f'Number of sitemap URLs: {len(sitemap_urls)}')
    
    # Get document URLs from first sitemap
    get_all_document_url(sitemap_urls[:1])
    
    # Load URLs
    url_list = load_record_to_list("./data/raw/urls/urls.lines")
    url_list = list(set(url_list))  # Remove duplicates
    logger.info(f'Number of document URLs: {len(url_list)}')
    
    # Initialize crawler with 4 threads
    crawler = DocumentCrawler(num_threads=4)
    
    # Split URLs into batches (50 URLs per batch)
    batch_size = 1
    url_batches = split_urls_into_batches(url_list, batch_size)
    total_batches = len(url_batches)
    
    # Process URL batches
    for i, batch in enumerate(url_batches, 1):
        logger.info(f'Processing batch {i}/{total_batches} ({len(batch)} URLs)')
        crawler.crawl_batch(batch)
        # Save after each batch
        crawler.save_documents()

if __name__ == "__main__":
    main()
