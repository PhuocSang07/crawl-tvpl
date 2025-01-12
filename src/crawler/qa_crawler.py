from typing import Dict, Any, Optional, List
import pandas as pd
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging
from bs4 import BeautifulSoup

from ..utils.url_utils import get_id_from_url, load_url
from ..extractor.document_extractor import (
    get_document_attributes_from_ajax,
    modify_document_attribute,
    get_document_content,
    extract_raw_text_from_html
)

def clean_text(text):
    return text.strip() if text else ""

def remove_link_tag(links):
    return [link for link in links if link]

class QACrawler:
    def __init__(self, num_threads: int = 4):
        self.documents = []
        self.num_threads = num_threads
        self.lock = Lock()
        self.successful_urls = []
        self.failed_urls = []
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def crawl_qa(self, url: str, kw: List[str], time: str, date: str, type_of_qa: str) -> Optional[Dict[str, Any]]:
        try:
            soup = load_url(url, return_content=True)
            title = clean_text(soup.find("h1").text)
            
            # Check if the introduction exists
            introduction_tag = soup.find("strong", {"class": "d-block mt-3 mb-3 sapo"})
            introduction = clean_text(introduction_tag.text if introduction_tag else "No introduction found")

            title_content = soup.find_all("h2")
            author = soup.find("span", {"class": "text-end fw-bold"})

            metadata = {
                "time_published": time,
                "date_published": date, 
                "type": type_of_qa,
                "author": clean_text(author.text) if author else "",
            }
            
            content = []
            for index, h2_tag in enumerate(title_content):
                siblings = h2_tag.find_next_siblings()

                sub_content = []
                for sibling in siblings:
                    if sibling.name == 'h2':
                        break
                    if sibling.name == 'p':
                        if sibling.find("img"):
                            img_tag = sibling.find("img")
                            img_src = img_tag.get("src") if img_tag and img_tag.get("src") else "No image source"
                            sub_content.append(img_src)
                        else:
                            sub_content.append(clean_text(sibling.text))
                    if sibling.name == 'blockquote':
                        from_law = []
                        ems = sibling.find_all("em")
                        for em in ems:
                            from_law.append(em.text)
                        sub_content.append(from_law)
                    if sibling.name == 'a':
                        continue

                # Flatten the sub_content and join all items into strings
                def flatten_and_join(content):
                    flattened = []
                    for item in content:
                        if isinstance(item, list):
                            flattened.extend(flatten_and_join(item))  # Recursively flatten lists
                        else:
                            flattened.append(str(item))  # Ensure the item is a string
                    return flattened
                
                content.append({
                    "sub_title": h2_tag.find("strong").text if h2_tag.find("strong") else "",
                    "sub_content": "\n".join(flatten_and_join(sub_content)),
                })
                

            article_data = {
                "urls": url,
                "keyword": kw,
                "title": title,
                "introduction": introduction,
                "content": content,
                "metadata": metadata,
            }

            return article_data
            
        except Exception as e:
            self.logger.error(f"Error crawling document {url}: {str(e)}")
            return None

    def crawl_batch(self, df: List[pd.DataFrame]) -> None:
        """Crawl a batch of URLs using multiple threads."""
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_url = {executor.submit(self.crawl_qa, data['link'], data['keyword'], data['date'], data['time'], data['type']): data for _, data in df.iterrows()}
            for future in as_completed(future_to_url):
                try:
                    item = future.result()
                    if item and isinstance(item, dict) and 'urls' in item:
                        with self.lock:
                            self.documents.append(item)
                            self.successful_urls.append(item['urls'])
                            self.logger.info(f"Successfully crawled: {item['urls']}")
                    else:
                        with self.lock:
                            url = future_to_url[future].get('link', 'unknown')
                            self.failed_urls.append(url)
                            self.logger.warning(f"Failed to crawl: {url}")
                except Exception as e:
                    url = future_to_url[future].get('link', 'unknown')
                    with self.lock:
                        self.failed_urls.append(url)
                        self.logger.error(f"Error crawling {url}: {str(e)}")

        # Save URLs to files
        if self.successful_urls:
            with open("successful_qa_urls.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.successful_urls))
            self.logger.info(f"Saved {len(self.successful_urls)} successful URLs to successful_qa_urls.txt")

        if self.failed_urls:
            with open("failed_qa_urls.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.failed_urls))
            self.logger.info(f"Saved {len(self.failed_urls)} failed URLs to failed_qa_urls.txt")

    def save_documents(self, output_file: str = "data/qa/documents.json"):
        """Save crawled documents to JSON file."""
        with self.lock:
            if not self.documents:
                self.logger.warning("No documents to save")
                return
                
            # Clean and prepare documents for JSON serialization
            cleaned_documents = []
            for doc in self.documents:
                cleaned_doc = doc.copy()
                cleaned_documents.append(cleaned_doc)
                
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(cleaned_documents, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Saved {len(cleaned_documents)} documents to {output_file}")
