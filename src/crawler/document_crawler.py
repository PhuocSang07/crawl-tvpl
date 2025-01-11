from typing import Dict, Any, Optional, List
import pandas as pd
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging

from ..utils.url_utils import get_id_from_url, load_url
from ..extractor.document_extractor import (
    get_document_attributes_from_ajax,
    modify_document_attribute,
    get_document_content,
    extract_raw_text_from_html
)

class DocumentCrawler:
    def __init__(self, num_threads: int = 4):
        self.documents = []
        self.num_threads = num_threads
        self.lock = Lock()
        self.successful_urls = []
        self.failed_urls = []
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def crawl_document(self, url: str) -> Optional[Dict[str, Any]]:
        """Crawl a single document and extract its information."""
        try:
            # Get document ID
            doc_id = get_id_from_url(url)
            
            # Get document attributes
            extracted_attributes = modify_document_attribute(
                get_document_attributes_from_ajax(url)
            )
            
            if not extracted_attributes:
                self.logger.warning(f"No attributes found for {url}")
                return None
                
            # Extract title
            extracted_title = extracted_attributes["document_type"][0].strip() + " " + extracted_attributes["official_number"][0].strip()

            # Get document content
            doc_content = load_url(url, return_content=True)
            extracted_html_text = get_document_content(doc_content)
            
            if not extracted_html_text:
                self.logger.warning(f"No content at {url}")
                return None
                
            extracted_full_text = extract_raw_text_from_html(extracted_html_text)
            
            
            doc_object = {
                "source_id" : doc_id,
                "source" : "thuvienphapluat.vn",
                "url": url,
                "title" : extracted_title,
                "html_text": extracted_html_text,
                "full_text" : extracted_full_text,
                "attribute": extracted_attributes,
                
            }
            return doc_object
            
        except Exception as e:
            self.logger.error(f"Error crawling document {url}: {str(e)}")
            return None
    
    def crawl_batch(self, urls: List[str]) -> None:
        """Crawl a batch of URLs using multiple threads."""
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_url = {executor.submit(self.crawl_document, url): url for url in urls}
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    doc = future.result()
                    if doc:
                        with self.lock:
                            self.documents.append(doc)
                            self.successful_urls.append(url)
                            self.logger.info(f"Successfully crawled: {url}")
                    else:
                        with self.lock:
                            self.failed_urls.append(url)
                            self.logger.warning(f"Failed to crawl: {url}")
                except Exception as e:
                    with self.lock:
                        self.failed_urls.append(url)
                        self.logger.error(f"Error crawling {url}: {str(e)}")

        # Save URLs to files
        if self.successful_urls:
            with open("successful_urls.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.successful_urls))
            self.logger.info(f"Saved {len(self.successful_urls)} successful URLs to successful_urls.txt")

        if self.failed_urls:
            with open("failed_urls.txt", "w", encoding="utf-8") as f:
                f.write("\n".join(self.failed_urls))
            self.logger.info(f"Saved {len(self.failed_urls)} failed URLs to failed_urls.txt")

    def save_documents(self, output_file: str = "data/processed/documents.json"):
        """Save crawled documents to JSON file."""
        with self.lock:
            if not self.documents:
                self.logger.warning("No documents to save")
                return
                
            # Clean and prepare documents for JSON serialization
            cleaned_documents = []
            for doc in self.documents:
                cleaned_doc = doc.copy()
                # Clean HTML text by replacing problematic characters
                if 'html_text' in cleaned_doc:
                    cleaned_doc['html_text'] = cleaned_doc['html_text'].replace('\r\n', '\\n').replace('\n', '\\n')
                cleaned_documents.append(cleaned_doc)
                
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(cleaned_documents, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Saved {len(cleaned_documents)} documents to {output_file}")
