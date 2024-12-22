import fasteners
from typing import List
import os
import re
from tqdm import tqdm
from ..utils.url_utils import load_url

def write_to_record(object, file_output_path, by_line=False, is_append=False):
    try:
        # Create base directories if they don't exist
        directory = os.path.dirname(os.path.abspath(file_output_path))
        if not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)
            
        # Use absolute path for the lock file
        lock_path = os.path.join(directory, f"{os.path.basename(file_output_path)}.lock")
        
        with fasteners.InterProcessLock(lock_path):
            mode = "a" if is_append else "w+"
            with open(file_output_path, mode, encoding='utf-8') as file:
                if by_line:
                    file.write(str(object) + '\n')
                else:
                    file.write(str(object))
                    
    except PermissionError as e:
        print(f"Permission denied when writing to {file_output_path}. Please check folder permissions.")
        raise
    except Exception as e:
        print(f"Error writing to {file_output_path}: {str(e)}")
        raise

def get_all_document_url_per_page(sitemap_url):
    sitemap_soup = load_url(sitemap_url, return_content=True)
    if sitemap_soup is None:
        return []

    res = [url.text for url in sitemap_soup.find_all("loc")]
    return res

def get_all_document_url(
    sitemap_urls: List[str], 
    output_dir_url: str = "./data/raw/urls",
    output_dir_sitemap: str = "./data/raw/sitemap"
    ) -> None:

    """Extract all document URLs from sitemaps and save them."""
    os.makedirs(output_dir_url, exist_ok=True)
    os.makedirs(output_dir_sitemap, exist_ok=True)

    url_output_file = os.path.join(output_dir_url, "urls.lines")
    
    for sitemap_url in tqdm(sitemap_urls, total=len(sitemap_urls)):
        try:
            match = re.findall("(\d+)", sitemap_url)
            if not match:
                continue
                
            file_name = os.path.join(output_dir_sitemap, f"sitemaps_part{match[0]}.xml")
            soup = load_url(sitemap_url, return_content=True)
            if soup:
                write_to_record(soup.prettify(), file_name)
                
                document_urls = get_all_document_url_per_page(sitemap_url)

                for document_url in document_urls:
                    write_to_record(document_url, url_output_file, by_line=True, is_append=True)


        except Exception as e:
            print(f"Error processing sitemap {sitemap_url}: {str(e)}")
            continue

def load_record_to_list(file_path: str) -> List[str]:
    """Load URLs from file into a list."""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"Error loading URLs from file: {str(e)}")
        return []