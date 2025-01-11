import json
import requests
from bs4 import BeautifulSoup
import os
from typing import Optional
from random import choice
from concurrent.futures import ThreadPoolExecutor, as_completed

# Get the absolute path to the project root directory
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
PROXY_LIST_PATH = os.path.join(PROJECT_ROOT, 'config', 'proxy_list.txt')

def clean_text(text):
    return text.strip() if text else ""

def remove_link_tag(links):
    return [link for link in links if link]

def get_type_of_law(url):
    text = url.split("/")[3]
    return text.split("?")[0]

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

def load_url(url: str, return_content: bool = False) -> Optional[str]:
    proxy = choice_proxy()
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
    response = requests.get(
        url, 
        headers=headers, 
        proxies={'http':proxy}
        )
    try:
        response.raise_for_status()
        if not return_content:
            return response
        else:
            soup = BeautifulSoup(response.content, 'html.parser')
            return soup
    except Exception as e: 
        print(e)


def crawl_thuvienphapluat(base_url, start_num_page=0, end_num_page=1, delay=2):
    articles_data = []  

    for i in range(start_num_page, end_num_page + 1):  
        url = base_url.format(i)
        print(f"Crawling URL: {url}")
        type_of_question = get_type_of_law(url)
        response = load_url(url, return_content=True)

        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            
            # Extract article links
            articles = soup.select('article')
            keywords = []
            sub_time = []
            links = []
            for article in articles:
                tag_a = article.find('a', class_='title-link')
                links.append(tag_a.get("href") if tag_a else None)

                # Keyword
                keyword_find = article.select('.d-block.sub-item-head-keyword')
                if keyword_find:
                    keywords.append([kw.get_text(strip=True) for kw in keyword_find])
                else:
                    keywords.append([None])
                    
                 # Get sub-time
                sub_time_tag = article.find('span', class_='sub-time')
                sub_time.append(sub_time_tag.get_text(strip=True) if sub_time_tag else None)

            for index, link in enumerate(links):
                if link is None:
                    continue
                detail_url = f"https://thuvienphapluat.vn{link}"
                article_data = parse_detail_article(detail_url, keywords[index], sub_time[index], type_of_question)
                if article_data:
                    articles_data.append(article_data)
                    
                    with open("crawled_data.json", "a", encoding="utf-8") as f:
                        json.dump(article_data, f, ensure_ascii=False, indent=4)
                        f.write(",\n")       
        else:
            print(f"Failed to fetch data from {url}. HTTP Status Code: {response.status_code}")

def parse_detail_article(url, kw, st, delay=2, type_of_question=None):
    response = load_url(url, return_content=True)
    
    if st:
        time, date = st.replace(" ","").split("|")
    else:
        time = ""
        date = ""
            
    if response.status_code == 200:
        html = response.text
        soup = BeautifulSoup(html, features="lxml")

        title = clean_text(soup.find("h1").text)
        
        # Check if the introduction exists
        introduction_tag = soup.find("strong", {"class": "d-block mt-3 mb-3 sapo"})
        introduction = clean_text(introduction_tag.text if introduction_tag else "No introduction found")

        title_content = soup.find_all("h2")
        author = soup.find("span", {"class": "text-end fw-bold"})

        metadata = {
            "time_published": time,
            "date_published": date, 
            "type": type_of_question,
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

            def flatten_and_join(content):
                flattened = []
                for item in content:
                    if isinstance(item, list):
                        flattened.extend(flatten_and_join(item)) 
                    else:
                        flattened.append(str(item))  
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
    else:
        print(f"Failed to fetch detail article from {url}. HTTP Status Code: {response.status_code}")
        return None
    
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

num_page=500

with ThreadPoolExecutor(max_workers=5) as executor:
    future_to_url = {
        executor.submit(
            crawl_thuvienphapluat, 
            base_url=url, 
            start_num_page=1, 
            end_num_page=num_page
        ): url for url in base_url
    }
    
    for future in as_completed(future_to_url):
        url = future_to_url[future]
        try:
            future.result()
        except Exception as e:
            print(f'Error crawling {url}: {str(e)}')