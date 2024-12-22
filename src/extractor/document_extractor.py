import json
import requests
import html2text
from bs4 import BeautifulSoup
import re
from typing import Dict, Any, Optional
from ..utils.url_utils import load_url_luocdo, get_id_from_url

def keep_one_white_space(string):
    return re.sub(' +', ' ', string)

def extract_raw_text_from_html(html_text: str) -> str:
    """Extract raw text from HTML content."""
    if html_text == "":
        return ""
    else:
        text = html2text.html2text(html_text)
        text = re.sub('(\*|\||\_|\-)', '', text)
        text = text.replace("\\", "")
        text = re.sub(r'\n\s*\n', '\n\n', text)
        return keep_one_white_space(text)

def get_document_content(soup) -> str:
    if soup.find("div", attrs={"class": "TaiVanBan"}) is None:
        #Loại trừ các văn bản không thể xem được
        content = soup.find("div", attrs={"class": "content1"})
        if content.find("a", attrs={"style": "color:blue", "class":"clsopentLogin"}) is None:
            for p_tag in content.find_all(True):
                del p_tag["href"]
            return str(content)
        else:
            return ""
    else:
        return ""

def get_document_attributes_from_ajax(url: str) -> Dict[str, Any]:
    """Get document attributes from AJAX endpoint."""
    doc_id = get_id_from_url(url)
    url_luocdo = "https://thuvienphapluat.vn/AjaxLoadData/LoadLuocDo.aspx?LawID=" + doc_id

    try:
        soup = load_url_luocdo(url, url_luocdo, return_content=True)
        atts = {}

        atts['Mô tả'] = soup.find("div", attrs={"class": "tt"}).text.strip() #None type has no "text"
        for att_soup in soup.find_all("div", attrs={"class": "att"}):
            att_name = att_soup.find("div", attrs={"class": "hd fl"}).text.strip().replace(":", "")
            att_value = att_soup.find("div", attrs={"class": "ds fl"}).text.strip()

            atts[att_name] = att_value
        atts['Ghi chú'] = soup.find("div", attrs={"class": "tt", "style": "font-weight: normal"}).text.strip()
        return atts

    except Exception as e:
        print("get_document_attributes_from_ajax error: " + str(e) + " at " + str(url))
        return {}

def modify_document_attribute(doc_attribute: Dict[str, Any]) -> Dict[str, Any]:
    new_atts = {}
    new_atts["official_number"] = [doc_attribute.get("Số hiệu", "")]
    new_atts["document_info"] = [doc_attribute.get('Mô tả', ""),
                                 "Tình trạng: " + doc_attribute.get("Tình trạng", "")]
    new_atts["issuing_body/office/signer"] = [doc_attribute.get("Nơi ban hành", ""),
                                              "",
                                              doc_attribute.get("Người ký", "")]
    new_atts["document_type"] = [doc_attribute.get("Loại văn bản", "")]
    new_atts["document_field"] = [doc_attribute.get("Lĩnh vực, ngành", "")]

    new_atts["issued_date"] = doc_attribute.get("Ngày ban hành", "")
    new_atts["effective_date"] = doc_attribute.get("Ngày hiệu lực", "")
    new_atts["enforced_date"] = doc_attribute.get("Ngày đăng", "")
    new_atts["note"] = doc_attribute.get("Ghi chú", "")

    #extra atts
    # new_atts["the_reason_for_this_expiration"] = []
    # new_atts["the_reason_for_this_expiration_part"] = []
    # new_atts["effective_area"] = ""
    # new_atts["expiry_date"] = ""
    # new_atts["gazette_date"] = ""
    # new_atts["information_applicable"] = []
    # new_atts["document_department"] = []
    # new_atts["collection_source"] = []

    return new_atts