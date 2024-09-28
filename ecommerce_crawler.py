import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
from datetime import datetime

class EcommerceCrawler:
    def __init__(self, config_file):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
        with open('ecomm_data.json', 'r') as data_file:
            self.ecomm_data = json.load(data_file)['ECOMM']
        self.session = requests.Session()
        self.ecomm_data = self.config['ECOMM']

    def crawl(self, website):
        website_config = self.ecomm_data[website]
        base_url = website_config['base_url']
        selectors = self.config['ECOMM'][website]['selectors']
        website_data = self.ecomm_data[website]

        for category, data in website_data['categories'].items():
            self.crawl_category(website, base_url, category, data['url'], selectors)

        self.save_ecomm_data()

    def crawl_category(self, website, base_url, category, url, selectors, depth=0):
        if depth > self.config[website].get('max_subcategory_depth', 5):
            return

        full_url = urljoin(base_url, url)
        response = self.session.get(full_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        category_container = soup.select_one(selectors['category_container'])
        if category_container:
            subcategories = category_container.select(selectors['subcategory_items'])
            for subcategory in subcategories:
                subcategory_name = subcategory.text.strip()
                subcategory_url = subcategory.find('a')['href']
                new_category = f"{category} > {subcategory_name}"
                self.create_subcategory(website, category, new_category, subcategory_url)
                self.crawl_category(website, base_url, new_category, subcategory_url, selectors, depth + 1)
        else:
            self.crawl_products(website, base_url, category, full_url, selectors)

    def crawl_products(self, website, base_url, category, url, selectors):
        page = 1
        while page <= self.config[website].get('max_pages_per_category', 10):
            full_url = url if page == 1 else urljoin(url, f"?page={page}")
            response = self.session.get(full_url)
            soup = BeautifulSoup(response.text, 'html.parser')

            product_container = soup.select_one(selectors['product_container'])
            if not product_container:
                break

            for product in product_container.select(selectors['product_item']):
                if any(ignore_class in product.get('class', []) for ignore_class in self.config[website].get('ignore_classes', [])):
                    continue

                product_link = product.select_one(selectors['product_link'])
                if product_link:
                    product_name = product_link.text.strip()
                    product_url = urljoin(base_url, product_link['href'])
                    self.save_product(website, category, product_name, product_url)

            next_page = soup.select_one(selectors['next_page'])
            if not next_page or 'disabled' in next_page.get('class', []):
                break

            page += 1
            time.sleep(1)  # Be respectful with request frequency

    def create_subcategory(self, website, parent_category, new_category, url):
        categories = self.ecomm_data[website]['categories']
        category_parts = new_category.split(' > ')
        
        current_dict = categories
        for part in category_parts[:-1]:
            if part not in current_dict:
                current_dict[part] = {"sub_categories": {}, "products": [], "crawled_at": None}
            current_dict = current_dict[part]['sub_categories']
        
        last_part = category_parts[-1]
        if last_part not in current_dict:
            current_dict[last_part] = {
                "url": url,
                "sub_categories": {},
                "products": [],
                "crawled_at": None
            }

    def save_product(self, website, category, name, url):
        categories = self.ecomm_data[website]['categories']
        category_parts = category.split(' > ')
        
        current_dict = categories
        for part in category_parts[:-1]:
            current_dict = current_dict[part]['sub_categories']
        
        last_part = category_parts[-1]
        current_dict[last_part]['products'].append({"name": name, "url": url})
        current_dict[last_part]['crawled_at'] = datetime.now().isoformat()

    def save_ecomm_data(self):
        with open('ecomm_data.json', 'w') as f:
            json.dump(self.ecomm_data, f, indent=2)

if __name__ == "__main__":
    crawler = EcommerceCrawler('ecomm_config.json')
    crawler.crawl('amazon')