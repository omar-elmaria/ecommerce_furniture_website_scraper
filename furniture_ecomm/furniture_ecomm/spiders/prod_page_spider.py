# Load the packages
import scrapy
import os
from dotenv import load_dotenv
from w3lib.html import remove_tags
from scraper_api import ScraperAPIClient
import json
import re
from furniture_ecomm.spiders.home_page_spider import custom_settings_dict

# Load environment variables
load_dotenv()

# Get the Scraper API key
client = ScraperAPIClient(api_key = os.getenv("SCRAPER_API_KEY"))

# Load the JSON file containing the product page urls
f = open("cat_page.json", "r")
prod_page_urls_json = json.load(f)
f.close()

# Get the category page URLs from the JSON file using list comprehension
prod_page_urls_list = [i["product_url"] for i in prod_page_urls_json]

# Define the spider class
class ProdPageSpider(scrapy.Spider):
    name = "prod_page_spider" # Name of the spider
    allowed_domains = ["kemitt.com"] # Allowed domains to crawl
    custom_settings = custom_settings_dict # Standard custom settings of the spider
    custom_settings["FEEDS"] = {"prod_page.json":{"format": "json", "overwrite": True}} # Export to a JSON file with an overwrite functionality

    def start_requests(self):
        for url in prod_page_urls_list:
            yield scrapy.Request(
                client.scrapyGet(url = url, country_code = "de", render = True),
                callback = self.parse,
                dont_filter = True # This is important so that scrapy does not filter out similar requests. We want all requests to be sent
            )

    # Define a function to start the crawling process. This function takes the URLs from prod_page_urls_list
    def parse(self, response):
        # The product page could have one or two prices, depending on whether or not the product has a discount. This piece of logic handles this case
        current_price = response.xpath("//h3[contains(@class, 'productSingle-price')]/text()").getall()
        try:
            current_price = int(re.findall(pattern = "\d+", string = current_price[1])[0]) # If the page has two prices, the current price will be located at index 1
        except IndexError:
            print("There is no strikethrough price. Index is out of range")
            current_price = int(re.findall(pattern = "\d+", string = current_price[0])[0]) # If the page has one price, the current price will be located at index 0
        
        # Some product pages don't have a strikethrough price. Construct a try-except function to deal with that
        strikethrough_price = response.xpath("//h3[contains(@class, 'productSingle-priceBefore')]/text()").get()
        try:
            strikethrough_price = int(strikethrough_price)
        except TypeError:
            strikethrough_price = None

        # Crawl the data
        yield {
            "product_name": response.xpath("//h1[contains(@class, 'productSingle-title')]/text()").get(),
            "category_name": response.xpath("//div[contains(@class, 'productSingle-flex')]/h5/text()").get(),
            "category_url": "https://kemitt.com" + response.xpath("//div[contains(@class, 'productSingle-flex')]/a/@href").get(),
            "supplier_url": "https://kemitt.com" + response.css("a.productSingle-designer::attr(href)").get(),
            "supplier_name": re.findall(pattern = "(?<=designers\/).+", string = response.css("a.productSingle-designer::attr(href)").get())[0],
            "strikethrough_price": strikethrough_price,
            "current_price": current_price,
            "promised_delivery_time_in_days": int(re.findall(pattern = "\d+", string = response.xpath("//p[contains(@tooltip, 'It Takes That Time')]/text()").get())[0]),
            "main_image_url": response.xpath("//div[@class = 'productSingle-slider']//img/@src").get(),
            "response_url_prod_page": remove_tags(response.headers["Sa-Final-Url"])
        }