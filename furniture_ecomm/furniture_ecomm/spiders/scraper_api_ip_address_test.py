# Load packages
import scrapy
from dotenv import load_dotenv
import os
from scraper_api import ScraperAPIClient
from furniture_ecomm.spiders.home_page_spider import custom_settings_dict

# Load environment variables
load_dotenv()

# Get the Scraper API key
client = ScraperAPIClient(api_key = os.getenv("SCRAPER_API_KEY"))

# Define the spider
class IPAddressSpider(scrapy.Spider):
    name = "ip_address_spider" # Name of the spider
    allowed_domains = ["httpbin.org"] # Allowed domains to crawl
    custom_settings = custom_settings_dict # Standard custom settings of the spider
    custom_settings["FEEDS"] = {"ip_address.json":{"format": "json", "overwrite": True}} # Export to a JSON file with an overwrite functionality

    def start_requests(self):
        for i in range(0, 10):
            yield scrapy.Request(client.scrapyGet(url = "http://httpbin.org/ip"), method = "GET", callback = self.parse, dont_filter = True)

    def parse(self, response):
        print(response.text)
