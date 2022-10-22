# Load the packages
import scrapy
import os
from scraper_api import ScraperAPIClient
from dotenv import load_dotenv
import json
from w3lib.html import remove_tags
from furniture_ecomm.spiders.home_page_spider import custom_settings_dict

# Load environment variables
load_dotenv()

# Get the scraper API key
client = ScraperAPIClient(api_key = os.getenv("SCRAPER_API_KEY"))

# Load the JSON file containing the category page urls. This was produced in the previous step
f = open("home_page.json", "r")
cat_page_urls_json = json.load(f)
f.close()

# Get the category page URLs from the JSON file using list comprehension
cat_page_urls_list = [i["category_url"] for i in cat_page_urls_json]
cat_page_urls_list = cat_page_urls_list[0:2] # Test the first two only. In a production scenario, this line would be deleted

# Define the spider class
class CatPageSpider(scrapy.Spider):
    name = "cat_page_spider_async_pagination_logic" # Name of the spider
    allowed_domains = ["kemitt.com"] # Allowed domains to crawl
    custom_settings = custom_settings_dict # Standard custom settings of the spider
    custom_settings["FEEDS"] = {"cat_page.json":{"format": "json", "overwrite": True}} # Export to a JSON file with an overwrite functionality

    # Define a function to start the crawling process. This function takes the URLs from cat_page_urls_list
    def start_requests(self):
        for url in cat_page_urls_list:
            yield scrapy.Request(
                client.scrapyGet(url = url, country_code = "de", render = True),
                callback = self.parse,
                dont_filter = True, # This is important so that scrapy does not filter out similar requests. We want all requests to be sent
                meta = dict(master_url = url) # The meta parameter sends a URL that we can refer to in the response
            )
    
    # Define the parsing function that takes the rendered HTML code from the start_requests function
    def parse(self, response):
        # The category's last page. In a production scenario, this determines when to stop crawling
        last_page = response.css("li.number:nth-last-child(2) a::text").get()
        # Change the last page to an integer. If it doesn't exist because the selector above did not return a result, set the last_page variable to "None"
        try:
            last_page = int(last_page)
        except TypeError:
            last_page = None
        
        # If the last page is not None, yield a scrapy.Request that calls another parsing function self.products_parse. Otherwise, terminate the script and throw an error message
        try:
            for i in range(1, 4): # In a production scenario, you would loop until last_page + 1. Here, we only crawl 3 pages for testing purposes
                yield scrapy.Request(
                    client.scrapyGet(url = response.meta["master_url"] + "?page={}".format(i), country_code = "de", render = True),
                    callback = self.products_parse,
                    dont_filter = True,
                    meta = dict(last_page = last_page)
                )
        except TypeError as err: # Handles the case if last_page = None in a production scenario
            print(err + "\n")
            print("Cannot determine the last page of this category. Terminate the script!")
    
    def products_parse(self, response):
        # The selector that contains the data we want (names and links of categories)
        products = response.css("a.productCard-url")

        # Crawl the data
        for prod in products:
            data_dict = {
                "product_name": prod.css("a::attr(title)").get(), # CSS selector of the product name
                "product_url": "https://kemitt.com" + prod.css("a::attr(href)").get(), # CSS selector of the product URL. Append kemitt.com to the selector to get the complete URL
                "page_rank": int(response.css("li.number.active a::text").get()), # CSS selector of the current_page
                "last_page": response.meta["last_page"], # No need to crawl the last page again since we already did it once. We can simply get it from the meta parameter in the response  
                "response_url_cat_page": remove_tags(response.headers["Sa-Final-Url"]) # Response URL
            }

            yield data_dict
