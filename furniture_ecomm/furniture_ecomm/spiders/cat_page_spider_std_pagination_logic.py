# Load the packages
import scrapy
import os
from scraper_api import ScraperAPIClient
from dotenv import load_dotenv
import json
from w3lib.html import remove_tags
from furniture_ecomm.spiders.home_page_spider import custom_settings_dict
import re

# Load environment variables
load_dotenv()

# Get the scraper API key
client = ScraperAPIClient(api_key = os.getenv("SCRAPER_API_KEY"))

# Load the JSON file containing the category page urls
f = open("home_page.json", "r")
cat_page_urls_json = json.load(f)
f.close()

# Get the category page URLs from the JSON file using list comprehension. This was produced in the previous step
cat_page_urls_list = [i["category_url"] for i in cat_page_urls_json]
cat_page_urls_list = cat_page_urls_list[0:2] # Test the first two only

# Define a scrapy spider
class CatPageSpider(scrapy.Spider):
    name = "cat_page_spider_std_pagination_logic" # Name of the spider
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
                meta = dict(master_url = url, iter = "2") # The meta parameter sends some information that we can refer to in the response. We use "iter" to loop through the pages via the parse function
            )
    
    # Define the parsing function that takes the rendered HTML code from the start_requests function
    def parse(self, response):
        next_page = response.meta["iter"] # This is a string
        try:
            # The first time this function is used, the response belongs to start_requests, so it has the "master_url" key. After that, we don't send "master_url" anymore, which is where the "except" part comes in
            next_page_url = response.meta["master_url"] + "?page={}".format(response.meta["iter"])
        except:
            # After scrapy parses the first page, the response does not contain master_url anymore, so we simply get the URL from the headers
            # The URL in the headers looks like this "https://kemitt.com/en-eg/categories/bedroom?page=1". We replace the last digit with the next_page variable
            next_page_url = re.sub(pattern = "\d+", repl = next_page, string = remove_tags(response.headers["Sa-Final-Url"]))
        
        # The category's last page. In a production scenario, this determines when to stop crawling
        last_page = response.css("li.number:nth-last-child(2) a::text").get()
        # Change the last page to an integer. If it doesn't exist because the selector above did not return a result, set the last_page variable to "None"
        try:
            last_page = int(last_page)
        except TypeError:
            if next_page == "2": # If this is the first use of the parse function, set last_page to None because the meta parameters is only generated after the first iteration
                last_page = None
            else:
                last_page = response.meta["last_page"]
        
        # The selector that contains the data we want (names and links of categories)
        products = response.css("a.productCard-url")

        # Crawl the data
        for prod in products:
            data_dict = {
                "product_name": prod.css("a::attr(title)").get(), # CSS selector of the product name
                "product_url": "https://kemitt.com" + prod.css("a::attr(href)").get(), # CSS selector of the product URL. Append kemitt.com to the selector to get the complete URL
                "page_rank": int(response.css("li.number.active a::text").get()), # CSS selector of the current_page
                "last_page": last_page, # No need to crawl the last page again since we already did it once. We can simply get it from the variable above
                "response_url_cat_page": remove_tags(response.headers["Sa-Final-Url"]) # Response URL
            }

            yield data_dict

        # If the last page is not None, yield a scrapy.Request that calls another parsing function self.products_parse. Otherwise, terminate the script and throw an error message
        try:
            if int(next_page) < 4: # In a production scenario, we would replace "4" with last_page
                yield scrapy.Request(
                    client.scrapyGet(url = next_page_url, country_code = "de", render = True),
                    callback = self.parse,
                    dont_filter = True,
                    meta = dict(iter = str(int(next_page) + 1), last_page = last_page)
                )
            else:
                print("End of pagination loop")
        except TypeError as err: # Handles the case if last_page = None in a production scenario
            print(err + "\n")
            print("Cannot determine the last page of this category. Terminate the script!")