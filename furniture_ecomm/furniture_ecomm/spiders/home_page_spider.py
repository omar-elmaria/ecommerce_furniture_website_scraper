# Load the packages
import scrapy
from dotenv import load_dotenv
import os
from scraper_api import ScraperAPIClient
from w3lib.html import remove_tags

# Load the environment variables
load_dotenv()

# Get the scraper API key
client = ScraperAPIClient(api_key = os.getenv("SCRAPER_API_KEY"))

# Define the dictionary that contains the custom settings of the spiders. This will be used in all other spiders
custom_settings_dict = {
    "FEED_EXPORT_ENCODING": "utf-8", # UTF-8 deals with all types of characters
    "RETRY_TIMES": 3, # Retry failed requests up to 3 times
    "AUTOTHROTTLE_ENABLED": False, # Disables the AutoThrottle extension (recommended to be used with proxy services unless the website is tough to crawl)
    "CONCURRENT_REQUESTS": 5, # The maximum number of concurrent (i.e. simultaneous) requests that will be performed by the Scrapy downloader
    "DOWNLOAD_TIMEOUT": 60 # Setting the timeout parameter to 60 seconds as per the ScraperAPI documentation
}

# Define the spider class
class HomePageSpider(scrapy.Spider):
    name = "home_page_spider" # Name of the spider
    allowed_domains = ["kemitt.com"] # Allowed domains to crawl
    custom_settings = custom_settings_dict # Standard custom settings of the spider
    custom_settings["FEEDS"] = {"home_page.json":{"format": "json", "overwrite": True}} # Export to a JSON file with an overwrite functionality

    # Define a function to start the crawling process
    def start_requests(self):
        # The home page of the website we want to crawl
        url = "https://kemitt.com/en-eg/"
        yield scrapy.Request(client.scrapyGet(url = url, country_code = "de", render = True), callback = self.parse)

    # Define the parsing function that takes the rendered HTML code from the start_requests function
    def parse(self, response):
        # The selector that contains the data we want (names and links of categories)
        categories = response.css("a.categoriesSlider-slide")
        
        # Crawl the data
        for cat in categories:
            yield {
                "category_name": cat.css("h6.u-normal.line-hover::text").get(), # CSS selector for the category name
                "category_url": "https://kemitt.com" + cat.css("a::attr(href)").get(), # CSS selector for the category URL. Append kemitt.com to the selector to get the complete URL
                "response_url_home_page": remove_tags(response.headers["Sa-Final-Url"]) # The response URL
            }
