import scrapy, re, json, time
from scrapy.crawler import CrawlerProcess

class QuotesSpider(scrapy.Spider):
    name = "immoweb"
    r = range(1000,1011+1)
    start_urls = [f"https://www.immoweb.be/en/search/{TYPE}/for-sale/abc/{LOCALITY}?countries=BE&page=333&orderBy=relevance" for LOCALITY in r for TYPE in ["apartment","house"]]
    
    def parse(self,response):
        verification = response.css(".title--2::text").get()
        if "10000 properties" not in verification:
            properties = int(verification.split(" - ")[-1].split(" ")[0])
            last_page = (properties//30 if properties%30 == 0 else properties//30+1)
            for i in range(1,last_page+1):
                url = response.url.replace("page=333",f"page={i}")
                yield scrapy.Request(url, callback=self.get_url_from_correct_page)

    def get_url_from_correct_page(self, response):
        for url in response.css(".card__title-link::attr(href)").getall():
            yield scrapy.Request(url, callback=self.parse_specific_url)

    def parse_specific_url(self, response):
        pattern = re.search(r"window\.classified\s*=\s*({.*?});\s*\n", response.text, re.DOTALL)
        if pattern:
            dict_all_informations = json.loads(pattern.group(1))
            if dict_all_informations['property']['type'].lower() in ("house_group", "apartment_group"):
                for url in response.css("li.classified__list-item-link a::attr(href)").getall():
                    yield scrapy.Request(url, callback=self.parse_specific_url)
            else:
                yield json.loads(pattern.group(1))
        else:
            print(f"Error with {response.url} => {response.url}")

def main(process):
    process.start()

if __name__ == "__main__":
    process = CrawlerProcess(settings={
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'LOG_ENABLED': False,
        'FEEDS': {
            'all_data_in_json.csv': {'format': 'csv', 'overwrite': True}
        },
        'DOWNLOAD_DELAY': 0.01,
        'CONCURRENT_REQUESTS': 400
    })
    process.crawl(QuotesSpider)
    main(process)