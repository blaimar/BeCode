import scrapy, re, json, time
from scrapy.crawler import CrawlerProcess

class QuotesSpider(scrapy.Spider):
    name = "immoweb"

    dic_columns_name = {'property_ID':['id'],
                        'locality_name':['property','location','locality'],
                        'postal_code':['property','location','postalCode'],
                        'price':['transaction','sale','price'],
                        'type_of_property':['property', 'type'],
                        'subtype_of_property':['property', 'subtype'],
                        'type_of_sale':['transaction', 'subtype'],
                        'number_of_rooms':['property','bedroomCount'],
                        'living_area':['property','netHabitableSurface'],
                        'equipped_kitchen':['property','kitchen','type'],
                        'furnished':['transaction', 'isFurnished'],
                        'open_fire':['property','fireplaceExists'],
                        'terrace':['property','hasTerrace'],
                        'garden':['property','hasGarden'],
                        'surface_of_good':['property','land','surface'],
                        'number_of_facades':['property','building','facadeCount'],
                        'swimming_pool':['property','hasSwimmingPool'],
                        'state_of_building':['property','building','condition'],
                        'PBE':['transaction','certificates','epcScore'],
                        'energy_consumption':['transaction','certificates','primaryEnergyConsumptionYearly']
                        
                        }
    start_urls = [f"https://www.immoweb.be/en/search/{TYPE}/for-sale/abc/{LOCALITY}?countries=BE&page=333&orderBy=relevance" for LOCALITY in range(1000,1020+1) for TYPE in ["apartment","house"]]
    
    def parse(self,response):
        """
        Find all the correct page
        """
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
                yield pattern.group(1)
                #yield {key: self.take_path_from_dict_return_value(path,dict_all_informations) for key, path in self.dic_columns_name.items()}
        else:
            print(f"Error with {response.url} => {response.url}")

    def take_path_from_dict_return_value(self, arguments : list, dict_json_file : dict):
        """
        Function that get the path in the json and return the value

        arguments : A list of arguments to access the value on the dict_json_file
        dict_json_file : A dictionnary of all informations about the current link
        """
        
        try:
            value = dict_json_file
            for key in arguments:
                value = value[key]
            return str(value).lower()
        except:
            return "None"

def main(path_csv: str, process):
    START_TIME = time.time()
    START_LINE = 1
    process.start()
    END_TIME = time.time()
    with open(f'{path_csv}raw_data.csv','r') as f: END_LINE = sum(1 for _ in f)
    TIME = END_TIME - START_TIME
    LINE = max(1,END_LINE - START_LINE)
    print(f"\nTime : {TIME:.2f} sec for {LINE} lines")
    print(f"=> {TIME/LINE:.3f} seconds per line")
    print(f"=> {LINE/TIME:.1f} lines per second")


if __name__ == "__main__":
    process = CrawlerProcess(settings = {'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                    'LOG_ENABLED': False,
                    'FEEDS': {'../data/raw_data.csv': {'format': 'csv','overwrite': True}},
                    'DOWNLOAD_DELAY': 0.01,
                    'CONCURRENT_REQUESTS': 400})
    process.crawl(QuotesSpider)
    main("../data/", process)