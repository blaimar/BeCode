import scrapy, re, json, time
from scrapy.crawler import CrawlerProcess
from os import path, makedirs

dic_columns = {'id':['id'],
                'flags_isPublicSale': ['flags', 'isPublicSale'],
                'flags_isNewClassified': ['flags', 'isNewClassified'],
                'flags_isNewPrice': ['flags', 'isNewPrice'],
                'flags_isInvestmentProject': ['flags', 'isInvestmentProject'],
                'flags_isNewlyBuilt': ['flags', 'isNewlyBuilt'],
                'flags_isNotarySale': ['flags', 'isNotarySale'],
                'flags_isLifeAnnuitySale': ['flags', 'isLifeAnnuitySale'],
                'flags_adQuality': ['flags', 'adQuality'],
                'flags_date': ['flags', 'date'],
                'flags_priceSqm': ['flags', 'priceSqm'],
                'flags_price': ['flags', 'price'],
                'flags_default': ['flags', 'default'],
                'flags_isSoldOrRented': ['flags', 'isSoldOrRented'],
                'flags_isLowEnergy': ['flags', 'isLowEnergy'],
                'flags_percentSold': ['flags', 'percentSold'],
                'flags_isPassiveHouse': ['flags', 'isPassiveHouse'],
                'flags_isNewRealEstateProject': ['flags', 'isNewRealEstateProject'],
                'flags_isAnInteractiveSale': ['flags', 'isAnInteractiveSale'],
                'flags_isUnderOption': ['flags', 'isUnderOption'],
                'property_type': ['property', 'type'],
                'property_subtype': ['property', 'subtype'],
                'property_bedroomCount': ['property', 'bedroomCount'],
                'property_bedrooms': ['property', 'bedrooms'],
                'property_bathroomCount': ['property', 'bathroomCount'],
                'property_bathrooms': ['property', 'bathrooms'],
                'property_location_postalCode': ['property', 'location', 'postalCode'],
                'property_location_floor': ['property', 'location', 'floor'],
                'property_location_latitude': ['property', 'location', 'latitude'],
                'property_location_longitude': ['property', 'location', 'longitude'],
                'property_isHolidayProperty': ['property', 'isHolidayProperty'],
                'property_location_type': ['property', 'location', 'type'],
                'property_location_hasSeaView': ['property', 'location', 'hasSeaView'],
                'property_location_pointsOfInterest': ['property', 'location', 'pointsOfInterest'],
                'property_netHabitableSurface': ['property', 'netHabitableSurface'],
                'property_roomCount': ['property', 'roomCount'],
                'property_attic': ['property', 'attic'],
                'property_hasAttic': ['property', 'hasAttic'],
                'property_basement_surface': ['property', 'basement', 'surface'],
                'property_hasBasement': ['property', 'hasBasement'],
                'property_hasDressingRoom': ['property', 'hasDressingRoom'],
                'property_diningRoom': ['property', 'diningRoom'],
                'property_hasDiningRoom': ['property', 'hasDiningRoom'],
                'property_building_annexCount': ['property', 'building', 'annexCount'],
                'property_building_condition': ['property', 'building', 'condition'],
                'property_building_constructionYear': ['property', 'building', 'constructionYear'],
                'property_building_facadeCount': ['property', 'building', 'facadeCount'],
                'property_building_floorCount': ['property', 'building', 'floorCount'],
                'property_building_streetFacadeWidth': ['property', 'building', 'streetFacadeWidth'],
                'property_propertyCertificates_builtPlanStatus': ['property', 'propertyCertificates', 'builtPlanStatus'],
                'property_propertyCertificates_globalInsulationLevel': ['property', 'propertyCertificates', 'globalInsulationLevel'],
                'property_propertyCertificates_oilTankCertificateStatus': ['property', 'propertyCertificates', 'oilTankCertificateStatus'],
                'property_propertyCertificates_primaryEnergyConsumptionLevel': ['property', 'propertyCertificates', 'primaryEnergyConsumptionLevel'],
                'property_propertyCertificates_hasAsbestosCertificate': ['property', 'propertyCertificates', 'hasAsbestosCertificate'],
                'property_propertyCertificates_hasElectricalInstallationComplianceCertificate': ['property', 'propertyCertificates', 'hasElectricalInstallationComplianceCertificate'],
                'property_hasCaretakerOrConcierge': ['property', 'hasCaretakerOrConcierge'],
                'property_hasDisabledAccess': ['property', 'hasDisabledAccess'],
                'property_hasLift': ['property', 'hasLift'],
                'property_constructionPermit_constructionType': ['property', 'constructionPermit', 'constructionType'],
                'property_constructionPermit_floodZoneType': ['property', 'constructionPermit', 'floodZoneType'],
                'property_constructionPermit_isObtained': ['property', 'constructionPermit', 'isObtained'],
                'property_constructionPermit_hasObligationToConstruct': ['property', 'constructionPermit', 'hasObligationToConstruct'],
                'property_constructionPermit_hasPlotDivisionAuthorization': ['property', 'constructionPermit', 'hasPlotDivisionAuthorization'],
                'property_constructionPermit_hasPossiblePriorityPurchaseRight': ['property', 'constructionPermit', 'hasPossiblePriorityPurchaseRight'],
                'property_constructionPermit_isBreachingUrbanPlanningRegulation': ['property', 'constructionPermit', 'isBreachingUrbanPlanningRegulation'],
                'property_constructionPermit_floodZoneIconUrl': ['property', 'constructionPermit', 'floodZoneIconUrl'],
                'property_constructionPermit_totalBuildableGroundFloorSurface': ['property', 'constructionPermit', 'totalBuildableGroundFloorSurface'],
                'property_constructionPermit_urbanPlanningInformation': ['property', 'constructionPermit', 'urbanPlanningInformation'],
                'property_constructionPermit_pScore': ['property', 'constructionPermit', 'pScore'],
                'property_constructionPermit_gScore': ['property', 'constructionPermit', 'gScore'],
                'property_energy_heatingType': ['property', 'energy', 'heatingType'],
                'property_energy_hasHeatPump': ['property', 'energy', 'hasHeatPump'],
                'property_energy_hasPhotovoltaicPanels': ['property', 'energy', 'hasPhotovoltaicPanels'],
                'property_energy_hasThermicPanels': ['property', 'energy', 'hasThermicPanels'],
                'property_energy_hasCollectiveWaterHeater': ['property', 'energy', 'hasCollectiveWaterHeater'],
                'property_energy_hasDoubleGlazing': ['property', 'energy', 'hasDoubleGlazing'],
                'property_energy_performance': ['property', 'energy', 'performance'],
                'property_kitchen_surface': ['property', 'kitchen', 'surface'],
                'property_kitchen_type': ['property', 'kitchen', 'type'],
                'property_kitchen_hasOven': ['property', 'kitchen', 'hasOven'],
                'property_kitchen_hasMicroWaveOven': ['property', 'kitchen', 'hasMicroWaveOven'],
                'property_kitchen_hasDishwasher': ['property', 'kitchen', 'hasDishwasher'],
                'property_kitchen_hasWashingMachine': ['property', 'kitchen', 'hasWashingMachine'],
                'property_kitchen_hasFridge': ['property', 'kitchen', 'hasFridge'],
                'property_kitchen_hasFreezer': ['property', 'kitchen', 'hasFreezer'],
                'property_kitchen_hasSteamOven': ['property', 'kitchen', 'hasSteamOven'],
                'property_land': ['property', 'land'],
                'property_laundryRoom': ['property', 'laundryRoom'],
                'property_hasLaundryRoom': ['property', 'hasLaundryRoom'],
                'property_livingRoom': ['property', 'livingRoom'],
                'property_hasLivingRoom': ['property', 'hasLivingRoom'],
                'property_hasBalcony': ['property', 'hasBalcony'],
                'property_hasBarbecue': ['property', 'hasBarbecue'],
                'property_hasGarden': ['property', 'hasGarden'],
                'property_gardenSurface': ['property', 'gardenSurface'],
                'property_gardenOrientation': ['property', 'gardenOrientation'],
                'property_parkingCountIndoor': ['property', 'parkingCountIndoor'],
                'property_parkingCountOutdoor': ['property', 'parkingCountOutdoor'],
                'property_parkingCountClosedBox': ['property', 'parkingCountClosedBox'],
                'property_hasAirConditioning': ['property', 'hasAirConditioning'],
                'property_hasArmoredDoor': ['property', 'hasArmoredDoor'],
                'property_hasVisiophone': ['property', 'hasVisiophone'],
                'property_hasSecureAccessAlarm': ['property', 'hasSecureAccessAlarm'],
                'property_hasCableTV': ['property', 'hasCableTV'],
                'property_hasDoorPhone': ['property', 'hasDoorPhone'],
                'property_hasInternet': ['property', 'hasInternet'],
                'property_showerRoomCount': ['property', 'showerRoomCount'],
                'property_showerRooms': ['property', 'showerRooms'],
                'property_specificities_hasOffice': ['property', 'specificities', 'hasOffice'],
                'property_specificities_office': ['property', 'specificities', 'office'],
                'property_specificities_hasWorkspace': ['property', 'specificities', 'hasWorkspace'],
                'property_specificities_workspace': ['property', 'specificities', 'workspace'],
                'property_toiletCount': ['property', 'toiletCount'],
                'property_toilets': ['property', 'toilets'],
                'property_hasFitnessRoom': ['property', 'hasFitnessRoom'],
                'property_hasTennisCourt': ['property', 'hasTennisCourt'],
                'property_hasSwimmingPool': ['property', 'hasSwimmingPool'],
                'property_hasSauna': ['property', 'hasSauna'],
                'property_hasJacuzzi': ['property', 'hasJacuzzi'],
                'property_hasHammam': ['property', 'hasHammam'],
                'property_bedroomSurface': ['property', 'bedroomSurface'],
                'property_habitableUnitCount': ['property', 'habitableUnitCount'],
                'property_hasTerrace': ['property', 'hasTerrace'],
                'property_terraceSurface': ['property', 'terraceSurface'],
                'property_terraceOrientation': ['property', 'terraceOrientation'],
                'transaction_type': ['transaction', 'type'],
                'transaction_subtype': ['transaction', 'subtype'],
                'transaction_certificates_carbonEmission': ['transaction', 'certificates', 'carbonEmission'],
                'transaction_certificates_primaryEnergyConsumptionPerSqm': ['transaction', 'certificates', 'primaryEnergyConsumptionPerSqm'],
                'transaction_certificates_primaryEnergyConsumptionYearly': ['transaction', 'certificates', 'primaryEnergyConsumptionYearly'],
                'transaction_certificates_epcReference': ['transaction', 'certificates', 'epcReference'],
                'transaction_certificates_epcScore': ['transaction', 'certificates', 'epcScore'],
                'transaction_certificates_renovationObligation': ['transaction', 'certificates', 'renovationObligation'],
                'transaction_rental': ['transaction', 'rental'],
                'transaction_sale_price': ['transaction', 'sale', 'price'],
                'transaction_sale_cadastralIncome': ['transaction', 'sale', 'cadastralIncome'],
                'transaction_sale_publicSale': ['transaction', 'sale', 'publicSale'],
                'transaction_sale_pricePerSqm': ['transaction', 'sale', 'pricePerSqm'],
                'transaction_sale_isFurnished': ['transaction', 'sale', 'isFurnished'],
                'transaction_sale_homeToBuild': ['transaction', 'sale', 'homeToBuild'],
                }

class QuotesSpider(scrapy.Spider):
    name = "immoweb"

    dic_columns_name = dic_columns
    start_urls = [f"https://www.immoweb.be/en/search/{TYPE}/for-sale/abc/{LOCALITY}?countries=BE&page=333&orderBy=relevance" for LOCALITY in range(1000,9999+1) for TYPE in ["apartment","house"]]
    
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
                yield {key: self.take_path_from_dict_return_value(path,dict_all_informations) for key, path in self.dic_columns_name.items()}
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

def create_csv(): 
    makedirs(path.join('..', 'data'), exist_ok=True)
    columns_name = list(dic_columns.keys())
    with open(path.join('..', 'data', 'durty_data.csv'), 'w+') as f:
        f.write(','.join(columns_name))    

def main(process):
    START_TIME, START_LINE = time.time(), 1
    process.start()
    END_TIME = time.time()
    with open(path.join('..', 'data', 'durty_data.csv'), 'r', encoding='utf-8') as f: END_LINE = sum(1 for _ in f)
    TIME = END_TIME - START_TIME
    LINE = max(1, END_LINE - START_LINE)
    print(f"\nTime : {TIME:.2f} sec for {LINE} lines")
    print(f"=> {TIME/LINE:.3f} seconds per line")
    print(f"=> {LINE/TIME:.1f} lines per second")


if __name__ == "__main__":
    process = CrawlerProcess(settings={
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'LOG_ENABLED': False,
        'FEEDS': {
            path.join('..', 'data', 'durty_data.csv'): {'format': 'csv', 'overwrite': True}
        },
        'DOWNLOAD_DELAY': 0.01,
        'CONCURRENT_REQUESTS': 400
    })
    create_csv()
    print(f"Program started at : {time.strftime("%H:%M:%S")}")
    process.crawl(QuotesSpider)
    main(process)