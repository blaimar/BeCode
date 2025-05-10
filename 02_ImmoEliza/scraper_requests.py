import requests, json, time, re
from bs4 import BeautifulSoup

# -------------------------------------------------------------------- Initialisations of all variables
START_TIME = time.time()
informations_to_write_on_csv = []
headers_of_browser = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'}
dic_columns_name = {'property_ID':['id'],'locality_name':['property','location','locality'],'postal_code':['property','location','postalCode'],'price':['transaction','sale','price'],'type_of_property':['property', 'type'],'subtype_of_property':['property', 'subtype'],'type_of_sale':['transaction', 'subtype'],'number_of_rooms':['property','bedroomCount'],'living_area':['property','netHabitableSurface'],'equipped_kitchen':['property','kitchen','type'],'furnished':['transaction', 'isFurnished'],'open_fire':['property','fireplaceExists'],'terrace':['property','hasTerrace'],'garden':['property','hasGarden'],'surface_of_good':['property','netHabitableSurface'],'number_of_facades':['property','building','facadeCount'],'swimming_pool':['property','hasSwimmingPool'],'state_of_building':['property','building','condition'],'PBE':['transaction','certificates','epcScore'],'energy_consumption':['transaction','certificates','primaryEnergyConsumptionYearly'],'balcony':['property','hasBalcony']}
# -----------------------------------------------------------------------------------------------------

def take_path_from_dict_return_value(arguments : list, dict_json_file : dict):
    """
    Function that get the path in the json and return the value

    arguments : A list of arguments to access the value on the dict_json_file
    dict_json_file : A dictionnary of all informations about the current link 
    """
    try:
        for key in arguments:
            dict_json_file = dict_json_file[key]
        return str(dict_json_file).lower()
    except (KeyError, TypeError):
        return "None"

def take_response_return_dict(response): #type?
    """
    Function that takes the response of a get requests and return the json content in the page and the soup
    
    response : type? of the current immoweb page
    """
    soup = BeautifulSoup(response.content, "lxml")
    pattern = re.search(r"window\.classified\s*=\s*({.*?});\s*\n", response.text, re.DOTALL)
    if pattern:
        dict_all_informations_still_string = json.loads(pattern.group(1))
    else:
        print(f"Error with url : {response.url}")
    return dict_all_informations_still_string, soup

def parse_json_from_url(url: str, path_csv: str):
    """
    Function that takes an url from the main_loop_on_all_pages (search: "house for sale) OR by itself if
    this function get a link that content a "house_group". It will call the function take_response_return_dict
    to transform the page on a json file and then will call the function take_path_from_dict_return_value for 
    all the value that we want to get.

    url : A string that is the url of a immoweb page of a house / appartement / house_group
    """
    response = requests.get(url, headers=headers_of_browser)
    # Check if it's a valid requests 
    if response.status_code == 200 :
        dict_all_informations, soup = take_response_return_dict(response)
        # if it's a house_group : we get all links of appartement / house to parse them
        if take_path_from_dict_return_value(['property', 'type'],dict_all_informations) == "house_group" :
            for e in soup.select('ul.classified__list--striped li.classified__list-item-link a[href]'):
                parse_json_from_url(e["href"], path_csv)
            return
        # from the current_url, we call take_path_from_dict_return_value to get all values
        informations_current_url = []
        for key in dic_columns_name:
            informations_current_url.append(take_path_from_dict_return_value(dic_columns_name[key],dict_all_informations))
        # then we write them on the csv (maybe better to )
        with open(f'{path_csv}raw_data.csv', 'a', encoding='UTF-8') as f: f.write("\n"+','.join(informations_current_url))
    else:
        print(response.status_code,":",url)


def main(path_csv: str):
    """
    Function that navigates in all immoweb page from the search "house for sale". On each links found,
    it will call the function parse_json_from_url to parse json
    """
    with open(f'{path_csv}raw_data.csv','r') as f: START_LINE = sum(1 for _ in f)
    for i in range(1,6):
        url = "https://www.immoweb.be/en/search/house/for-sale?countries=BE&page="+str(i)+"&orderBy=relevance"
        response = requests.get(url, headers=headers_of_browser)
        soup = BeautifulSoup(response.content, "lxml")
        # For each link on the page, call the function to parse informations from the json
        for e in soup.find_all(class_="card__title-link"):
            parse_json_from_url(e["href"], path_csv)
        
    # -------------------------------------------------------------------- To print informations time & line
    END_TIME = time.time()
    with open(f'{path_csv}raw_data.csv','r') as f: END_LINE = sum(1 for _ in f)
    TIME = END_TIME - START_TIME
    LINE = max(1,END_LINE - START_LINE)
    print(f"\nTime : {TIME:.2f} sec for {LINE} lines")
    print(f"=> {TIME/LINE:.3f} seconds per line")
    print(f"=> {LINE/TIME:.1f} lines per second")
    # -----------------------------------------------------------------------------------------------------

if __name__ == "__main__" :
    main("../data/")