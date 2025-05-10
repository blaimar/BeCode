from os import path, rename

def create_csv(path_csv: str):
    columns_name = ['property_ID','locality_name','postal_code','price','type_of_property','subtype_of_property','type_of_sale','number_of_rooms','living_area','equipped_kitchen','furnished','open_fire','terrace','garden','surface_of_good','number_of_facades','swimming_pool','state_of_building','PBE','energy_consumption']
    with open(f"{path_csv}/raw_data.csv", 'w+') as f : f.write(','.join(columns_name))

def main(path_csv: str):
    if path.isfile(path_csv+"raw_data.csv"):
        i=1
        while True:
            if not path.isfile(f"{path_csv}/raw_data_{i}.csv"):
                rename(f"{path_csv}/raw_data.csv", f"{path_csv}/raw_data_{i}.csv")
                break
            i+=1
    create_csv(path_csv)




if __name__ == "__main__":
    main("../data/")


