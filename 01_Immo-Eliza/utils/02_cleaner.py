import pandas as pd 
import numpy as np
import warnings, ast, re
from pandas.errors import DtypeWarning
from os import path 


warnings.simplefilter(action='ignore', category=(DtypeWarning,FutureWarning))
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

df = pd.read_csv(path.join(path.dirname(__file__),'..', 'data', 'durty_data.csv'))

# drop duplicates propreties
df = df.drop_duplicates(subset="id", keep="first")

# Split column that has dictionnary into multiples columns
def to_dict(x):
    if isinstance(x, dict):
        return x
    if pd.isna(x):
        return {}
    if isinstance(x, str):
        x = re.sub(r'\bfalse\b', 'False', x, flags=re.IGNORECASE)
        x = re.sub(r'\btrue\b', 'True', x, flags=re.IGNORECASE)
        x = re.sub(r'\bnone\b', 'None', x, flags=re.IGNORECASE)
        try:
            return ast.literal_eval(x)
        except (ValueError, SyntaxError):
            return {}
    return {}

for col in ('property_land','property_specificities_workspace', 'property_specificities_office', 'property_livingRoom', 'property_attic'):
    df[col] = df[col].apply(to_dict)
    land_details = pd.json_normalize(df[col]).add_prefix(col + '_')
    df = df.drop(columns=[col]).join(land_details)

# drop row that has only one unique value
df = df.loc[:, df.apply(lambda col: col.nunique(dropna=False) > 1)]

# only nan and none
df = df.drop(columns=["property_propertyCertificates_globalInsulationLevel", "property_energy_performance", "property_energy_performance",
                      "property_kitchen_hasMicroWaveOven","property_kitchen_hasDishwasher", "property_kitchen_hasWashingMachine",
                      "property_kitchen_hasFridge", "property_kitchen_hasFreezer", "property_kitchen_hasSteamOven", "property_laundryRoom"])

# drop columns that looks useless or unexploitable
df = df.drop(columns=["flags_isPublicSale","flags_isSoldOrRented","property_bedrooms","property_building_streetFacadeWidth",
                      "property_constructionPermit_floodZoneType","property_constructionPermit_urbanPlanningInformation",
                      "transaction_certificates_epcReference","transaction_sale_publicSale", "transaction_sale_pricePerSqm",
                      "flags_percentSold", "property_diningRoom", "property_constructionPermit_totalBuildableGroundFloorSurface",
                      "property_land_latestusedesignation", "property_land_landwidth", "property_building_streetFacadeWidth"])

# Delete rows where type is not house or apartment and rename column in "property_type_isHouse"
df = df[df["property_type"].isin(("house","apartment"))]
df = df.rename(columns={'property_type': 'property_type_isHouse'})
df['property_type_isHouse'] = df['property_type_isHouse'].replace({"house":1,"apartment":0}).astype(bool)

# Delete rows where 'property_location_postalCode' not on 4 
df = df[(df['property_location_postalCode'] >= 1000) & (df['property_location_postalCode'] <= 9999)]

# Delete rows where transaction_subtype is not regular
df = df[df["transaction_subtype"] == "buy_regular"]
df = df.drop(columns=["transaction_subtype"])

# Normalize bool column
bool_columns : list[str] = [col for col in list(df) if "_has" in col or "_is" in col] + ['transaction_certificates_renovationObligation', 'property_propertyCertificates_builtPlanStatus', 'property_propertyCertificates_oilTankCertificateStatus']
df[bool_columns] = df[bool_columns].replace({"False":0,"false":0,"no":0,"yes_not_conform":0,
                                             "True":1,"true":1,"yes":1,"yes_conform":1,
                                             "none":-1,"NaN":-1,"nan":-1,"not_specified":-1}).fillna(-1).astype(int)
df = df.copy()

# Normalize int column
int_columns : list[str] = ["property_basement_surface", "property_bathroomCount", "property_showerRoomCount", "property_roomCount", "property_building_constructionYear",
                            "property_bedroomCount","property_parkingCountOutdoor", "property_building_floorCount", "property_attic_surface",
                            "property_building_annexCount", "property_kitchen_surface","property_toiletCount", "property_building_facadeCount", 
                            "property_parkingCountIndoor", "transaction_certificates_primaryEnergyConsumptionPerSqm", "property_netHabitableSurface", 
                            "transaction_sale_cadastralIncome", "transaction_certificates_carbonEmission", "property_land_surface", "property_location_floor", 
                            "property_specificities_office_surface", "property_livingRoom_surface", "property_specificities_workspace_surface", 
                            'transaction_certificates_primaryEnergyConsumptionYearly', "property_propertyCertificates_primaryEnergyConsumptionLevel",
                            'property_gardenSurface', 'property_terraceSurface', 'transaction_sale_price']
for col in ('transaction_certificates_primaryEnergyConsumptionYearly', "property_propertyCertificates_primaryEnergyConsumptionLevel", "property_location_floor"):
    df[col] = np.where("-" in df[col], "-1", df[col])
df[int_columns] = df[int_columns].fillna(-1).replace({"none":-1}).astype(int)
df["property_land_surface"] = df["property_land_surface"].replace({0:-1})
df = df.copy()

# Normalize float column
float_columns : list[str] = ['property_location_latitude', 'property_location_longitude']
df[float_columns] = df[float_columns].fillna(-1).replace({"none":-1}).astype(float)
df = df.copy()

# Gradute EPC
col = 'transaction_certificates_epcScore'
df[col] = df[col].apply(lambda val: "-1" if val not in ['a++', 'a+', 'a', 'b', 'c', 'd', 'e', 'f', 'g'] or pd.isna(val) else val)
df['region'] = df['property_location_postalCode'].apply(lambda postalcode: 'Bruxelles' if 1000 <= int(postalcode) <= 1299 else ('Wallonie' if 1300 <= int(postalcode) <= 1499 or 4000 <= int(postalcode) <= 7999 else 'Flandre'))
def apply_epc_score(row):
    region, epc_score = row['region'], row['transaction_certificates_epcScore']
    epc_scales = {'Wallonie': {"-1": -1, 'a++': 0, 'a+': 45, 'a': 45, 'b': 95, 'c': 150, 'd': 210, 'e': 275, 'f': 345, 'g': 345},
        'Flandre': {"-1": -1, 'a++': 0, 'a+': 100, 'a': 200, 'b': 300, 'c': 400, 'd': 500, 'e': 500, 'f': 500, 'g': 500},
        'Bruxelles': {"-1": -1, 'a++': 0, 'a+': 45, 'a': 85, 'b': 170, 'c': 255, 'd': 340, 'e': 425, 'f': 510, 'g': 510}}
    for score, threshold in epc_scales[region].items():
        if epc_score.lower() == score:
            return threshold
df['transaction_certificates_epcScore'] = df.apply(apply_epc_score, axis=1).astype(int)
df = df.drop(columns=['region'])

# Normalize string column
str_columns : list[str] = ['property_kitchen_type','property_land_sewerconnection','property_energy_heatingType','property_constructionPermit_constructionType', 'property_subtype', 'property_location_type']
df[str_columns] = df[str_columns].replace({"none":-1,"NaN":-1,"nan":-1,"not_specified":-1}).fillna(-1).astype(str)

# Gradute some columns
df['property_building_condition'] = df['property_building_condition'].replace({"to_restore": 0,"to_be_done_up": 1,"to_renovate": 2,"just_renovated": 3,"good": 4,"as_new": 5,"none": -1}).fillna(-1).astype(int)
df['property_constructionPermit_gScore'] = df['property_constructionPermit_gScore'].replace({"none":-1,"d":0,"c":1,"b":2,"a":3}).fillna(-1).astype(int)
df['property_constructionPermit_pScore'] = df['property_constructionPermit_pScore'].replace({"none":-1,"d":0,"c":1,"b":2,"a":3}).fillna(-1).astype(int)
df = df.copy()

# Gradute orientation
direction_to_angle = {'north': 0,'north_east': 45,'east': 90,'south_east': 135,'south': 180,'south_west': 225,'west': 270,'north_west': 315,'none': -1}
df['garden_angle'] = df['property_gardenOrientation'].map(direction_to_angle)
df['terrace_angle'] = df['property_terraceOrientation'].map(direction_to_angle)
df['terrace_cos'] = np.cos(np.radians(df['terrace_angle']))
df['garden_cos'] = np.cos(np.radians(df['garden_angle']))
df = df.drop(columns=['property_terraceOrientation', 'property_gardenOrientation', 'garden_angle', 'terrace_angle'])
df = df.copy()

df.to_csv(path.join(path.dirname(__file__),'..', 'data', 'cleaned_data.csv'))