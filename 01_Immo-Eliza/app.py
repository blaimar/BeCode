import pandas as pd 
import numpy as np 
import streamlit as st 
import seaborn as sns
import glob, joblib, tempfile, io
from os import path, unlink

st.set_page_config(layout="wide")

# ========== CONFIGURATION ==========
pages = ["The project",f"1\u200B. Web Scraping",f"2\u200B. Data Cleaning",f"3\u200B. Machine Learning",f"4\u200B. Web Application"]
# =================================== 

# ============= SIDEBAR =============
with st.sidebar:
    st.title("Summary")
    page = st.radio("Navigated", pages, label_visibility="collapsed")
    st.markdown("<div style='height:60vh;'></div>", unsafe_allow_html=True)
    dark_mode = st.checkbox("🌙 Mode sombre")

if dark_mode:st.markdown("""<style>.stApp {background-color: #0e1117;color: white;}</style>""", unsafe_allow_html=True)
else:st.markdown("""<style>.stApp {background-color: white;color: black;}</style>""", unsafe_allow_html=True)
# =================================== 

# ============== STYLE ==============
st.markdown("""<style>.st-emotion-cache-7czcpc > img {border-radius: 0 !important;}</style>""", unsafe_allow_html=True)
# =================================== 

@st.cache_data(show_spinner=True)
def load_model_from_parts(directory="data", pattern="model_*.pkl"):
    parts = sorted(glob.glob(path.join(directory, pattern)))
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        for part in parts:
            with open(part, "rb") as f:
                tmpfile.write(f.read())
        tmpfile.flush()
        model = joblib.load(tmpfile.name)
    return model

if page == pages[0] :
    st.write("### The project")
    st.write("As part of my Data Scientist training at [Becode](https://becode.org/fr/), I worked on the following end-to-end project:")
    st.write("**Project: Real Estate Price Prediction for Immo Eliza**")
    st.write("The real estate company Immo Eliza aimed to develop a machine learning model to predict property prices across Belgium. They brought me on to design and implement the entire data pipeline.")
    st.write("The project was structured over four weeks:")
    st.markdown('<span style="text-decoration: underline;">1. Web Scraping</span>',unsafe_allow_html=True)   
    st.write("I built a dataset by scraping information on over 10,000 properties from various real estate platforms across Belgium. This data would later serve as the foundation for training the predictive model.")
    st.markdown('<span style="text-decoration: underline;">2. Data Cleaning</span>',unsafe_allow_html=True)   
    st.write("I conducted data preprocessing and exploratory data analysis to extract key insights and identify data quality issues. Since the company had no in-house data team, they relied on external talent for these crucial steps.")
    st.markdown('<span style="text-decoration: underline;">3. Machine Learning</span>',unsafe_allow_html=True)   
    st.write("With the cleaned and structured dataset, I developed and evaluated regression models to predict real estate prices. The primary evaluation metric was Mean Absolute Error (MAE), which I carefully monitored and optimized.")
    st.markdown('<span style="text-decoration: underline;">4. Web Application</span>',unsafe_allow_html=True)   
    st.write("Finally, I built an interactive web application to make the model accessible to end-users. This tool allows users to input property features and receive an estimated price prediction.")

elif len(pages) > 1 and page == pages[1] :
    # ============ LOADING DF ===========
    all_durty_csv = glob.glob(path.join("data","durty_data_*.csv"))
    all_durty_csv_size = path.getsize(path.join("data","durty_data.csv"))
    durty_df=pd.concat([pd.read_csv(f, low_memory=False) for f in all_durty_csv], ignore_index=True)
    # =================================== 
    st.write(f"### Web scraping")
    st.write("To scrape the data, I used Scrapy, a fast asynchronous library.")
    st.write("I realized that when performing very broad searches, ImmoWeb only displays up to 333 pages for 10,000 properties.")
    st.image("data/img/10000_max_properties.png")
    st.write("Even if we need only 10,000 properties, I want to get as much as possible. Therefore, I needed to perform many smaller, targeted searches to ensure that I collected all available properties.")
    st.write("I also discovered that each property page contains a JSON file with the relevant data.")
    st.image("data/img/json_in_page.png")
    st.write("With this information, I was ready to start the scraping process. Below is a brief summary:")
    st.code("class QuotesSpider(scrapy.Spider):\n"
            "   start_urls = (house/apartment) in range (1000,10000)\n"
            "   columns_to_get = list(columns_names)\n"
            "   def parse(self,response):\n"
            "      if '10000 properties' not in page:\n"
            "         loop in range(1, last_page): \n"
            "            yield parse_all_propreties()\n"
            "   def parse_all_propreties(self, response): \n"
            "      loop in propreties_in_page:\n"
            "         yield parse_specific_url() \n"
            "   def parse_specific_url(self,response): \n"
            "      if jason in file: \n"
            "         if ('house_group' or 'apartment_group') in page:\n"
            "            for propreties in group: \n"
            "               yield parse_specific_url()\n"
            "         else: \n"
            "            loop columns in column_to_get:\n"
            "               yield take_path_from_dict_return_value(columns)\n"
            "      else: \n"
            "         print('json not in file') #This was never called\n"
            "   def take_path_from_dict_return_value(self,columns): \n"
            "      return columns_value \n"
            )
    
    st.write("And I choose columns with that:")
    # INSERT PART FOR THE COLUMNS HERE
    st.write("# INSERT PART FOR THE COLUMNS HERE !!!")

    st.write("First, I created the `start_urls` list containing all the URLs for houses and apartments across all postal codes from 1000 to 9999.")
    st.write("The initial function sends requests to each URL defined in `start_urls`.")
    st.write("If there are no properties for a given postal code, the website does not return an error but instead displays a default value of 10,000 properties.")
    st.write("Therefore, if the response indicates 10,000 properties, I assume there are no listings for that postal code.")
    st.write("The second function then sends requests for every available page of results corresponding to each postal code.")
    st.write("Subsequently, another function requests the details for each individual property listed on those pages.")
    st.write("Finally, for every property, we extract the necessary information from the embedded JSON file, field by field.")
    st.write(f"The entire scraping process took approximately 1 hour and 40 minutes to collect a dataset with shape `{durty_df.shape}`, resulting in a file size of `{all_durty_csv_size // 1_000_000} MB`.")
    st.write("One additional challenge was the upload size limit of 25 MB per file.")
    st.write("To overcome this, I developed a small utility script named `split_csv_too_large.py` located in the `small_prog/` directory, which splits the large CSV into smaller files that meet the upload requirements.")

elif len(pages) > 2 and page == pages[2] :
    st.write(f"### Data cleaning")
    st.write("text 3")

elif len(pages) > 3 and page == pages[3] :
    st.write(f"### {page}")
    st.write("text 4")

elif len(pages) > 4 and page == pages[4] :
    user_data = {}
    with st.container():
        spacer, col1, spacer, col2, spacer = st.columns([0.5, 1, 0.5, 1, 0.5])

        with col1:
            user_data["property_type_isHouse"] = 1 if st.selectbox("Property type", ["apartment", "house"], index=1) == "house" else 0

        with col2:
            property_subtype_choice = ["unknwon", "apartment", "apartment block", "bungalow", "castle", "chalet", "country cottage", "duplex", "exceptional property", "farmhouse", 
                                    "flat studio", "ground floor", "house", "kot", "loft", "manor house", "mansion", "mixed use building", "pavilion", "penthouse", 
                                    "service flat", "town house", "triplex", "villa", "other property"]
            user_data["property_subtype_"] = st.selectbox("Property subtype", property_subtype_choice, index=12)

    with st.expander("Overview", expanded=True):
        spacer, col1, spacer, col2, spacer = st.columns([0.5, 1, 0.5, 1, 0.5])
        with col1:
            user_data["property_bedroomCount"] = st.number_input("Number of bedroom", min_value=1, max_value=30, value=3)
        with col2:
            user_data["property_netHabitableSurface"] = st.number_input("Livable space (m²)", min_value=1, max_value=500, value=195)
        with col1:
            user_data["property_bathroomCount"] = st.number_input("Number of bathroom", min_value=1, max_value=15, value=1)
        with col2:
            user_data["property_land_surface"] = st.number_input("Surface land (m²)", min_value=1, max_value=500, value=282)

    with st.expander("General"):
        spacer, col1, spacer, col2, spacer = st.columns([0.5, 1, 0.5, 1, 0.5])
        with col1:
            user_data["property_building_constructionYear"] = int(str(st.selectbox("Construction Year", ["unknown"] + list(range(1900, 2026)), index=124)).replace("unknown","-1"))
        with col2:
            user_data["property_parkingCountIndoor"] = int(str(st.selectbox("Number of parking in door", ["unknown"] + list(range(0, 11)), index=2)).replace("unknown","-1"))
        with col1:
            user_data["property_building_floorCount"] = int(str(st.selectbox("Number of floor", ["unknown"] + list(range(1, 21)), index=2)).replace("unknown","-1"))
        with col2:
            user_data["property_parkingCountOutdoor"] = int(str(st.selectbox("Number of parking out door", ["unknown"] + list(range(0, 11)), index=2)).replace("unknown","-1"))
        with col1:
            property_building_condition_dict = {"unknown": -1,"to restore": 0,"to be_done_up": 1,"to renovate": 2,"just renovated": 3,"good": 4,"as new": 5}
            user_data["property_building_condition"] = property_building_condition_dict[st.selectbox("Building condition", property_building_condition_dict.keys(), index=0)]
        with col2:
            user_data["property_roomCount"] = int(str(st.selectbox("Number of rooms", ["unknown"] + list(range(1, 101)), index=0)).replace("unknown","-1"))
        with col1:
            user_data["property_building_facadeCount"] = int(str(st.selectbox("Number of facade", ["unknown"] + list(range(1, 5)), index=3)).replace("unknown","-1"))

    with st.expander("Location"):
        unknown_coords = st.checkbox("I don't know the longitude and latitude", value=True)

        spacer, col1, spacer, col2, spacer = st.columns([0.5, 1, 0.5, 1, 0.5])
 
        if unknown_coords: user_data["property_location_latitude"], user_data["property_location_longitude"] = -1, -1
        else:
            with col1:
                user_data["property_location_latitude"] = st.number_input("Latitude", min_value=47.0, max_value=51.5, step=0.000001, format="%.6f")
            with col2:
                user_data["property_location_longitude"] = st.number_input("Longitude", min_value=2.0, max_value=6.5, step=0.000001, format="%.6f")
        with col1:
            user_data["property_location_postalCode"] = int(str(st.selectbox("Postal code", ["unknown"] + list(range(1000, 10000)), index=121)).replace("unknown","-1"))
        with col2:
            user_data["property_building_annexCount"] = int(str(st.selectbox("Number of building annex", ["unknown", "0", "1", "2", "3"], index=0)).replace("unknown","-1"))
        with col1:
            user_data["property_location_hasSeaView"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has sea view", ["unknown", "yes", "no"], index=2)])
        with col2:
            if user_data["property_type_isHouse"] == 0:
                user_data["property_location_floor"] = int(str(st.selectbox("Location floor", ["unknown"] + list(range(0,51)), index=0)).replace("unknown","-1"))

    with st.expander("Interior"):
        spacer, col1, spacer, col2, spacer = st.columns([0.5, 1, 0.5, 1, 0.5])
        with col1:
            living_room = st.selectbox("Living room surface (m²)", ["no living room", "unknown"] + list(range(1, 101)), index=16)
            if living_room == "no living room": property_hasLivingRoom, property_livingRoom_surface = 0, 0
            elif living_room == "unknown": property_hasLivingRoom, property_livingRoom_surface = -1, -1
            else: property_hasLivingRoom, property_livingRoom_surface = 1, int(living_room)
            user_data["property_hasLivingRoom"], user_data["property_livingRoom_surface"] = property_hasLivingRoom, property_livingRoom_surface
        with col2:
            user_data["property_hasLaundryRoom"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has laundry", ["unknown", "yes", "no"], index=2)])
        with col1:
            user_data["property_hasDiningRoom"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has dining room", ["unknown", "yes", "no"], index=2)])
        with col2:
            office = st.selectbox("Office surface (m²)", ["no office", "unknown"] + list(range(1, 101)), index=0)
            if office == "no office": property_specificities_hasOffice, property_specificities_office_surface = 0, 0
            elif office == "unknown": property_specificities_hasOffice, property_specificities_office_surface = -1, -1
            else: property_specificities_hasOffice, property_specificities_office_surface = 1, int(office)
            user_data["property_specificities_hasOffice "], user_data["property_specificities_office_surface"] = property_specificities_hasOffice, property_specificities_office_surface
        with col1:
            user_data["property_hasDressingRoom"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has dressing room", ["unknown", "yes", "no"], index=2)])
        with col2:
            workspace = st.selectbox("Workspace surface (m²)", ["no workspace", "unknown"] + list(range(1, 101)), index=0)
            if workspace == "no workspace": property_specificities_hasWorkspace , property_specificities_workspace_surface = 0, 0
            elif workspace == "unknown": property_specificities_hasWorkspace , property_specificities_workspace_surface = -1, -1
            else: property_specificities_hasWorkspace , property_specificities_workspace_surface = 1, int(workspace)
            user_data["property_specificities_hasWorkspace "], user_data["property_specificities_workspace_surface"] = property_specificities_hasWorkspace , property_specificities_workspace_surface
        with col1:
            property_kitchen_type_choice = ["unknown", "hyper equipped", "installed", "not installed", "semi equipped", "usa hyper equipped", "usa installed", "usa semi equipped", "usa uninstalled"]
            user_data["property_kitchen_type_"] = st.selectbox("Kitchen type", property_kitchen_type_choice, index=2)
        with col2:
            basement = st.selectbox("Basement surface (m²)", ["no basement", "unknown"] + list(range(1, 101)), index=0)
            if basement == "no basement": property_hasBasement , property_basement_surface = 0, 0
            elif basement == "unknown": property_hasBasement , property_basement_surface = -1, -1
            else: property_hasBasement , property_basement_surface = 1, int(workspace)
            user_data["property_hasBasement "], user_data["property_basement_surface"] = property_hasBasement , property_basement_surface
        with col1:
            user_data["property_kitchen_surface"] = int(str(st.selectbox("Kitchen surface (m²)", ["unknown"] + list(range(1, 101)), index=0)).replace("unknown","-1"))
        with col2:
            user_data["transaction_sale_isFurnished"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Is furnished", ["unknown", "yes", "no"], index=2)])
        with col1:
            user_data["property_kitchen_hasOven"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Kitchen has oven", ["unknown", "yes", "no"], index=0)])
        with col2:
            attic = st.selectbox("Attic surface (m²)", ["no attic", "unknown"] + list(range(1, 101)), index=0)
            if attic == "no attic": property_hasAttic, property_attic_surface = 0, 0
            elif attic == "unknown": property_hasAttic, property_attic_surface = -1, -1
            else: property_hasAttic, property_attic_surface = 1, int(attic)
            user_data["property_hasAttic"], user_data["property_attic_surface"] = property_hasAttic, property_attic_surface
        with col1:
            user_data["property_showerRoomCount"] = int(str(st.selectbox("Number of shower", ["unknown"] + list(range(1, 20)), index=0)).replace("unknown","-1"))
        with col2:
            if user_data["property_hasAttic"] > 0:
                user_data["property_attic_isisolated"] = int(str(st.selectbox("Is attic isolated", ["unknown", "yes", "no"], index=0)).replace("unknown","-1").replace("yes", "1").replace("no","0"))
            else : 
                user_data["property_attic_isisolated"] = user_data["property_hasAttic"]
        with col1:
            user_data["property_toiletCount"] = int(str(st.selectbox("Number of toilet", ["unknown"] + list(range(1, 20)), index=2)).replace("unknown","-1"))
        with col2:
            user_data["property_hasArmoredDoor"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has armored door", ["unknown", "yes", "no"], index=2)])

    with st.expander("Exterior"):
        spacer, col1, spacer, col2, spacer = st.columns([0.5, 1, 0.5, 1, 0.5])
        direction_to_angle = {'unknown': -1, 'north': 0,'north_east': 45,'east': 90,'south_east': 135,'south': 180,'south_west': 225,'west': 270,'north_west': 315}

        with col1:
            user_data["property_land_isfacingstreet"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Is facing street", ["unknown", "yes", "no"], index=0)])
        with col2:
            user_data["property_land_hasgaswaterelectricityconnection"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Gas, water & electricity", ["unknown", "yes", "no"], index=0)])

        with col1:
            user_data["property_land_iswooded"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Is wooded", ["unknown", "yes", "no"], index=0)])
        with col2:
            garden = st.selectbox("Garden surface (m²)", ["no garden", "unknown"] + list(range(1, 1001)), index=1)
            if garden == "no garden": property_hasGarden, property_gardenSurface = 0, 0
            elif garden == "unknown": property_hasGarden, property_gardenSurface = -1, -1
            else: property_hasGarden, property_gardenSurface = 1, int(garden)
            user_data["property_hasGarden"], user_data["property_gardenSurface"] = property_hasGarden, property_gardenSurface

        with col1:
            user_data["property_land_hasplottorear"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has plot to rear", ["unknown", "yes", "no"], index=0)])
        with col2:
            if user_data["property_hasGarden"] == 1 :
                garden_angle = st.selectbox("Garden orientation", direction_to_angle.keys(), index=0)
                user_data["garden_cos"] = np.cos(direction_to_angle[garden_angle])

        with col1:
            user_data["property_land_isflat"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Is flat", ["unknown", "yes", "no"], index=0)])
        with col2:
            terrace = st.selectbox("Terrace surface (m²)", ["no terrace", "unknown"] + list(range(1, 1001)), index=1)
            if terrace == "no terrace": property_hasTerrace, property_terraceSurface = 0, 0
            elif terrace == "unknown": property_hasTerrace, property_terraceSurface = -1, -1
            else: property_hasTerrace, property_terraceSurface = 1, int(terrace)
            user_data["property_hasTerrace"], user_data["property_terraceSurface"] = property_hasTerrace, property_terraceSurface

        with col1:
            property_land_sewerconnection_choice = ["unknown", "can be connected", "cannot be connected", "connected", "not connected"]
            user_data["property_land_sewerconnection_"] = st.selectbox("Sewer connection", property_land_sewerconnection_choice, index=3)
        with col2:
            if user_data["property_hasTerrace"] == 1 :
                terrace_angle = st.selectbox("Terrace orientation", direction_to_angle.keys(), index=0)
                user_data["terrace_cos"] = np.cos(direction_to_angle[terrace_angle])

    with st.expander("Facilities"):
        spacer, col1, spacer, col2, spacer = st.columns([0.5, 1, 0.5, 1, 0.5])    

        with col1:
            user_data["property_hasCableTV"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has cable tv", ["unknown", "yes", "no"], index=0)])
        with col2:
            user_data["property_hasVisiophone"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has visiophone", ["unknown", "yes", "no"], index=0)])
        with col1:
            user_data["property_hasInternet"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has internet", ["unknown", "yes", "no"], index=0)])
        with col2:
            user_data["property_hasDoorPhone"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has door phone", ["unknown", "yes", "no"], index=0)])
        with col1:
            user_data["property_hasLift"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has lift", ["unknown", "yes", "no"], index=0)])
        with col2:
            user_data["property_hasAirConditioning"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has air conditioning", ["unknown", "yes", "no"], index=0)])
        with col1:
            user_data["property_hasDisabledAccess"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has disabled access", ["unknown", "yes", "no"], index=0)])
        with col2:
            user_data["property_hasSwimmingPool"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has swimming pool", ["unknown", "yes", "no"], index=0)])
        with col1:
            user_data["property_hasCaretakerOrConcierge"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has care take / concierge", ["unknown", "yes", "no"], index=0)])
        with col2:
            user_data["property_hasSauna"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has sauna", ["unknown", "yes", "no"], index=0)])
        with col1:
            user_data["property_hasSecureAccessAlarm"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has secure access alarm", ["unknown", "yes", "no"], index=0)])
        with col2:
            user_data["property_hasJacuzzi"] = int({"unknown":"-1","yes":"1","no":"0"}[st.selectbox("Has jacuzzi", ["unknown", "yes", "no"], index=0)])

    with st.expander("Energy"):
        spacer, col1, spacer, col2, spacer = st.columns([0.5, 1, 0.5, 1, 0.5])    

        with col1:
            user_data["transaction_certificates_primaryEnergyConsumptionPerSqm"] = int(str(st.selectbox("Primary energy consumption per m²", ["unknown"] + list(range(0, 10000)), index=0)).replace("unknown","-1"))
        with col2:
            user_data["transaction_certificates_primaryEnergyConsumptionYearly"] = int(str(st.selectbox("Primary energy consumption yearly", ["unknown"] + list(range(0, 10000)), index=0)).replace("unknown","-1"))
            
        with col1:
            epc_choices = ["unknown", "a++", "a+", "a", "b", "c", "d", "e", "f", "g"]
            selected_epc = st.selectbox("EPC Score", epc_choices, index=0)

            postal_code = user_data.get("property_location_postalCode", -1)

            # Déduire la région si possible
            if isinstance(postal_code, int) and 1000 <= postal_code <= 9999:
                if 1000 <= postal_code <= 1299:
                    region = "Bruxelles"
                elif (1300 <= postal_code <= 1499) or (4000 <= postal_code <= 7999):
                    region = "Wallonie"
                else:
                    region = "Flandre"
            else:
                region = "Wallonie"  # fallback par défaut si postal_code invalide

            # Dictionnaire de conversion par région
            epc_scales = {
                'Wallonie': {"-1": -1, 'a++': 0, 'a+': 45, 'a': 45, 'b': 95, 'c': 150, 'd': 210, 'e': 275, 'f': 345, 'g': 345},
                'Flandre':  {"-1": -1, 'a++': 0, 'a+': 100, 'a': 200, 'b': 300, 'c': 400, 'd': 500, 'e': 500, 'f': 500, 'g': 500},
                'Bruxelles':{"-1": -1, 'a++': 0, 'a+': 45, 'a': 85, 'b': 170, 'c': 255, 'd': 340, 'e': 425, 'f': 510, 'g': 510}
            }

            # Nettoyage et conversion
            epc_clean = selected_epc.lower() if selected_epc != "unknown" else "-1"
            epc_value = epc_scales[region].get(epc_clean, -1)

            # Enregistrement dans user_data
            user_data["transaction_certificates_epcScore"] = epc_value




    if st.button("Predict"):

        columns_to_get_dummies = {
            "property_subtype_": user_data["property_subtype_"],
            "property_kitchen_type_": user_data["property_kitchen_type_"],
            "property_land_sewerconnection_": user_data["property_land_sewerconnection_"],
        }

        # Chargement modèle + features
        all_ml_csv = glob.glob(path.join("data","ml_data_*.csv"))
        ml_df = pd.concat([pd.read_csv(f, low_memory=False) for f in all_ml_csv], ignore_index=True)
        model_rf = load_model_from_parts("data")
        features = model_rf.feature_names_in_

        df_to_predict = pd.DataFrame([user_data])

        def one_hot_encode_column(df, column_prefix, selected_value, features):
            all_possible_cols = [col for col in features if col.startswith(column_prefix)]
            
            cleaned_selected_value = selected_value.replace(" ", "_")
            selected_col = f"{column_prefix}{cleaned_selected_value}"

            if selected_value == "unknown":
                # unknown = toutes les colonnes mises à -1
                for col in all_possible_cols:
                    df[col] = -1
            else:
                for col in all_possible_cols:
                    df[col] = 0
                if selected_col in all_possible_cols:
                    df[selected_col] = 1
                else:
                    st.warning(f"⚠️ Attention : la colonne {selected_col} n'existe pas dans les features")

            return df, all_possible_cols

        all_one_hot_cols = []

        for col_prefix, selected_val in columns_to_get_dummies.items():
            df_to_predict, created_cols = one_hot_encode_column(df_to_predict, col_prefix, selected_val, features)
            all_one_hot_cols.extend(created_cols)

        for col in features:
            if col not in df_to_predict.columns:
                df_to_predict[col] = np.nan

        #st.write(df_to_predict.iloc[0])

        df_to_predict = df_to_predict[features]

        prediction = model_rf.predict(df_to_predict)
        st.write(f"Prediction: {prediction}")


elif page == "TODO":
    """
    A faire (ordre) :
    - Finir l'app 4
    - Finir l'app 2
    - Finir l'app 3
    - clean le cleaner.py (pipeline)
    - Scrap les différents json
    - Finir l'app 1 (avec le nouveau scraper)
    - Uniformiser les ' et "
    """