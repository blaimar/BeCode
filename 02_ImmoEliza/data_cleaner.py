import pandas as pd

df_raw_data = pd.read_csv("../data/raw_data.csv")

df_raw_data = df_raw_data.drop_duplicates(subset="property_ID", keep="first")

df_raw_data.to_csv("../data/raw_data_deduplicated.csv", index=False)
