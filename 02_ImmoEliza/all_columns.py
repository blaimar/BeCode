import pandas as pd
import ast
from collections.abc import Mapping

# Fonction de fusion récursive de dictionnaires
def deep_merge_keys(base, new):
    for key, value in new.items():
        if isinstance(value, Mapping):
            base[key] = deep_merge_keys(base.get(key, {}), value)
        else:
            base.setdefault(key, "")
    return base

# Charger le CSV
df = pd.read_csv("../data/raw_data.csv")


# Colonnes potentiellement JSON
json_columns = ['customers', 'flags', 'media', 'property', 'publication', 'transaction', 'priceType']

# Initialiser le squelette global
global_skeleton = {}

# Parcourir les lignes du DataFrame
for _, row in df.iterrows():
    for col in json_columns:
        try:
            value = ast.literal_eval(row[col]) if pd.notnull(row[col]) else None
            if isinstance(value, list):  # ex: liste de dictionnaires
                for item in value:
                    if isinstance(item, dict):
                        global_skeleton[col] = deep_merge_keys(global_skeleton.get(col, {}), item)
            elif isinstance(value, dict):
                global_skeleton[col] = deep_merge_keys(global_skeleton.get(col, {}), value)
        except Exception as e:
            continue  # ignore les erreurs de parsing

# Afficher le squelette
import pprint
pprint.pprint(global_skeleton, width=120)

