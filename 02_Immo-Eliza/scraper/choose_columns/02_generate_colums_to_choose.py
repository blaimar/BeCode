# GENERATE BY CHATGPT

import pandas as pd
import ast, pprint
from collections.abc import Mapping

def deep_merge_keys(base, new):
    for key, value in new.items():
        if isinstance(value, Mapping): base[key] = deep_merge_keys(base.get(key, {}), value)
        else: base.setdefault(key, "")
    return base

df = pd.read_csv("all_data_in_json.csv")
json_columns = ['customers', 'flags', 'media', 'property', 'publication', 'transaction', 'priceType']
global_skeleton = {}
for _, row in df.iterrows():
    for col in json_columns:
        try:
            value = ast.literal_eval(row[col]) if pd.notnull(row[col]) else None
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict): global_skeleton[col] = deep_merge_keys(global_skeleton.get(col, {}), item)
            elif isinstance(value, dict): global_skeleton[col] = deep_merge_keys(global_skeleton.get(col, {}), value)
        except Exception as e: continue


# Skeleton in dictionnary
#pprint.pprint(global_skeleton, width=120)

def flatten_dict_paths(d, prefix=None, result=None):
    if result is None: result = {}
    if prefix is None: prefix = []
    for key, value in d.items():
        new_prefix = prefix + [key]
        if isinstance(value, dict): flatten_dict_paths(value, new_prefix, result)
        else: result['_'.join(new_prefix)] = new_prefix
    return result

flattened = flatten_dict_paths(global_skeleton)
pprint.pprint(flattened, width=500)
with open("columns_to_choose.txt", "w", encoding="utf-8") as f: f.write(pprint.pformat(flattened, width=500))