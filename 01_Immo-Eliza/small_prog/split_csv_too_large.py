from os import path
from math import ceil
import pandas as pd

name_csv = "durty_data"
path_to_save = path.join("..","data",f"{name_csv}.csv")

df=pd.read_csv(path_to_save)
size = path.getsize(path_to_save)
number_of_file = ceil(size/20_000_000)
precision = len(str(number_of_file))

number_part = len(df) // number_of_file

for i in range(number_of_file):
    start = i*number_part
    end = (i+1) * (number_part if i < (number_of_file-1) else len(df))
    df.iloc[start:end].to_csv(path.join("..","data",f"{name_csv}_{i:0{precision}d}.csv"), index=False)