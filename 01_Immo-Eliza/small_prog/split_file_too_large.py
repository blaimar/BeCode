from os import path
import pandas as pd

name_file = "durty_data"
extension_file = "csv"
path_to_save = path.join("..","data",f"{name_file}")

def split_file(filepath, extension_file, part_size_mb=20):
    if extension_file == "csv":
        total_size_bytes = path.getsize(filepath+".csv")
        target_chunk_size_bytes = part_size_mb * 1024 * 1024
        df = pd.read_csv(filepath+".csv")
        total_rows = df.shape[0]
        avg_row_size = total_size_bytes / total_rows
        lines_per_chunk = int(target_chunk_size_bytes / avg_row_size)
        for i in range(0, total_rows, lines_per_chunk):
            chunk = df.iloc[i:i+lines_per_chunk]
            chunk.to_csv(f"{filepath.rstrip('.csv')}_part{i//lines_per_chunk:03d}.csv", index=False)
    else:
        part_size = part_size_mb * 1024 * 1024
        with open(f"{filepath}.{extension_file}", 'rb') as f:
            part_num = 0
            while True:
                chunk = f.read(part_size)
                if not chunk:
                    break
                with open(f"{filepath}_{part_num:03d}.{extension_file}", 'wb') as part_file:
                    part_file.write(chunk)
                part_num += 1

split_file(path_to_save, extension_file, part_size_mb=20)