# Apply static columns to sales data already unpivotted

from datetime import datetime
from pathlib import Path

import os
import pandas as pd

file_fulpath = "C:\\Users\\ktang\\Documents\\Work\\playground\\DART\\19.Pernod Ricard\\JS\\Zipped\\Sainsburys EPOS Sales Apr - Jun 2020.csv"
filename = Path(file_fulpath).name
output_filename = "C:\\Users\\ktang\\Documents\\Work\\playground\\DART\\19.Pernod Ricard\\JS\\Zipped\\" + filename + "_processed.csv"
has_header = None
raw_data_header = ["RetailCode", "RetailName", "StoreName", "Date", "SKUCode", "ProdDesc", "ProdSize", "PernodCode", "SalesVolume", "SalesValue"]


originator_lookup = {"Sainsbury": 36, "Sainsburys": 36,"Tesco": 38}

output_columns = [
            "FileName",
            "Filters",
            "StoreCode",
            "StoreName",
            "StoreFormat",
            "SKUCode",
            "ProductName",
            "OriginatorID",
            "StoreID",
            "StoreFormatID",
            "ProductID",
            "Date",
            "SalesVolume",
            "SalesValue",
            "SourceFileInstanceID",
        ]

static_columns = {
            "FileName": filename,
            "Filters": "",
            "StoreCode": "",
            "StoreFormat": "",
            "OriginatorID": get_orginator_id(filename),
            "StoreID": -1,
            "StoreFormatID": -1,
            "ProductID": -1,
            "SourceFileInstanceID": -1,
        }

def get_orginator_id(filename):
    """
    """
    for originator_name in originator_lookup:
        if originator_name in filename:
            return originator_lookup[originator_name]
    return -1

def append_static_columns(new_columns, df):
    for static_column in new_columns:
        df[static_column] = new_columns[static_column]
    return df

raw_data = pd.read_csv(file_fulpath, header=has_header, names=raw_data_header, dtype={"SKUCode": object})
raw_data["ProductName"] = raw_data["ProdDesc"] + " " +  raw_data["ProdSize"]
new_data = raw_data[list(set(raw_data.columns).intersection(set(output_columns)))]
output = append_static_columns(static_columns, new_data)
final_output = output[output_columns]

final_output.to_csv(output_filename, index=False, sep="|")