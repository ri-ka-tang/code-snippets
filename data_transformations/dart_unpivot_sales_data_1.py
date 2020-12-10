#!/usr/bin/env python3

# Unpivot data and add static columns

from datetime import datetime
from pathlib import Path

import os
import pandas as pd


class Config:
    """
    """

    __FILE_ENDINGS = ["Daily.xls", "Daily(3).xls", "Daily (3).xls", "UNITS.xls"]

    def __init__(self, input_file):
        self.__input_file = input_file
        self.pos_end_of_header = 3
        # Product row in header
        self.pos_custom_row1 = 0

        self.columns_key = ["Store Code", "Store Name", "Format"]
        self.columns_key_rename = {"Store Code": "StoreCode", "Store Name": "StoreName", "Format": "StoreFormat"}
        self.columns_to_remove = ["236 day sales", "Stock in Store", "Stock in Transit", "Total Stock"]
        self.columns_to_remove_dynamic = ["day sales"]

        self.originator_lookup = {"Sainsbury": 36, "Tesco": 38}
        self.__column_name_value = "SalesValue"
        self.__column_name_volume = "SalesVolume"

        self.unpivot_column_name_var = "Date"

        self.output_columns = [
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

        self.output_file_name = self.__input_file + "_processed.csv"
        self.output_separator = "|"

    def is_volume_load(self):
        """
        """
        for files_ending in self.__FILE_ENDINGS:
            if files_ending in self.__input_file:
                return True
        return False

    def get_unpivot_column_name_value(self):
        """
        """
        if self.is_volume_load():
            return self.__column_name_volume

        return self.__column_name_value

    def get_missing_metric_column(self):
        """
        """
        if self.is_volume_load():
            return self.__column_name_value

        return self.__column_name_volume

    def get_filename(self):
        return Path(self.__input_file).name


class ConvertFile:
    """
    """

    def __init__(self, input_file):
        self.cfg = Config(input_file)
        self.raw_data = pd.read_excel(input_file)

    def get_product_info(self, string, splitter1):
        """
        """
        tmp_string = string[string.index(splitter1) + 1 :]
        reverse_string = tmp_string[::-1]
        reverse_string = reverse_string[reverse_string.index(splitter1) + 1 :]
        tmp_string = reverse_string[::-1]

        starting_idx2 = tmp_string.index("(")
        product_name = tmp_string[0:starting_idx2].strip()
        sku_code = tmp_string[starting_idx2 + 1 : (len(tmp_string) - 2)].strip()

        return product_name, sku_code

    def get_orginator_id(self, filters):
        """
        """
        for originator_name in self.cfg.originator_lookup:
            if originator_name in filters:
                return self.cfg.originator_lookup[originator_name]
        return -1

    def get_mssql_datetime(self, dt):
        """
        """
        return (datetime.strptime(dt, "%d-%b-%y")).strftime("%Y-%m-%d %H:%M:%S")

    def get_header(self):
        return self.raw_data.loc[0 : self.cfg.pos_end_of_header]

    def get_filters(self):
        return self.get_header()["Unnamed: 1"].iloc[self.cfg.pos_custom_row1]

    def get_product_name(self):
        product_name, _ = self.get_product_info(self.get_filters(), "-")
        return product_name

    def get_sku_code(self):
        _, sku_code = self.get_product_info(self.get_filters(), "-")
        return sku_code

    def get_originator_id(self):
        return self.get_orginator_id(self.get_filters())

    def get_data_header(self):
        return self.raw_data.loc[self.cfg.pos_end_of_header : self.cfg.pos_end_of_header].values.flatten().tolist()

    def get_pivotted_columns(self):
        pivotted_cols = []

        for col in self.get_data_header():
            remove = False

            if col in self.cfg.columns_to_remove:
                remove = True
            else:
                for wildcard in self.cfg.columns_to_remove_dynamic:
                    if wildcard in col:
                        remove = True
                        break

            if not remove:
                pivotted_cols.append(col)

        return pivotted_cols

    def get_pivotted_data(self):
        data = self.raw_data.loc[self.cfg.pos_end_of_header + 1 : len(self.raw_data.index) - 2]
        data.columns = self.get_data_header()

        return data[self.get_pivotted_columns()]

    def get_unpivot_columns(self):
        return [col for col in self.get_pivotted_data() if col not in self.cfg.columns_key]

    def get_unpivot_data(self, pivotted_data):
        return pivotted_data.melt(
            id_vars=self.cfg.columns_key,
            value_vars=self.get_unpivot_columns(),
            var_name=self.cfg.unpivot_column_name_var,
            value_name=self.cfg.get_unpivot_column_name_value(),
        )

    def get_statics_columns(self):
        return {
            "FileName": self.cfg.get_filename(),
            "Filters": self.get_filters(),
            "SKUCode": self.get_sku_code(),
            "ProductName": self.get_product_name(),
            "OriginatorID": self.get_originator_id(),
            "StoreID": -1,
            "StoreFormatID": -1,
            "ProductID": -1,
            "SourceFileInstanceID": -1,
            self.cfg.get_missing_metric_column(): 0,
        }

    def append_static_columns(self, df):
        for static_column in self.get_statics_columns():
            df[static_column] = self.get_statics_columns()[static_column]

        return df

    def process(self):

        if not os.path.exists(self.cfg.output_file_name):
            pivotted_data = self.get_pivotted_data()
            unpivotted_data = self.get_unpivot_data(pivotted_data)
            output_data = self.append_static_columns(unpivotted_data)
            output_data[self.cfg.unpivot_column_name_var] = output_data[self.cfg.unpivot_column_name_var].map(
                lambda dt: self.get_mssql_datetime(dt)
            )
            output_with_correct_column_names = output_data.rename(columns=self.cfg.columns_key_rename)
            final_output = output_with_correct_column_names[self.cfg.output_columns]

            final_output.to_csv(self.cfg.output_file_name, index=False, sep=self.cfg.output_separator)
        else:
            print("File already processed skipping")


def main():
    # directory = "/home/ktang/dev/projects/DART/Pernod/Data/JS/Historical/SELLING SKUS"
    directory = "/home/ktang/dev/projects/DART/Pernod/Data/Tesco/Historical/SELLING SKUS"

    for filename in os.listdir(directory):
        if filename.endswith(".xls"):
            fullpath = os.path.join(directory, filename)
            print("Processing " + filename)
            convert_file = ConvertFile(input_file=fullpath)
            convert_file.process()


if __name__ == "__main__":
    main()