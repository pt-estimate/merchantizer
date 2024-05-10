from metaflow import FlowSpec, step, IncludeFile

RAW_URL = r"https://drive.usercontent.google.com/download?id=1ipLgd_ZzAjRXTCfoIcpCUCA_seqqG_iY&confirm=t&uuid=a208ebfc-464b-4e4f-a3f5-d3d2ae7b6c7b&at=APZUnTXnsGYGd_lVT2u5p52PGvw1:1713713272496"
FILENAME = "10mil_random.csv"


class TransactionLoadFlow(FlowSpec):
    """
    A flow to download the transaction data..

    This flow performs the following steps:
    1) Attempt to download the data from a local file
    2) Otherwise download data from remote url
    3) Prints shape of downloaded data
    """

    @step
    def start(self):
        """
        The start step:
        1) Checks if a local copy exists and downloads it if not
        2) Otherwise loads the local transaction data into dataframe

        """
        import io
        import os
        import pandas as pd
        import polars as pl
        import requests
        import zipfile

        if not os.path.isfile(FILENAME):
            print(f"Reading data from {RAW_URL}")
            r = requests.get(RAW_URL)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall()

        self.df = pl.read_csv(FILENAME, null_values="null", dtypes={'merchantZip':str})
        self.next(self.end)

    @step
    def end(self):
        """
        The end step:
        1) Prints out the shape of the dataframe

        """
        print(f" The data has {self.df.shape[0]} records (rows)" + \
        f" and {self.df.shape[1]} fields (columns).")


if __name__ == "__main__":
    TransactionLoadFlow()
