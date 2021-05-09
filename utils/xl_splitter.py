from utils.table_data import TableData
from google.cloud.storage import Client
import pandas as pd


class XLSplitter:

    def __init__(self, request):
        self._request = request
        self._table_data = []

    def split(self):
        gcs = Client(self._request['project'])
        gcs_bucket = gcs.get_bucket(self._request['bucket'])
        blob = gcs_bucket.blob(self._request['uri'].split(self._request['bucket'] + '/')[1])
        data = blob.download_as_string()
        wb = pd.ExcelFile(data)
        split_folder = '.'.join(blob.name.split('.')[:-1]) + '/'
        for sheet in wb.sheet_names:
            sheet_name = sheet.replace(' ', '_')
            filename = sheet_name + '.csv'
            df = pd.read_excel(data, sheet_name=sheet)
            cols = [c for c in df.columns if "Unnamed: " not in str(c)]
            df = df[cols]
            filepath = split_folder + filename
            uri = "gs://" + self._request['bucket'] + '/' + filepath
            blob = gcs_bucket.blob(filepath)
            blob.upload_from_string(df.to_csv(index=False))
            sheet_table_data = TableData(project=self._request['project'],
                                         dataset=self._request['dataset'],
                                         name=sheet_name,
                                         uri=uri,
                                         bucket=self._request['bucket'])
            self._table_data.append(sheet_table_data)

    def get_table_data(self):
        return self._table_data
