from google.cloud.bigquery import Client as BQClient
from google.cloud.bigquery import SchemaField, LoadJobConfig
from google.cloud.storage import Client as GCSClient
from dataclasses import dataclass


@dataclass
class TableData:
    project: str
    dataset: str
    name: str
    uri: str
    bucket: str

    def get_full_table_name(self):
        return f'{self.project}.{self.dataset}.{self.name}'

    def get_schema(self):
        gcs = GCSClient(self.project)
        gcs_bucket = gcs.get_bucket(self.bucket)
        blob = gcs_bucket.blob(self.uri.split(self.bucket + '/')[1])
        header = get_csv_header_line(blob)
        return [schemify(h) for h in header.split(',')]

    def upload_to_bq(self):
        client = BQClient(self.project)
        job_config = LoadJobConfig(schema=self.get_schema(),
                                   skip_leading_rows=1)
        table_id = self.get_full_table_name()
        load_job = client.load_table_from_uri(source_uris=self.uri,
                                              destination=table_id,
                                              job_config=job_config)
        load_job.result()
        destination_table = client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))


def schemify(header):
    s = SchemaField(name=header.replace(' ', '_'),
                    field_type='STRING')
    return s


def get_csv_header_line(blob):
    chunk_size = 2000
    position = 0
    buff = []
    while True:
        chunk = blob.download_as_string(start=position,
                                        end=position + chunk_size).decode()
        if '\n' in chunk:
            part1, part2 = chunk.split('\n', 1)
            buff.append(part1)
            return ''.join(buff)
        else:
            buff.append(chunk)

        position += chunk_size + 1
        if len(chunk) < chunk_size:
            return ''.join(buff)

