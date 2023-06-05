from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import json
import argparse
import sys
import os

parser = argparse.ArgumentParser(description='311 Requests Data')
parser.add_argument('--page_size', type=int, help='how many rows to get per page', required=True)
parser.add_argument('--num_pages', type=int, help='how many pages to get in total')
args = parser.parse_args(sys.argv[1:])
print(args)


DATASET_ID=os.environ["DATASET_ID"]
APP_TOKEN=os.environ["APP_TOKEN"]
ES_HOST=os.environ["ES_HOST"]
ES_USERNAME=os.environ["ES_USERNAME"]
ES_PASSWORD=os.environ["ES_PASSWORD"]
INDEX_NAME=os.environ["INDEX_NAME"]



if __name__ == '__main__':
    try:
        resp = requests.post(f"{ES_HOST}/{INDEX_NAME}", auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
                            json={
                                "settings": {
                                    "number_of_shards": 1,
                                    "number_of_replicas": 1
                                },
                                "mappings": {
                                    "properties": {
                                        "starfire_incident_id": {"type": "keyword"},
                                        "incident_datetime": {"type": "date"},
                                        "incident_borough": {"type": "keyword"},
                                        "incident_classification": {"type": "keyword"},
                                        "engines_assigned_quantity": {"type": "integer"},
                                        "incident_response_seconds_qy": {"type": "integer"},
                                    }
                                },
                            }
                            )
        resp.raise_for_status()
        print(resp.json())

    except Exception as e:
        print("Index already exists! Skipping")

    client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout=10000)
    page = 1
    records_loaded = 0
    es_rows = []
    # Count the number of rows in the dataset
    count_result = client.get(DATASET_ID, select='COUNT(*)', where="starfire_incident_id IS NOT NULL AND incident_datetime IS NOT NULL")
    total_rows = int(count_result[0]['COUNT'])
    print(f"Total rows in the dataset: {total_rows}")

    while (records_loaded < 10000) or (args.num_pages is not None and page <= args.num_pages):
        offset = (page - 1) * args.page_size
        print(f"Fetching page {page} with offset {offset}")

        rows = client.get(DATASET_ID, limit=args.page_size, offset=offset,
                         where="starfire_incident_id IS NOT NULL AND incident_datetime IS NOT NULL")

        for row in rows:
            try:
                es_row = {}
                es_row["starfire_incident_id"] = row["starfire_incident_id"]
                es_row["incident_datetime"] = row["incident_datetime"]
                es_row["incident_borough"] = row["incident_borough"]
                es_row["incident_classification"] = row["incident_classification"]
                es_row["engines_assigned_quantity"] = int(row["engines_assigned_quantity"])
                es_row["incident_response_seconds_qy"] = int(row["incident_response_seconds_qy"])

            except Exception as e:
                print(f"Error!: {e}, skipping row: {row}")
                continue
            es_rows.append(es_row)

        bulk_upload_data = ""
        for line in es_rows:
            action = '{"index": {"_index": "' + INDEX_NAME + '", "_type": "_doc", "_id": "' + line["starfire_incident_id"] + '"}}'
            data = json.dumps(line)
            bulk_upload_data += f"{action}\n"
            bulk_upload_data += f"{data}\n"

        try:
            resp = requests.post(f"{ES_HOST}/_bulk",
                                 data=bulk_upload_data, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD), headers={"Content-Type": "application/x-ndjson"})
            resp.raise_for_status()
            print(resp.content)
            print('Done')

        except Exception as e:
            print(f"Failed to insert in ES: {e}")
        records_loaded += len(rows)
        print(f"Loaded {records_loaded} records so far")

        page += 1

        if args.num_pages is not None and page > args.num_pages:
            print(f"Fetched {page - 1} pages, stopping as requested.")
            break

    print(f"Total records loaded: {records_loaded}")