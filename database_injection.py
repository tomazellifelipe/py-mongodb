import concurrent.futures
import configparser
import json
import os
import urllib.request

from pymongo import MongoClient

config = configparser.ConfigParser()
config.read(os.path.abspath(os.path.join(".ini")))
dburi = config['PROD']['DB_URI']

ranges = [[8131611, 8151611], [8151611, 8171611],
          [8171611, 8191611], [8191611, 8211611]]


def scrapper(ranges: list) -> list:
    data = []
    for token in range(ranges[0], ranges[1]):
        token_str = str(token)
        json_url = config['PROD']['PREFIX'] + \
            token_str + config['PROD']['SUFFIX']
        try:
            request = urllib.request.urlopen(json_url)
            data.append(json.load(request))
        except Exception:
            pass
    return data


if __name__ == '__main__':
    data = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = [executor.submit(scrapper, ran) for ran in ranges]

        for f in concurrent.futures.as_completed(results):
            data.append(f.result())

    client = MongoClient(dburi)
    db = client.test_db
    test_coll = db.test_collection
    for elem in data:
        test_coll.insert_many(elem)
