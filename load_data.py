from elasticsearch import Elasticsearch
from elasticsearch import helpers
import json

def load_datas():
    actions = list()
    with open('anal_product_2019-09.json', 'r', encoding='utf8') as f:
        for data in f.readlines():
            source = json.loads(data.strip())

            actions.append({
                "_index": "anal_product_2019-09",
                "_op_type": "index",
                "_source": source
            })

    return actions

if __name__ == "__main__":
    es = Elasticsearch('http://localhost:9200')
    data = load_datas()
    helpers.bulk(es, data)