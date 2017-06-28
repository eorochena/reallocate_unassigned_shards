#!/usr/bin/python

import random
import requests
import json

elasticsearch_cluster = ['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4']
elasticsearch_port = '9200'

def get_unassigned():
    elasticsearch_url = '%s:%s/_cat/shards?h=index,shard,prirep,state,unassigned.reason' \
                        % (random.choice(elasticsearch_cluster), elasticsearch_port)
    response = requests.get('http://%s' % elasticsearch_url)

    unassigned = []

    for indexs in response.content.split('\n'):
        unassigned.append(indexs)
    return unassigned
print(get_unassigned())

