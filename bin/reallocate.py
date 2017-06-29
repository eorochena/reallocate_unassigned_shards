#!/usr/bin/python

import random
import requests

from collections import defaultdict

elasticsearch_cluster = ['192.168.1.1', '192.168.1.2', '192.168.1.3', '192.168.1.4']
elasticsearch_port = '9200'

def get_unassigned():
    elasticsearch_url = '%s:%s/_cat/shards?h=index,shard,prirep,state,unassigned.reason' \
                        % (random.choice(elasticsearch_cluster), elasticsearch_port)
    response = requests.get('http://%s' % elasticsearch_url)

    full_response = []
    index = []
    for result in response.content.split('\n'):
        full_response.append(result)
    return full_response

def reroute():
    for values in get_unassigned():
        if values:
            payload = {"commands":
                       [{"allocate":
                             {"index":values.split()[0],
                              "shard":values.split()[1],
                              "node":random.choice(elasticsearch_cluster),
                              "allow_primary":1
                              }
                         }
                        ]
                   }
            print payload
print(reroute())

