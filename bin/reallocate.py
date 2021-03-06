#!/usr/bin/python

import random
import requests
import json
import sys

elasticsearch_port = '9200'
elasticsearch_node = raw_input('Enter ip of one node that is part of the elaticsearch cluster > ')
elastic_session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=10)
elastic_session.mount('http://', adapter)

def elasticsearch_cluster():
    cluster = []
    try:
        get_cluster = 'http://%s:%s/_cat/nodes?pretty' % (elasticsearch_node, elasticsearch_port)
        response = elastic_session.get(get_cluster, timeout=5)
        for data_server in response.content.split('\n'):
            if data_server:
                if data_server.split()[0] and data_server.split()[0] not in cluster:
                    cluster.append(data_server.split()[0])
        return cluster
    except Exception as e:
        print(e, '\n\n  Is elasticsearch process running on ' + elasticsearch_node + '?' )
        sys.exit(2)

def get_unassigned():
    if elasticsearch_cluster():
        elasticsearch_url = 'http://%s:%s/_cat/shards?h=index,shard,prirep,state,unassigned.reason' \
                        % (random.choice(elasticsearch_cluster()), elasticsearch_port)
        response = elastic_session.get('%s' % elasticsearch_url, timeout=10)
        full_response = []
        duplicates = []
        for result in response.content.split('\n'):
            if result:
                index_shard = result.split()[0] + ' ' + result.split()[1]
                if 'UNASSIGNED' in result and index_shard not in duplicates:
                    duplicates.append(index_shard)
                    full_response.append(result)
        return full_response
    else:
        print(elasticsearch_cluster())
        print('Unable to get cluster information')
        sys.exit(1)

def reroute():
    if get_unassigned():
        for values in get_unassigned():
            if values:
                payload = {"commands":
                       [{"allocate_replica":
                             {"index":values.split()[0],
                              "shard":values.split()[1],
                              "node":random.choice(elasticsearch_cluster())
                              }
                         }
                        ]
                    }
                elasticsearch_reroute = 'http://%s:%s/_cluster/reroute' % (random.choice(elasticsearch_cluster()),
                                                                elasticsearch_port)
                post_request = elastic_session.post(elasticsearch_reroute, data=json.dumps(payload), timeout=30)
                print(post_request)
    else:
        print('No shards in UNASSIGNED state were found')
        sys.exit(0)


reroute()
sys.exit(0)

