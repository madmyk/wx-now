#!/usr/bin/env python3
"""
Wx Now from Elasticsearch

Local testing:
    kubectl port-forward service/elasticsearch 9200:9200 -n the-project
    HOST=localhost:9200 python ./wx-now.py
"""
__author__ = "Myke Madded"
__version__ = "0.0.1"
__license__ = "MIT"

import os
import json
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, connections

host = os.environ['HOST']
connections.create_connection(hosts=[host], timeout=20)
client = Elasticsearch()
s = Search(using=client)


def now(event, context):
    """
        Return wx now
        Uses the Python Elasticsearch DSL
        https://elasticsearch-dsl.readthedocs.io/en/latest/search_dsl.html
    """
    global s

    res = s.from_dict({
        "size": 1,
        "query": {
            "range": {
                "@timestamp": {
                    "gt": "now-10m"
                }
            }
        }
    }).execute()

    return res.to_dict()


if __name__ == '__main__':
    """
    Mock event and context
    See: https://kubeless.io/docs/kubeless-functions/
    """
    event = {
        "data": "",
        "event-id": "test",
        "event-type": "application/json",
        "event-time": "",
        "event-namespace": "wx-stats.mk.imti.co",
        "extensions": {
            "request": {},
            "response": {}
        }
    }

    context = {
        "function-name": "mock",
        "timeout": "180",
        "runtime": "nodejs6",
        "memory-limit": "128M"
    }

    json_string = json.dumps(now(event, context), indent=2)
    print(json_string)