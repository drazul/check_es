#!/usr/bin/env python
import argparse
import json
import requests
import sys
from elasticsearch import Elasticsearch

class Checker:
    def __init__(self, args):
        self.args = args
        self.fields_to_be_returned = args.fields_to_be_returned.split(',') if args.fields_to_be_returned else []
        self.fields_to_be_deleted = args.fields_to_be_deleted.split(',') if args.fields_to_be_deleted else []
        
        if self.args.hostname:
            hostname, port = self.args.hostname.split(':')
        elif self.args.host and self.args.port:
            hostname = self.args.host
            port = self.args.port

        self.elasticsearch = Elasticsearch([{'host': hostname, 'port': port}])

    def perform_check(self):
        if self.args.mode == 'search':
            status_code, message = self.perform_search()
        if self.args.mode == 'cluster-health':
            status_code, message = self.perfom_check_cluster_health()

        
        self.nagios_output(status_code, message)

    def nagios_output(self, status_code, message):
        pretty_status_code = {0:'OK', 1: 'WARNING', 2: 'CRITICAL', 3: 'UNKNOW'}

        pretty_output = '%(status_code)s - %(message)s' % {'status_code': pretty_status_code[status_code], 'message': message}

        print (pretty_output)
        sys.exit(status_code)

    def perfom_check_cluster_health(self):
        json_result = self.elasticsearch.cluster.health()
        status_code = self.check_limits(json_result['status'])
        return (status_code, json_result)


    def perform_search(self):
        output = []

        json_data = self.elasticsearch.search(index = args.index, body = args.query)


        if self.fields_to_be_returned:
            for element in json_data['hits']['hits']:
                data = dict(
                    (k, v)
                    for k, v in element['_source'].items()
                    if k in self.fields_to_be_returned
                )
                output.append(data)

        elif self.fields_to_be_deleted:
            for element in json_data['hits']['hits']:
                data = dict(
                    (k, v)
                    for k, v in element['_source'].items()
                    if k not in self.fields_to_be_deleted
                )
                output.append(data)

        else:
            for element in json_data['hits']['hits']:
                output.append(element['_source'])

        number_of_entries = len(output)
        json_result = json.dumps(output, indent=2, sort_keys=True)
        status_code = self.check_limits(number_of_entries)
        
        data_to_track = "counter=%(number_of_entries)s;%(warning)s;%(critical)s;0" % {'number_of_entries': number_of_entries, 'warning': self.args.warning, 'critical': self.args.critical}
        message = "%(number_of_entries)s elements found.\n%(json_result)s\n|%(data_to_track)s" % {'number_of_entries': number_of_entries, 'json_result': json_result, 'data_to_track': data_to_track} 
        
        
        return (status_code, message)

    def check_limits(self, value_to_check):
        try:
            if self.args.warning:
                int(self.args.warning)
            if self.args.critical:
                int(self.args.critical)
            return self.__check_limits_numbers(value_to_check)
        except Exception:
            return self.__check_limits_strings(value_to_check)

            
    def __check_limits_strings(self, value_to_check):
        if value_to_check == None:
            return 3
        if self.args.critical and value_to_check == self.args.critical:
            return 2
        if self.args.warning and value_to_check == self.args.warning:
            return 1
        return 0
    
    def __check_limits_numbers(self, value_to_check):
        if value_to_check == None:
            return 3
        if self.args.critical and int(value_to_check) >= int(self.args.critical):
            return 2
        if self.args.warning and int(value_to_check) >= int(self.args.warning):
            return 1
        return 0



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description =  'Return result of a elasticsearch query with nagios format')

    parser.add_argument('--host', help = 'ip or cname of elasticsearch endpoint', type = str, default = 'localhost')
    parser.add_argument('--port', help = 'port of elasticsearch', type = int, default = 9200)
    parser.add_argument('--hostname', help = 'host:port of elasticsearch', type = int, default = None)

    parser.add_argument('--mode', choices = ['search', 'cluster-health'], help = 'operation mode', type = str, default = 'search')
    parser.add_argument('--query', help = '(only in searchs) json string with elasticsearch query', type = str, default = '{}')
    parser.add_argument('--index', help = '(only in searchs) index where apply query', type = str, default = '*')

    parser.add_argument('-w', '--warning', help = 'number of entries neededed to throw a warning', type = str, default = None)
    parser.add_argument('-c', '--critical', help = 'number of entries neededed to throw a critical', type = str, default = None)
    parser.add_argument('--fields-to-be-returned', help = '(only in searchs) fields to be returned, separated by ,', type = str, default = None)
    parser.add_argument('--fields-to-be-deleted', help = '(only in searchs) fields to be deleted on return, separated by ,', type = str, default = None)

    args = parser.parse_args()

    Checker(args).perform_check()
    