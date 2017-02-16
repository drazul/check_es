#!/usr/bin/env python
import argparse
import json
import requests
import sys
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, NotFoundError


class Checker:
    def __init__(self, args):
        self.fields_to_be_returned = args.fields_to_be_returned.split(',') if args.fields_to_be_returned else []
        self.fields_to_be_deleted = args.fields_to_be_deleted.split(',') if args.fields_to_be_deleted else []
        self.error_return_status = args.error_return_status
        self.index = args.index
        self.query = ' '.join(args.query)
        self.warning = args.warning
        self.critical = args.critical
        self.mode = args.mode

        if args.hostname:
            self.hostname, self.port = args.hostname.split(':')
        elif args.host and args.port:
            self.hostname = args.host
            self.port = args.port

        self.elasticsearch = Elasticsearch([{'host': self.hostname, 'port': self.port}])

    def nagios_output(self, status_code, message):
        pretty_status_code = {0: 'OK', 1: 'WARNING', 2: 'CRITICAL', 3: 'UNKNOW'}

        pretty_output = "%(status_code)s - %(message)s" % {
                'status_code': pretty_status_code[status_code],
                'message': message
            }

        print (pretty_output)
        sys.exit(status_code)

    def perform_check(self):
        if self.mode == 'search':
            status_code, message = self.perform_search()
        if self.mode == 'cluster-health':
            status_code, message = self.perfom_check_cluster_health()

        self.nagios_output(status_code, message)

    def perfom_check_cluster_health(self):
        json_result = self.elasticsearch.cluster.health()
        status_code = self.check_limits(json_result['status'])
        return (status_code, json_result)

    def perform_search(self):
        output = []

        try:
            json_data = self.elasticsearch.search(index=self.index, body=self.query)
        except ConnectionError:
            self.nagios_output(self.error_return_status, "Error connecting to %(host)s:%(port)s." % {
                    'host': self.hostname,
                    'port': self.port
                })
        except NotFoundError:
            self.nagios_output(self.error_return_status, "Error querying index %(index)s." % {
                    'index': self.index
                })

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

        data_to_track = "counter=%(number_of_entries)s;%(warning)s;%(critical)s;0" % {
                'number_of_entries': number_of_entries,
                'warning': self.warning,
                'critical': self.critical
            }
        message = "%(number_of_entries)s elements found.\n%(json_result)s\n|%(data_to_track)s" % {
                'number_of_entries': number_of_entries,
                'json_result': json_result,
                'data_to_track': data_to_track
            }

        return (status_code, message)

    def check_limits(self, value_to_check):
        try:
            if self.warning:
                int(self.warning)
            if self.critical:
                int(self.critical)
            return self.__check_limits_numbers(value_to_check)
        except Exception:
            return self.__check_limits_strings(value_to_check)

    def __check_limits_strings(self, value_to_check):
        if value_to_check is None:
            return 3
        if self.critical and value_to_check == self.critical:
            return 2
        if self.warning and value_to_check == self.warning:
            return 1
        return 0

    def __check_limits_numbers(self, value_to_check):
        if value_to_check is None:
            return 3
        if self.critical and int(value_to_check) >= int(self.critical):
            return 2
        if self.warning and int(value_to_check) >= int(self.warning):
            return 1
        return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Return result of elasticsearch query in nagios check format.')

    parser.add_argument('--host', help='elasticsearch ip or hostname', type=str, default='localhost')
    parser.add_argument('--port', help='elasticsearch port', type=int, default=9200)
    parser.add_argument('--hostname', help='elasticsearch host:port', type=str, default=None)

    parser.add_argument('--mode', choices=['search', 'cluster-health'], help='operation mode', type=str, default='search')
    parser.add_argument('--query', help='[search mode] JSON string with elasticsearch query', type=str, default='{}', nargs='+')
    parser.add_argument('--index', help='[search mode] index name to query', type=str, default='*')

    parser.add_argument('-w', '--warning', help='number of entries needed to throw a WARNING', type=int, default=None)
    parser.add_argument('-c', '--critical', help='number of entries needed to throw a CRITICAL', type=int, default=None)
    parser.add_argument('-e', '--error-return-status', help=('status to return on connection or index error:'
                        ' 0 - OK, 1 - WARNING, 2 - CRITICAL, 3 - UNKNOWN'), type=int, default=3)
    parser.add_argument('--fields-to-be-returned', help='[search mode] fields to return (separated by ",")', type=str, default=None)
    parser.add_argument('--fields-to-be-deleted', help='[search mode] fields to delete from return output (separated by ",")', type=str, default=None)

    args = parser.parse_args()

    Checker(args).perform_check()
