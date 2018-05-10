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

    def perform_check(self):
        if self.mode == 'search':
            status_code, message = self.perform_search()
        elif self.mode == 'cluster-health':
            status_code, message = self.perfom_check_cluster_health()
        elif self.mode == 'indices-stats':
            status_code, message = self.perform_check_indices_stats()
        elif self.mode == 'nodes-stats':
            status_code, message =  self.perform_check_nodes_stats()
        elif self.mode == 'all-stats':
            status_code_cluster_health, message_cluster_health = self.perfom_check_cluster_health()
            status_code_indices_stats, message_indices_stats = self.perform_check_indices_stats()
            status_code_nodes_stats, message_nodes_stats =  self.perform_check_nodes_stats()

            status_code = max(status_code_cluster_health, status_code_indices_stats, status_code_nodes_stats)
            message = str(message_cluster_health) + message_indices_stats + message_nodes_stats

        self.nagios_output(status_code, message)

    def nagios_output(self, status_code, message):
        pretty_status_code = {0: 'OK', 1: 'WARNING', 2: 'CRITICAL', 3: 'UNKNOW'}

        pretty_output = "%(status_code)s - %(message)s" % {
                'status_code': pretty_status_code[status_code],
                'message': message
            }

        print (pretty_output)
        sys.exit(status_code)

    def perfom_check_cluster_health(self):
        json_result = self.elasticsearch.cluster.health()
        status_code = self.check_limits(json_result['status'])
        return (status_code, json_result)

    def _get_data_from_index_stats(self, index_name, index_stats):
        result = dict()
        sanitized_index_name = index_name.replace('.', '')

        result['index.%s.docs.count' % sanitized_index_name] = index_stats['docs']['count']
        result['index.%s.store.size_in_bytes' % sanitized_index_name] = index_stats['store']['size_in_bytes']

        result['index.%s.search.query_total' % sanitized_index_name] = index_stats['search']['query_total']
        result['index.%s.search.query_time_in_millis' % sanitized_index_name] = index_stats['search']['query_time_in_millis']

        result['index.%s.search.fetch_total' % sanitized_index_name] = index_stats['search']['fetch_total']
        result['index.%s.search.fetch_time_in_millis' % sanitized_index_name] = index_stats['search']['fetch_time_in_millis']

        result['index.%s.search.scroll_total' % sanitized_index_name] = index_stats['search']['scroll_total']
        result['index.%s.search.scroll_time_in_millis' % sanitized_index_name] = index_stats['search']['scroll_time_in_millis']

        result['index.%s.search.total' % sanitized_index_name] = (
            result['index.%s.search.query_total' % sanitized_index_name] +
            result['index.%s.search.fetch_total' % sanitized_index_name] +
            result['index.%s.search.scroll_total' % sanitized_index_name]
        )
        result['index.%s.search.time_in_millis' % sanitized_index_name] = (
            result['index.%s.search.query_time_in_millis' % sanitized_index_name] +
            result['index.%s.search.fetch_time_in_millis' % sanitized_index_name] +
            result['index.%s.search.scroll_time_in_millis' % sanitized_index_name]
        )

        if result['index.%s.search.total' % sanitized_index_name] != 0:
            result['index.%s.search.average_latency_in_millis' % sanitized_index_name] = (
                result['index.%s.search.time_in_millis' % sanitized_index_name] /
                result['index.%s.search.total' % sanitized_index_name]
            )
        else:
            result['index.%s.search.average_latency_in_millis' % sanitized_index_name] = 0

        return result

    def _merge_dict(self, d1, d2):
        if isinstance(d1, dict):
            merged = dict()
            for k in d1:
                merged[k] = self._merge_dict(d1[k], d2[k])
            return merged

        elif isinstance(d1, int):
            return d1 + d2

        else:
            return 'Unknow'

    def _merge_indices_stats(self, indices):
        merged_indices = dict()

        for index_name in indices:
            short_name = index_name.split('-')[0]
            if not short_name in merged_indices:
                merged_indices[short_name] = indices[index_name]['total']
            else:
                merged_indices[short_name] = self._merge_dict(
                    merged_indices[short_name],
                    indices[index_name]['total']
                )

        return merged_indices

    def _get_data_from_shards_stats(self, shard_stats):
        return {
            'shards.total': shard_stats['total'],
            'shards.successful': shard_stats['successful'],
            'shards.failed': shard_stats['failed']
        }

    def perform_check_indices_stats(self):
        json_result = self.elasticsearch.indices.stats()

        data = dict()
        data.update(self._get_data_from_shards_stats(json_result['_shards']))
        data.update(self._get_data_from_index_stats('all', json_result['_all']['total']))

        indices = self._merge_indices_stats(json_result['indices'])

        for index_name in indices:
            data.update(self._get_data_from_index_stats(index_name, indices[index_name]))

        graphite_output = "| "
        for index_name in data:
            graphite_output += "%s=%s;;;" % (index_name, data[index_name])
        return (0, graphite_output)

    def _get_data_from_node_stats(self, node_stats):
        data = dict()
        node_name = node_stats['name']

        data['node.%s.http.current_open' % node_name] = node_stats['http']['current_open']
        return data

    def perform_check_nodes_stats(self):
        json_result = self.elasticsearch.nodes.stats()

        nodes = dict()
        for node_hash in json_result['nodes']:
            nodes.update(self._get_data_from_node_stats(json_result['nodes'][node_hash]))

        graphite_output = "| "
        for node_name in nodes:
            graphite_output += "%s=%s;;;" % (node_name, nodes[node_name])

        return (0, graphite_output)

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

    parser.add_argument('--mode', choices = ['search', 'cluster-health', 'indices-stats', 'nodes-stats', 'all-stats'], help = 'operation mode', type = str, default = 'search')
    parser.add_argument('--query', help='[search mode] JSON string with elasticsearch query', type=str, default='{}', nargs='+')
    parser.add_argument('--index', help='[search mode] index name to query', type=str, default='*')

    parser.add_argument('-w', '--warning', help='number of entries needed to throw a WARNING', type=str, default=None)
    parser.add_argument('-c', '--critical', help='number of entries needed to throw a CRITICAL', type=str, default=None)
    parser.add_argument('-e', '--error-return-status', help=('status to return on connection or index error:'
                        ' 0 - OK, 1 - WARNING, 2 - CRITICAL, 3 - UNKNOWN'), type=int, default=3)
    parser.add_argument('--fields-to-be-returned', help='[search mode] fields to return (separated by ",")', type=str, default=None)
    parser.add_argument('--fields-to-be-deleted', help='[search mode] fields to delete from return output (separated by ",")', type=str, default=None)

    args = parser.parse_args()

    Checker(args).perform_check()
