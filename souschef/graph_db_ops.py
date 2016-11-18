from http_ops import HttpOps
import json
import sys

class GraphDbOps(object):

    def __init__(self, creds, logger):
        self.logger = logger
        self.http_ops = HttpOps(creds, logger)
        self.query_prefix = "def gt = graph.traversal(); "
        self.vertices = {}
        self.edges = {}

    def log_in(self):
        resp = self.http_ops.log_in()
        if resp is None:
            self.logger.error('An error occurred while logging in... exiting...')
            sys.exit(1)

    def get_schema(self):
        resp = self.http_ops.do_get('/schema')
        self.logger.info('schema is: "{0}"'.format(resp))
        return resp['result']['data'][0]

    def save_schema(self, schema):
        self.add_or_update_element(schema, 'schema', 'schema', None)

    def add_or_update_vertex(self, vertex, id=None):
        vertex = self.add_or_update_element(vertex, 'vertex', 'vertices', id)
        if vertex is not None:
            if vertex['id'] not in self.vertices:
                self.vertices[vertex['id']] = vertex
            return vertex
        return None

    def add_or_update_edge(self, edge, id=None):
        edge = self.add_or_update_element(edge, 'edge', 'edges', id)
        if edge is not None:
            if edge['id'] not in self.edges:
                self.edges[edge['id']] = edge
            return edge
        return None

    def add_or_update_element(self, element, element_type, element_endpoint, id=None):
        if id is not None:
            self.logger.log_message_in_pretty('Updating {0} {1}'.format(element_type, element))
            element_endpoint = '/{0}/{1}'.format(element_endpoint, id)
        else:
            self.logger.log_message_in_pretty('Adding {0} {1}'.format(element_type, element))
            element_endpoint = '/{0}'.format(element_endpoint)
        resp = self.http_ops.do_post(element_endpoint, element)
        if resp is not None and 'result' in resp:
            self.logger.info(resp)
            if resp and 'id' in resp['result']['data'][0]:
                element['id'] = resp['result']['data'][0][u'id']
            if id is not None:
                self.logger.info('{0} updated {1}\n'.format(element_type, element))
            else:
                self.logger.info('{0} added {1}\n'.format(element_type, element))
            return element
        return None

    def delete_vertex(self, id):
        self.delete_element('vertex', 'vertices', id)

    def delete_edge(self, id):
        self.delete_element('edge', 'edges', id)

    def delete_element(self, element_type, element_endpoint, id):
        self.logger.log_message_in_pretty('Deleting {0} by id {1}'.format(element_type, id))
        resp = self.http_ops.do_delete('/{0}/{1}'.format(element_endpoint, id))
        self.logger.info('{0} deleted: {1}'.format(element_type, resp))

    def execute_gremlin_query(self, query_str, exclude_prefix=False):
        self.logger.log_message_in_pretty('Executing query: {0}'.format(query_str))
        query_json = {"gremlin": self.query_prefix + query_str}
        if exclude_prefix == True:
            query_json = {"gremlin": query_str}
        resp = self.http_ops.do_post('/gremlin', query_json)
        self.logger.info('{0}'.format(resp))

    def bulkload_graphson(self):
        resp = self.http_ops.do_post(
            '/bulkload/graphson',
            file_path='./data/nxnw_dataset_v3.json'
        )
        if resp is not None and 'result' in resp:
            self.logger.info(resp['result'])

    def bulkload_graphson(self, filepath):
        resp = self.http_ops.do_post(
            '/bulkload/graphson',
            file_path=filepath
        )
        if resp is not None and 'result' in resp:
            self.logger.info(resp['result'])
