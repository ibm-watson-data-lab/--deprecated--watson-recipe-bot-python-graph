from graph_db_ops import GraphDbOps


class RecipeGraph(object):
    def __init__(self, credentials, logger):
        self.logger = logger
        self.db_ops = GraphDbOps(credentials, logger)

    def init_graph(self):
        self.logger.info("Getting Graph Schema...")
        self.db_ops.log_in()
        schema = self.db_ops.get_schema()
        schema_exists = (schema is not None and schema['propertyKeys'] is not None and len(schema['propertyKeys']) > 0)
        if not schema_exists:
            self.logger.info("Creating Graph Schema...")
            schema = {
                'propertyKeys': [
                    {'name': 'name', 'dataType': 'String', 'cardinality': 'SINGLE'},
                    {'name': 'title', 'dataType': 'String', 'cardinality': 'SINGLE'},
                    {'name': 'detail', 'dataType': 'String', 'cardinality': 'SINGLE'}
                ],
                'vertexLabels': [
                    {'name': 'person'},
                    {'name': 'ingredient'},
                    {'name': 'cuisine'},
                    {'name': 'recipe'}
                ],
                'edgeLabels': [
                    {'name': 'selects'}
                ],
                'vertexIndexes': [
                    {'name': 'vertexByName', 'propertyKeys': ['name'], 'composite': True, 'unique': True}
                ],
                'edgeIndexes': []
            }
            self.db_ops.save_schema(schema)
        else:
            self.logger.info("Graph Schema exists.")

    def add_user_vertex(self, user_id):
        vertex = {
            'label': 'person',
            'properties': {
                'name': user_id
            }
        }
        return self.add_vertex_if_not_exists(vertex, 'name')
    
    def add_vertex_if_not_exists(self, vertex, unique_property_name):
        property_value = vertex['properties'][unique_property_name]
        query = 'g.V().hasLabel("{}").has("{}", "{}")'.format(vertex['label'], unique_property_name, property_value)
        response = self.db_ops.execute_gremlin_query(query)
        if len(response) > 0:
            self.logger.info("Returning {} vertex where {}={}".format(vertex['label'], unique_property_name, property_value))
            return response[0]
        else:
            self.logger.info("Creating {} vertex where {}={}".format(vertex['label'], unique_property_name, property_value))
            return self.db_ops.add_or_update_vertex(vertex)