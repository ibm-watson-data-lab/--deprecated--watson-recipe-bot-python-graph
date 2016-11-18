from graph_db_ops import GraphDbOps


class RecipeGraph(object):
    def __init__(self, credentials, logger):
        self.logger = logger
        self.db_ops = GraphDbOps(credentials, logger)

    def init_graph(self):
        self.db_ops.log_in()
        schema = self.db_ops.get_schema()
        schema_exists = (schema is not None and schema['propertyKeys'] is not None and len(schema['propertyKeys']) > 0)
        if not schema_exists:
            self.logger.info('schema does not exist')
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
            self.logger.info('schema exists')