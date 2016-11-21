import json


class RecipeGraph(object):
    def __init__(self, graph_client):
        self.graph_client = graph_client

    def init_graph(self):
        print 'Getting Graph Schema...'
        schema = self.graph_client.get_schema()
        schema_exists = (schema is not None and schema['propertyKeys'] is not None and len(schema['propertyKeys']) > 0)
        if not schema_exists:
            print 'Creating Graph Schema...'
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
            self.graph_client.save_schema(schema)
        else:
            print 'Graph Schema exists.'

    # User

    def add_user_vertex(self, user_id):
        vertex = {
            'label': 'person',
            'properties': {
                'name': user_id
            }
        }
        return self.add_vertex_if_not_exists(vertex, 'name')

    # Ingredients

    @staticmethod
    def get_unique_ingredients_name(ingredients_str):
        ingredients = [x.strip() for x in ingredients_str.lower().strip().split(',')]
        ingredients.sort()
        return ','.join([x for x in ingredients])

    def find_ingredients_vertex(self, ingredients_str):
        return self.find_vertex('ingredient', 'name', self.get_unique_ingredients_name(ingredients_str))

    def add_ingredients_vertex(self, ingredients_str, matching_recipes, user_vertex):
        ingredient_vertex = {
            'label': 'ingredient',
            'properties': {
                'name': self.get_unique_ingredients_name(ingredients_str),
                'detail': json.dumps(matching_recipes)
            }
        }
        ingredient_vertex = self.add_vertex_if_not_exists(ingredient_vertex, 'name')
        self.increment_ingredient_edge(ingredient_vertex, user_vertex)
        return ingredient_vertex

    def increment_ingredient_edge(self, ingredient_vertex, user_vertex):
        ingredient_edge = {
            'label': 'selects',
            'outV': user_vertex['id'],
            'inV': ingredient_vertex['id'],
            'properties': {
                'count': 1
            }
        }
        self.add_update_edge(ingredient_edge)

    # Cuisine

    @staticmethod
    def get_unique_cuisine_name(cuisine):
        return cuisine.strip().lower()

    def find_cuisine_vertex(self, cuisine_str):
        return self.find_vertex('cuisine', 'name', self.get_unique_cuisine_name(cuisine_str))

    def add_cuisine_vertex(self, cuisine_str, matching_recipes, user_vertex):
        cuisine_vertex = {
            'label': 'cuisine',
            'properties': {
                'name': self.get_unique_cuisine_name(cuisine_str),
                'detail': json.dumps(matching_recipes)
            }
        }
        cuisine_vertex = self.add_vertex_if_not_exists(cuisine_vertex, 'name')
        self.increment_cuisine_edge(cuisine_vertex, user_vertex)
        return cuisine_vertex

    def increment_cuisine_edge(self, cuisine_vertex, user_vertex):
        cuisine_edge = {
            'label': 'selects',
            'outV': user_vertex['id'],
            'inV': cuisine_vertex['id'],
            'properties': {
                'count': 1
            }
        }
        self.add_update_edge(cuisine_edge)

    # Recipe

    @staticmethod
    def get_unique_recipe_name(recipe_id):
        return str(recipe_id).strip().lower()

    def find_recipe_vertex(self, recipe_id):
        return self.find_vertex('recipe', 'name', self.get_unique_recipe_name(recipe_id))

    def find_recipes_for_user(self, user_id):
        query = 'g.V().hasLabel("person").has("name", "{}").outE().inV().hasLabel("recipe").path()'.format(user_id)
        response = self.graph_client.run_gremlin_query(query)
        if len(response) > 0:
            return response
        else:
            return None

    def add_recipe_vertex(self, recipe_id, recipe_title, recipe_detail, ingredient_cuisine_vertex, user_vertex):
        recipe_vertex = {
            'label': 'recipe',
            'properties': {
                'name': self.get_unique_recipe_name(recipe_id),
                'title': recipe_title.strip(),
                'detail': recipe_detail
            }
        }
        recipe_vertex = self.add_vertex_if_not_exists(recipe_vertex, 'name')
        self.increment_recipe_edges(recipe_vertex, ingredient_cuisine_vertex, user_vertex)
        return recipe_vertex

    def increment_recipe_edges(self, recipe_vertex, ingredient_cuisine_vertex, user_vertex):
        # add one edge from the user to the recipe (this will let us find a user's favorite recipes, etc)
        edge = {
            'label': 'selects',
            'outV': user_vertex['id'],
            'inV': recipe_vertex['id'],
            'properties': {
                'count': 1
            }
        }
        self.add_update_edge(edge)
        if ingredient_cuisine_vertex is not None:
            edge = {
                'label': 'selects',
                'outV': ingredient_cuisine_vertex['id'],
                'inV': recipe_vertex['id'],
                'properties': {
                    'count': 1
                }
            }
            self.add_update_edge(edge)

    # Graph Helper Methods

    def find_vertex(self, label, property_name, property_value):
        query = 'g.V().hasLabel("{}").has("{}", "{}")'.format(label, property_name, property_value)
        response = self.graph_client.run_gremlin_query(query)
        if len(response) > 0:
            return response[0]
        else:
            return None

    def add_vertex_if_not_exists(self, vertex, unique_property_name):
        property_value = vertex['properties'][unique_property_name]
        query = 'g.V().hasLabel("{}").has("{}", "{}")'.format(vertex['label'], unique_property_name, property_value)
        response = self.graph_client.run_gremlin_query(query)
        if len(response) > 0:
            print 'Returning {} vertex where {}={}'.format(vertex['label'], unique_property_name, property_value)
            return response[0]
        else:
            print 'Creating {} vertex where {}={}'.format(vertex['label'], unique_property_name, property_value)
            return self.graph_client.add_vertex(vertex)

    def add_update_edge(self, edge):
        query = 'g.V({}).outE().inV().hasId({}).path()'.format(edge['outV'], edge['inV'])
        response = self.graph_client.run_gremlin_query(query)
        if len(response) > 0:
            edge = response[0]['objects'][1]
            count = 0
            if 'count' in edge['properties'].keys():
                count = edge['properties']['count']
            edge['properties']['count'] = count + 1
            self.graph_client.update_edge(edge)
        else:
            self.graph_client.add_edge(edge)
