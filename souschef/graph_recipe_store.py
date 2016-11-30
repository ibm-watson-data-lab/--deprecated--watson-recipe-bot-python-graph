import json

from ibm_graph import Edge
from ibm_graph import Vertex
from ibm_graph.schema import EdgeLabel
from ibm_graph.schema import PropertyKey
from ibm_graph.schema import Schema
from ibm_graph.schema import VertexIndex
from ibm_graph.schema import VertexLabel


class GraphRecipeStore(object):
    def __init__(self, graph_client):
        self.graph_client = graph_client

    def init(self):
        print 'Getting Graph Schema...'
        schema = self.graph_client.get_schema()
        schema_exists = (schema is not None and schema.property_keys is not None and len(schema.property_keys) > 0)
        if not schema_exists:
            print 'Creating Graph Schema...'
            schema = Schema([
                    PropertyKey('name', 'String', 'SINGLE'),
                    PropertyKey('title', 'String', 'SINGLE'),
                    PropertyKey('detail', 'String', 'SINGLE')
                ],
                [
                    VertexLabel('person'),
                    VertexLabel('ingredient'),
                    VertexLabel('cuisine'),
                    VertexLabel('recipe')
                ],
                [
                    EdgeLabel('selects')
                ],
                [
                    VertexIndex('vertexByName', ['name'], True, True)
                ],
                []
            )
            self.graph_client.save_schema(schema)
        else:
            print 'Graph Schema exists.'

    # User

    def add_user(self, user_id):
        vertex = Vertex('person', {
            'name': user_id
        })
        return self.add_vertex_if_not_exists(vertex, 'name')

    # Ingredients

    @staticmethod
    def get_unique_ingredients_name(ingredients_str):
        ingredients = [x.strip() for x in ingredients_str.lower().strip().split(',')]
        ingredients.sort()
        return ','.join([x for x in ingredients])

    def find_ingredient(self, ingredients_str):
        return self.find('ingredient', 'name', self.get_unique_ingredients_name(ingredients_str))

    def add_ingredient(self, ingredients_str, matching_recipes, user_vertex):
        ingredient_vertex = Vertex('ingredient', {
            'name': self.get_unique_ingredients_name(ingredients_str),
            'detail': json.dumps(matching_recipes)
        })
        ingredient_vertex = self.add_vertex_if_not_exists(ingredient_vertex, 'name')
        self.increment_ingredient_for_user(ingredient_vertex, user_vertex)
        return ingredient_vertex

    def increment_ingredient_for_user(self, ingredient_vertex, user_vertex):
        ingredient_edge = Edge('selects', user_vertex.id, ingredient_vertex.id, {
            'count': 1
        })
        self.add_update_edge(ingredient_edge)

    # Cuisine

    @staticmethod
    def get_unique_cuisine_name(cuisine):
        return cuisine.strip().lower()

    def find_cuisine(self, cuisine_str):
        return self.find('cuisine', 'name', self.get_unique_cuisine_name(cuisine_str))

    def add_cuisine(self, cuisine_str, matching_recipes, user_vertex):
        cuisine_vertex = Vertex('cuisine', {
            'name': self.get_unique_cuisine_name(cuisine_str),
            'detail': json.dumps(matching_recipes)
        })
        cuisine_vertex = self.add_vertex_if_not_exists(cuisine_vertex, 'name')
        self.increment_cuisine_for_user(cuisine_vertex, user_vertex)
        return cuisine_vertex

    def increment_cuisine_for_user(self, cuisine_vertex, user_vertex):
        cuisine_edge = Edge('selects', user_vertex.id, cuisine_vertex.id, {
            'count': 1
        })
        self.add_update_edge(cuisine_edge)

    # Recipe

    @staticmethod
    def get_unique_recipe_name(recipe_id):
        return str(recipe_id).strip().lower()

    def find_recipe(self, recipe_id):
        return self.find('recipe', 'name', self.get_unique_recipe_name(recipe_id))

    def find_recipes_for_user(self, user_id):
        query = 'g.V().hasLabel("person").has("name", "{}").outE().inV().hasLabel("recipe").path()'.format(user_id)
        response = self.graph_client.run_gremlin_query(query)
        if len(response) > 0:
            return response
        else:
            return None

    def add_recipe(self, recipe_id, recipe_title, recipe_detail, ingredient_cuisine_vertex, user_vertex):
        recipe_vertex = Vertex('recipe', {
            'name': self.get_unique_recipe_name(recipe_id),
            'title': recipe_title.strip(),
            'detail': recipe_detail
        })
        recipe_vertex = self.add_vertex_if_not_exists(recipe_vertex, 'name')
        self.increment_recipe_for_user(recipe_vertex, ingredient_cuisine_vertex, user_vertex)
        return recipe_vertex

    def increment_recipe_for_user(self, recipe_vertex, ingredient_cuisine_vertex, user_vertex):
        # add one edge from the user to the recipe (this will let us find a user's favorite recipes, etc)
        edge = Edge('selects', user_vertex.id, recipe_vertex.id, {
            'count': 1
        })
        self.add_update_edge(edge)
        if ingredient_cuisine_vertex is not None:
            edge = Edge('selects', ingredient_cuisine_vertex.id, recipe_vertex.id, {
                'count': 1
            })
            self.add_update_edge(edge)

    # Graph Helper Methods

    def find(self, label, property_name, property_value):
        query = 'g.V().hasLabel("{}").has("{}", "{}")'.format(label, property_name, property_value)
        response = self.graph_client.run_gremlin_query(query)
        if len(response) > 0:
            return response[0]
        else:
            return None

    def add_vertex_if_not_exists(self, vertex, unique_property_name):
        property_value = vertex.get_property_value(unique_property_name)
        query = 'g.V().hasLabel("{}").has("{}", "{}")'.format(vertex.label, unique_property_name, property_value)
        response = self.graph_client.run_gremlin_query(query)
        if len(response) > 0:
            print 'Returning {} vertex where {}={}'.format(vertex.label, unique_property_name, property_value)
            return response[0]
        else:
            print 'Creating {} vertex where {}={}'.format(vertex.label, unique_property_name, property_value)
            return self.graph_client.add_vertex(vertex)

    def add_update_edge(self, edge):
        query = 'g.V({}).outE().inV().hasId({}).path()'.format(edge.out_v, edge.in_v)
        response = self.graph_client.run_gremlin_query(query)
        if len(response) > 0:
            edge = response[0].objects[1]
            edge_count = edge.get_property_value('count')
            count = 0
            if edge_count is not None:
                count += edge_count
            edge.set_property_value('count', count+1)
            self.graph_client.update_edge(edge)
        else:
            self.graph_client.add_edge(edge)
