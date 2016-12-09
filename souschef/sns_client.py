import httplib
import json


class SNSClient(object):

    def __init__(self, api_url, api_key):
        self.api_url = api_url
        self.api_key = api_key
        url = self.api_url
        index = self.api_url.find('://')
        if index > 0:
            url = self.api_url[self.api_url.find('://')+3:]
        index = url.find('/')
        if index > 0:
            self.base_url = url[0:index]
        else:
            self.base_url = url

    def post_start_message(self, state):
        self.post_message('start', state, '{} started a new conversation.'.format(state.user_id))

    def post_favorites_message(self, state):
        self.post_message('favorites', state, '{} requested their favorite recipes.'.format(state.user_id))

    def post_ingredient_message(self, state, ingredient_str):
        self.post_message('ingredient', state, '{} requested recipes for ingredient \'{}\'.'.format(state.user_id, ingredient_str))

    def post_cuisine_message(self, state, cuisine_str):
        self.post_message('ingredient', state, '{} requested recipes for cuisine \'{}\'.'.format(state.user_id, cuisine_str))

    def post_recipe_message(self, state, recipe_id, recipe_title):
        self.post_message('ingredient', state, '{} selected recipe \'{}\'.'.format(state.user_id, recipe_title), recipe_id)

    def post_message(self, action, state, message, recipe_id=None):
        path = '/notification'
        ingredient = None
        cuisine = None
        if state.ingredient_cuisine is not None:
            if state.ingredient_cuisine.label == 'ingredient':
                ingredient = state.ingredient_cuisine.get_property_value('name')
            else:
                cuisine = state.ingredient_cuisine.get_property_value('name')
        body = json.dumps({
            'notification': {
                'type': 'action',
                'action': action,
                'message': message,
                'state': {
                    'user': state.user_id,
                    'ingredient': ingredient,
                    'cuisine': cuisine,
                    'recipe': recipe_id
                }
            }
        })
        self.do_http_post(path, body)

    def do_http_post(self, path, body=''):
        return self.do_http_post_url('/{}{}'.format(self.api_key, path), body)

    def do_http_post_url(self, url, body=''):
        conn = httplib.HTTPConnection(self.base_url)
        conn.request('POST', url, body, headers={
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        })
        response = conn.getresponse()
        data = response.read()
        conn.close()
        return json.loads(data)
