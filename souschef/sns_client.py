import httplib
import json

from Queue import Queue
from threading import Thread


class SNSClient(object):

    def __init__(self, api_url, api_key):
        if api_url is None:
            self.enabled = False
        else:
            self.enabled = True
            self.api_url = api_url
            self.api_key = api_key
            url = self.api_url
            index = self.api_url.find('://')
            if index > 0:
                url = self.api_url[index+3:]
            index = url.find('/')
            if index > 0:
                self.base_url = url[0:index]
            else:
                self.base_url = url
            self.queue_thread_count = 10
            self.queue = Queue(self.queue_thread_count)
            for i in range(self.queue_thread_count):
                worker = Thread(target=self.do_http_post_from_queue)
                worker.setDaemon(True)
                worker.start()

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
        # if sns not enabled then return
        if not self.enabled:
            return
        ingredient = None
        cuisine = None
        if state.ingredient_cuisine is not None:
            if state.ingredient_cuisine.label == 'ingredient':
                ingredient = state.ingredient_cuisine.get_property_value('name')
            else:
                cuisine = state.ingredient_cuisine.get_property_value('name')
        body = json.dumps({
            'userQuery': {
                'type': 'action'
            },
            'notification': {
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
        self.queue.put(body)

    def do_http_post_from_queue(self):
        while True:
            body = self.queue.get()
            self.do_http_post('/notification', body)
            self.queue.task_done()

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
