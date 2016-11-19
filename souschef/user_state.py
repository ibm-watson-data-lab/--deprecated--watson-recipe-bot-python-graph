class UserState(object):
    def __init__(self, user_id):
        self.user_id = user_id
        self.conversation_context = {}
        self.user_vertex = None
        self.ingredient_cuisine_vertex = None

