class UserState(object):
    def __init__(self, user_id):
        self.user_id = user_id
        self.conversation_context = {}
        self.conversation_started = False
        self.user = None
        self.ingredient_cuisine = None
        self.recipe = None
