import json
import os
import pprint
import time
from slackclient import SlackClient
from recipe import RecipeClient
from user_state import UserState


class SousChef:
    def __init__(self, recipe_graph, bot_id, slack_client, conversation_client, conversation_workspace_id,
                 recipe_client):
        self.recipe_graph = recipe_graph
        self.bot_id = bot_id

        self.slack_client = slack_client
        self.conversation_client = conversation_client
        self.recipe_client = recipe_client

        self.at_bot = "<@" + bot_id + ">:"
        self.delay = 0.5  # second
        self.workspace_id = conversation_workspace_id

        self.user_state_map = {}

        self.pp = pprint.PrettyPrinter(indent=4)

    def parse_slack_output(self, slack_rtm_output):
        output_list = slack_rtm_output
        if output_list and len(output_list) > 0:
            for output in output_list:
                # print json.dumps(output)
                if output and 'text' in output and 'user_profile' not in output and self.at_bot in output['text']:
                    return output['text'].split(self.at_bot)[1].strip().lower(), output['user'], output['channel']
                elif output and 'text' in output and 'user_profile' not in output:
                    return output['text'].lower(), output['user'], output['channel']
        return None, None, None

    def post_to_slack(self, response, channel):
        self.slack_client.api_call("chat.postMessage",
                                   channel=channel,
                                   text=response, as_user=True)

    def make_formatted_steps(self, recipe_info, recipe_steps):
        response = "Ok, it takes *" + \
                   str(recipe_info['readyInMinutes']) + \
                   "* minutes to make *" + \
                   str(recipe_info['servings']) + \
                   "* servings of *" + \
                   recipe_info['title'] + "*. Here are the steps:\n\n"

        if recipe_steps and recipe_steps[0]['steps']:
            for i, r_step in enumerate(recipe_steps[0]['steps']):
                equip_str = ""
                for e in r_step['equipment']:
                    equip_str += e['name'] + ", "
                if not equip_str:
                    equip_str = "None"
                else:
                    equip_str = equip_str[:-2]

                response += "*Step " + str(i + 1) + "*:\n" + \
                            "_Equipment_: " + equip_str + "\n" + \
                            "_Action_: " + r_step['step'] + "\n\n"
        else:
            response += "_No instructions available for this recipe._\n\n"

        response += "*Say anything to me to start over...*"
        return response

    def handle_start_message(self, state, watson_response):
        if state.user_vertex is None:
            user_vertex = self.recipe_graph.add_user_vertex(state.user_id)
            state.user_vertex = user_vertex
        response = ''
        for text in watson_response['output']['text']:
            response += text + "\n"
        return response

    def handle_favorites_message(self, state):
        matching_recipes = []
        paths = self.recipe_graph.find_recipes_for_user(state.user_id)
        if paths is not None and len(paths) > 0:
            paths.sort(key=lambda x: x['objects'][1]['properties']['count'], reverse=True)
            for path in paths:
                matching_recipes.append({
                    'id': path['objects'][2]['properties']['name'][0]['value'],
                    'title': path['objects'][2]['properties']['title'][0]['value']
                })
        # update state
        state.conversation_context['recipes'] = matching_recipes
        state.ingredient_cuisine_vertex = None
        # return the response
        response = "Lets see here...\n" + \
                   "I've found these recipes: \n"

        for i, recipe in enumerate(state.conversation_context['recipes']):
            response += str(i + 1) + ". " + recipe['title'] + "\n"
        response += "\nPlease enter the corresponding number of your choice."

        return response

    def handle_ingredients_message(self, state, message):

        # we want to get a list of recipes based on the ingredients (message)
        # first we see if we already have the ingredients in our graph
        ingredients_str = message
        ingredient_vertex = self.recipe_graph.find_ingredients_vertex(ingredients_str)
        if ingredient_vertex is not None:
            print "Ingredients vertex exists for {}. Returning recipes from vertex.".format(ingredients_str)
            matching_recipes = json.loads(ingredient_vertex['properties']['detail'][0]['value'])
            # increment the count on the user-ingredient edge
            self.recipe_graph.increment_ingredient_edge(ingredient_vertex, state.user_vertex)
        else:
            # we don't have the ingredients in our graph yet, so get list of recipes from Spoonacular
            print "Ingredients vertex does not exist for {}. Querying Spoonacular for recipes.".format(ingredients_str)
            matching_recipes = self.recipe_client.find_by_ingredients(ingredients_str);
            # add vertex for the ingredients to our graph
            ingredient_vertex = self.recipe_graph.add_ingredients_vertex(ingredients_str, matching_recipes, state.user_vertex)

        state.conversation_context['recipes'] = matching_recipes
        state.ingredient_cuisine_vertex = ingredient_vertex

        response = "Lets see here...\n" + \
                   "I've found these recipes: \n"

        for i, recipe in enumerate(state.conversation_context['recipes']):
            response += str(i + 1) + ". " + recipe['title'] + "\n"
        response += "\nPlease enter the corresponding number of your choice."

        return response

    def handle_cuisine_message(self, state, cuisine):
        
        # we want to get a list of recipes based on the cuisine
        # first we see if we already have the cuisine in our graph
        cuisine_vertex = self.recipe_graph.find_cuisine_vertex(cuisine)
        if cuisine_vertex is not None:
            print "Cuisine vertex exists for {}. Returning recipes from vertex.".format(cuisine)
            matching_recipes = json.loads(cuisine_vertex['properties']['detail'][0]['value'])
            # increment the count on the user-cuisine edge
            self.recipe_graph.increment_cuisine_edge(cuisine_vertex, state.user_vertex)
        else:
            # we don't have the cuisine in our graph yet, so get list of recipes from Spoonacular
            print "Cuisine vertex does not exist for {}. Querying Spoonacular for recipes.".format(cuisine)
            matching_recipes = self.recipe_client.find_by_cuisine(cuisine);
            # add vertex for the cuisine to our graph
            cuisine_vertex = self.recipe_graph.add_cuisine_vertex(cuisine, matching_recipes, state.user_vertex)

        state.conversation_context['recipes'] = matching_recipes
        state.ingredient_cuisine_vertex = cuisine_vertex

        response = "Lets see here...\n" + \
                   "I've found these recipes: \n"

        for i, recipe in enumerate(state.conversation_context['recipes']):
            response += str(i + 1) + ". " + recipe['title'] + "\n"
        response += "\nPlease enter the corresponding number of your choice."

        return response

    def handle_selection_message(self, state, selection):

        if 1 <= selection <= 5:
            # we want to get a the recipe based on the selection
            # first we see if we already have the recipe in our graph
            recipes = state.conversation_context['recipes']
            recipe_id = recipes[selection-1]['id']
            recipe_vertex = self.recipe_graph.find_recipe_vertex(recipe_id)
            if recipe_vertex is not None:
                print "Recipe vertex exists for {}. Returning recipe steps from vertex.".format(recipe_id)
                recipe_detail = recipe_vertex['properties']['detail'][0]['value']
                # increment the count on the ingredient/cuisine-recipe edge and the user-recipe edge
                self.recipe_graph.increment_recipe_edges(recipe_vertex, state.ingredient_cuisine_vertex, state.user_vertex)
            else:
                print "Recipe vertex does not exist for {}. Querying Spoonacular for details.".format(recipe_id)
                recipe_info = self.recipe_client.get_info_by_id(recipe_id)
                recipe_steps = self.recipe_client.get_steps_by_id(recipe_id)
                recipe_detail = self.make_formatted_steps(recipe_info, recipe_steps);
                # add vertex for recipe
                self.recipe_graph.add_recipe_vertex(recipe_id, recipe_info['title'], recipe_detail, state.ingredient_cuisine_vertex, state.user_vertex)
            # clear out state
            state.ingredient_cuisine_vertex = None
            state.conversation_context = None
            # return response
            return recipe_detail
        else:
            state.conversation_context['selection_valid'] = False
            return "Invalid selection! Say anything to see your choices again...";

    def handle_message(self, message, message_sender, channel):

        # get or create state for the user
        if message_sender in self.user_state_map.keys():
            state = self.user_state_map[message_sender]
        else:
            state = UserState(message_sender)
            self.user_state_map[message_sender] = state

        watson_response = self.conversation_client.message(
            workspace_id=self.workspace_id,
            message_input={'text': message},
            context=state.conversation_context)

        # print json.dumps(watson_response)

        state.conversation_context = watson_response['context']

        if 'is_favorites' in state.conversation_context.keys() and state.conversation_context['is_favorites']:
            response = self.handle_favorites_message(state)

        elif 'is_ingredients' in state.conversation_context.keys() and state.conversation_context['is_ingredients']:
            response = self.handle_ingredients_message(state, message)

        elif 'is_selection' in state.conversation_context.keys() and state.conversation_context['is_selection']:
            state.conversation_context['selection_valid'] = False
            response = "Invalid selection! " + \
                       "Say anything to see your choices again..."

            if state.conversation_context['selection'].isdigit():
                selection = int(state.conversation_context['selection'])
                response = self.handle_selection_message(state, selection)

        elif watson_response['entities'] and watson_response['entities'][0]['entity'] == 'cuisine':
            cuisine = watson_response['entities'][0]['value']
            response = self.handle_cuisine_message(state, cuisine)

        else:
            response = self.handle_start_message(state, watson_response)

        self.post_to_slack(response, channel)

    def run(self):
        self.recipe_graph.init_graph()
        if self.slack_client.rtm_connect():
            print("sous-chef is connected and running!")
            while True:
                slack_output = self.slack_client.rtm_read()
                message, message_sender, channel = self.parse_slack_output(slack_output)
                if message and channel:
                    self.handle_message(message, message_sender, channel)
                time.sleep(self.delay)
        else:
            print("Connection failed. Invalid Slack token or bot ID?")
