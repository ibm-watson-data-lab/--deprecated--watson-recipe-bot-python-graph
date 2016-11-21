import json
import os
import time
from dotenv import load_dotenv
from ibm_graph import IBMGraphClient
from slackclient import SlackClient
from watson_developer_cloud import ConversationV1

from souschef.recipe import RecipeClient
from souschef.recipe_graph import RecipeGraph
from souschef.souschef import SousChef

if __name__ == "__main__":
  load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

  bot_id = os.environ.get("SLACK_BOT_ID")
  conversation_workspace_id = os.environ.get("CONVERSATION_WORKSPACE_ID")

  slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))

  conversation_client = ConversationV1(
    username=os.environ.get("CONVERSATION_USERNAME"),
    password=os.environ.get("CONVERSATION_PASSWORD"),
    version='2016-07-11')

  recipe_client = RecipeClient(os.environ.get("SPOONACULAR_KEY"))

  recipe_graph = RecipeGraph(IBMGraphClient(
      os.environ.get("GRAPH_API_URL"),
      os.environ.get("GRAPH_USERNAME"),
      os.environ.get("GRAPH_PASSWORD"))
  )

  souschef = SousChef(recipe_graph,
                      bot_id,
                      slack_client,
                      conversation_client,
                      conversation_workspace_id,
                      recipe_client)
  souschef.run()