import os
import sys

from dotenv import load_dotenv
from ibm_graph import IBMGraphClient
from slackclient import SlackClient
from watson_developer_cloud import ConversationV1

from souschef.graph_recipe_store import GraphRecipeStore
from souschef.recipe import RecipeClient
from souschef.sns_client import SNSClient
from souschef.souschef import SousChef

if __name__ == "__main__":
    try:
        # load environment variables
        load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
        slack_bot_id = os.environ.get("SLACK_BOT_ID")
        slack_client = SlackClient(os.environ.get('SLACK_BOT_TOKEN'))
        conversation_workspace_id = os.environ.get("CONVERSATION_WORKSPACE_ID")
        conversation_client = ConversationV1(
            username=os.environ.get("CONVERSATION_USERNAME"),
            password=os.environ.get("CONVERSATION_PASSWORD"),
            version='2016-07-11'
        )
        recipe_client = RecipeClient(os.environ.get("SPOONACULAR_KEY"))
        recipe_store = GraphRecipeStore(
            IBMGraphClient(
                os.environ.get("GRAPH_API_URL"),
                os.environ.get("GRAPH_USERNAME"),
                os.environ.get("GRAPH_PASSWORD")
            ),
            os.environ.get("GRAPH_ID")
        )
        sns_client = SNSClient(
            os.environ.get("SNS_API_URL"),
            os.environ.get("SNS_API_KEY")
        )
        # start the souschef bot
        souschef = SousChef(slack_bot_id,
                            slack_client,
                            conversation_client,
                            conversation_workspace_id,
                            recipe_client,
                            recipe_store,
                            sns_client)
        souschef.start()
        sys.stdin.readline()
    except (KeyboardInterrupt, SystemExit):
        pass
    souschef.stop()
    souschef.join()

