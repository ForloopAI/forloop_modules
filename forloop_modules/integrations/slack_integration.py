import slack

import forloop_modules.flog as flog

from slack.errors import SlackApiError
#from config.config import other_config


def post_message_to_slack(client, channel_id: str, text: str = 'Test message'):
    try:
        # Call the conversations.list method using the WebClient
        result = client.chat_postMessage(
            channel=channel_id,
            text=text
        )

        flog.info(result)

    except SlackApiError as e:
        flog.error(f"Error: {e}")


def post_file_in_slack_channel(client, file_name, channel_id: str,
                               comment: str = "Hey, let's share! :robot_face:"):
    try:
        # Call the files.upload method using the WebClient
        # Uploading files requires the `files:write` scope
        result = client.files_upload(
            channels=channel_id,
            initial_comment=comment,
            file=file_name,
        )

        flog.info(result)

    except SlackApiError as e:
        flog.error(f"Error uploading file: {e}")

def get_channels_in_workspace(slack_token):
    client = slack.WebClient(token=slack_token) #other_config.SLACK_TOKEN

    try:
        response = client.conversations_list()['channels']
    except Exception as e:
        flog.error(f"Error retrieving channels from slack workspace: {e}")
        response = None

    if response is not None:
        channels = [x['name'] for x in response]
    else:
        channels = []

    return channels

def send_message_to_slack_direct_execute(slack_token, channel_name, text, file_name=None, *args):
    client = slack.WebClient(token=slack_token) #other_config.SLACK_TOKEN

    response = client.conversations_list()['channels']
    channel_dict = {}

    for channel in response:
        channel_dict[channel['name']] = channel['id']

    channel_id = channel_dict[channel_name]

    if file_name is not None:
        post_file_in_slack_channel(client, file_name, channel_id, text)
    else:
        post_message_to_slack(client, channel_id, text)

