import requests
import datetime
from slack_sdk import WebClient

class Slack:
    def __init__(self, token):
        self.client = WebClient(token=token)

    def post_blocks_message(self, date, header: str, text: str, image_url = '', push_text='Notification', reciever ='', context_mark='Analytics Report', channel_name='reports-testing', thread_ts=0.0):
        '''
        Function sents typical message to Slack channel that was defined via webhook by initialization object.
        Args:
            - date: report date that will be displayed after report name (by default it will be today - 1)
            - push_text: message that will be displayed after slack notification push
            - header: usually the name of the report that will be displayed in the first row
            - reciever: if you want you can define reciever for report. He will be mentioned in Slack channel.
            - text: the main body of the message
            - image_url: if you want to attach image you shoud pass url to this image
            - context_mark: mark in context block with type of message (report, alert and etc.)
            - channel_name: the name of slack channel for message
            - thread_ts: timestamp of parent message to send message in thread
        '''
        channel_map = {
            "reports-testing":"G07Q1MZG02Y",
            "airflow-alerts":"G07PJLMGEP9"
        }
        channel_id = channel_map.get(channel_name)

        date = datetime.date.today() - datetime.timedelta(days=1) if date is None else date
        try:
            date_str = date.strftime("%B %d, %Y")
        except:
            date_str = date
        if reciever == '':
            context_text = f'*{date_str}*  |  {context_mark}'
        else:
            context_text = f'*{date_str}*  |  {context_mark} \n Reciever: <{reciever}>'

        push_text = f"{push_text}"
        blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"{header}"
                    }
                },
                {
                    "type": "context",
                    "elements": [
                        {
                            "text": f"{context_text}",
                            "type": "mrkdwn"
                        }
                    ]
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"{text}"
                    }
                }
        ]

        if image_url != '':
            image_block = {
                "type": "image",
                "title": {
                    "type": "plain_text",
                    "text": f"{header} - {date}",
                    "emoji": True
                },
                "image_url": f"{image_url}",
                "alt_text": "image1"
            }
            blocks.append(image_block)

        if thread_ts == 0.0:
            result = self.client.chat_postMessage(
                channel=channel_id,
                blocks=blocks,
                text=push_text
            )
        else:
            result = self.client.chat_postMessage(
                channel=channel_id,
                blocks=blocks,
                text=push_text,
                thread_ts=thread_ts
            )

        return result