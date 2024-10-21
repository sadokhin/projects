import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patheffects as path_effects
from datetime import datetime

from airflow.hooks.base_hook import BaseHook
#from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.slack.operators.slack import SlackAPIPostOperator

def hex_to_string(hex):
    '''
    Function for transform hex data in bytes into python string
    '''
    if hex[:2] == '0x':
        hex = hex[2:]
    string_value = bytes.fromhex(hex).decode('utf-8')
    return string_value

def add_labels_boxplot(ax: plt.Axes, fmt: str = ".3f") -> None:
    """
    Add text labels to the median and whiskers lines of a seaborn boxplot.
    Args:
        - ax: plt.Axes, e.g. the return value of sns.boxplot()
        - fmt: format string for the value
    """
    # Get all lines from subplot (for example for 4 boxplots in 1 subplot, each boxplot contains 5 metrics 
    # (1 - q1, 2 - q2, 3 - bottom of whisker, 4 - top of whisker, 5 - median). So in example we will have 20 lines in one Ax (subplot)
    lines = ax.get_lines()
    boxes = [c for c in ax.get_children() if "Patch" in str(c)]
    start = 4
    if not boxes:  # seaborn v0.13 => fill=False => no patches => +1 line
        boxes = [c for c in ax.get_lines() if len(c.get_xdata()) == 5]
        start += 1
    lines_per_box = len(lines) // len(boxes)
    # Median start in the 5 position and repeated each 5 lines
    for median in lines[start::lines_per_box]:
        x, y = (data.mean().round(3) for data in median.get_data())
        value = x if len(set(median.get_xdata())) == 1 else y
        text = ax.text(x, y, f'{value:{fmt}}', ha='center', va='center',
                color='black')
        # create median-colored border around white text for contrast
        text.set_path_effects([
            path_effects.Stroke(linewidth=2, foreground=median.get_color()),
            path_effects.Normal(),
        ])
    # Bottom whisker start in the 3 position and repeated each 5 lines
    for iqrq1 in lines[2::5]:
        x, y = (data.mean().round(3) for data in iqrq1.get_data())
        # add customization to display label less then whisker
        y = y - 0.2
        value = x if len(set(median.get_xdata())) == 1 else y
        text = ax.text(x, y, f'{value:{fmt}}', ha='center', va='center',
                color='black')
    # Top whisker start in the 4 position and repeated each 5 lines
    for iqrq3 in lines[3::5]:
        x, y = (data.mean().round(3) for data in iqrq3.get_data())
        # add customization to display label higher then whisker
        y = y + 0.1
        value = x if len(set(median.get_xdata())) == 1 else y
        text = ax.text(x, y, f'{value:{fmt}}', ha='center', va='center',
                color='black')
    
    return True

def task_fail_slack_alert(context):
    '''
    Sending message with alert to Slack if one of the Airflow task marked as 'Failed'
    Args:
        - context: all data related to task that was Failed
    '''
    # Get the name of the failed task
    task_tuple=context.get('task_instance').task_id,
    task = task_tuple[0] if isinstance(task_tuple, tuple) else task_tuple

    # Get the name of the failed DAG
    dag_tuple=context.get('task_instance').dag_id,
    dag = dag_tuple[0] if isinstance(dag_tuple, tuple) else dag_tuple

    # Get Owner of the DAG to mention in alert
    owner_tuple = context.get('dag').owner
    owner = owner_tuple[0] if isinstance(owner_tuple, tuple) else owner_tuple

    # Get link to logs of failed task
    log_url_tuple=context.get('task_instance').log_url,
    log_url = log_url_tuple[0] if isinstance(log_url_tuple, tuple) else log_url_tuple

    # Get first 100 symbols of the error
    error=f"{str(context.get('exception'))[0:100]}..." if len(str(context.get('exception')))>100 else str(context.get('exception'))
    
    # Prepare message with alert
    attachments = [
        {
            "color":"#FF0000",
            "blocks":[
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"🚨 *TASK FAILED* 🚨 | Responsible: <{owner}>"
                    }
                },
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "fields":[
                    {
                        "type": "mrkdwn",
                        "text": f"*Dag:*\n{dag}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Task ID:*\n{task}"
                    }
                    ]
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*Error:*\n{error}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*<{log_url}|View Log Explorer>*"
                    }
                }
            ]
        },
    ]

    # Create task for Airflow and execute this task
    operator = SlackAPIPostOperator(
        task_id='send_slack_alert',
        #slack_webhook_conn_id = 'slack_hook_airflow_alerts',
        slack_conn_id = 'slack_hook_airflow_alerts',
        attachments=attachments
    )
    return operator.execute(context=context)