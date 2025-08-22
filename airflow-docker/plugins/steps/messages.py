from airflow.providers.telegram.hooks.telegram import TelegramHook
import os

def send_telegram_failfure_message(context):
    hook = TelegramHook(
        telegram_conn_id=os.environ.get('TELEGRAM_CONN_ID', 'test'),
        token=os.environ.get('TELEGRAM_TOKEN'), 
        chat_id=os.environ.get('TELEGRAM_CHAT_ID')
    )

    dag = context['dag']
    run_id = context['run_id']
    task_instance_key_str = context['task_instance_key_str']

    message = f'Исполнение DAG {dag} с id={run_id} task_instance_key_str={task_instance_key_str} прошло с ошибкой!'
    hook.send_message({
        'chat_id': '{вставьте ваш chat_id}',
        'text': message
    })
