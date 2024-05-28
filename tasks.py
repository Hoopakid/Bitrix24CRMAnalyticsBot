import os
import requests
import pandas as pd
import datetime

from celery import Celery
from celery.schedules import crontab
from dotenv import load_dotenv

from Database.base_datas import get_calls_per_user
from informations import get_datas

load_dotenv()

app = Celery(
    'tasks',
    broker='redis://redis_clock:6379',
    backend='redis://redis_clock:6379'
)

app.conf.beat_schedule = {
    'send_message': {
        'task': 'tasks.send_message_to_user',
        'schedule': crontab(hour=14, minute=10)
    }
}

MBI_CHAT_ID = os.environ.get('MBI_CHAT_ID')
ISAYEV_CHAT_ID = os.environ.get('ISAYEV_CHAT_ID')
HASAN_CHAT_ID = os.environ.get('HASAN_CHAT_ID')
SHER_CHAT_ID = os.environ.get('SHER_CHAT_ID')
BOT_TOKEN = os.environ.get('BOT_TOKEN')

chat_ids = [int(MBI_CHAT_ID), int(ISAYEV_CHAT_ID), int(HASAN_CHAT_ID), int(SHER_CHAT_ID)]


def seconds_to_hms(seconds):
    td = datetime.timedelta(seconds=seconds)
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    seconds = td.seconds % 60
    return f"{hours:02d}", f"{minutes:02d}", f"{seconds:02d}"


@app.task()
def send_message_to_user():
    bot_calls = get_calls_per_user()
    if bot_calls != False:
        today = (datetime.datetime.today() - datetime.timedelta(days=1)).date()
        message = f"""Xodimlarning {today} kungi hisoboti\n\n"""
        for key, val in bot_calls.items():
            hours, minutes, seconds = seconds_to_hms(val['all_call_durations'])
            successful_calls = val['successful_calls']
            unsuccessful_calls = val['unsuccessful_calls']
            qarz_calls = val['qarz_calls']
            all_calls = successful_calls + unsuccessful_calls
            f_money = val.get('opportunity', 0.0)
            money = f"{f_money:,.0f}".replace(',', '.')
            message += f"""👤 *{key}*:\n  📞Barcha qong'iroqlar: {all_calls}\n  ☎️Davomiyligi: {hours}:{minutes}:{seconds}\n  ✅Ko'tarilgan qo'ngiroqlar: {successful_calls}\n  🚫Ko'tarilmagan qong'iroqlar: {unsuccessful_calls}\n  💣Qarz qo'ng'iroqlar: {qarz_calls}\n  💰Kirim: {money}\n\n"""

        url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
        for chat_id in chat_ids:
            data = {
                'chat_id': chat_id,
                'text': message,
                'parse_mode': 'Markdown'
            }
            requests.post(url, data)
        return True
