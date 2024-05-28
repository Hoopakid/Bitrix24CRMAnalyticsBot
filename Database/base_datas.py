from datetime import datetime, timedelta

import psycopg2
import os
from psycopg2 import extras
from dotenv import load_dotenv

load_dotenv()

DB_NAME = os.environ.get('DB_NAME')
DB_HOST = os.environ.get('DB_HOST')
DB_PORT = os.environ.get('DB_PORT')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')


def connection():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD
    )
    return conn


def get_opportunity_per_user_id():
    conn = None
    try:
        conn = connection()
        cursor = conn.cursor(cursor_factory=extras.DictCursor)
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        cursor.execute(
            "SELECT assigned_by_id, SUM(opportunity) AS total_opportunity FROM deal WHERE date_create BETWEEN %s AND %s GROUP BY assigned_by_id",
            (yesterday, today)
        )
        result = cursor.fetchall() if cursor.fetchall() else 0
        if result == 0:
            return False
        return {row['assigned_by_id']: row['total_opportunity'] for row in result}
    finally:
        if conn:
            conn.close()


def get_calls_per_user():
    conn = None
    try:
        conn = connection()
        cursor = conn.cursor(cursor_factory=extras.DictCursor)
        opportunity_data = get_opportunity_per_user_id()
        if opportunity_data == False:
            return False
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday = today - timedelta(days=1)
        cursor.execute(
            """
            SELECT author_id, start_time, end_time
            FROM activity
            WHERE created BETWEEN %s AND %s AND type_id::integer = %s
            """,
            (yesterday, today, 2)
        )
        result = cursor.fetchall()
        aggregated_data = {}
        for row in result:
            author_id = row['author_id']
            start_time = row['start_time']
            end_time = row['end_time']
            duration = (end_time - start_time).total_seconds()
            if author_id not in aggregated_data:
                aggregated_data[author_id] = {'all_call_durations': 0, 'successful_calls': 0, 'unsuccessful_calls': 0,
                                              'all_calls': 0}
            aggregated_data[author_id]['all_calls'] += 1
            aggregated_data[author_id]['all_call_durations'] += duration
            if duration > 0:
                aggregated_data[author_id]['successful_calls'] += 1
            else:
                aggregated_data[author_id]['unsuccessful_calls'] += 1
        for key, val in aggregated_data.items():
            aggregated_data[key]['qarz_calls'] = 70 - aggregated_data[key]['all_calls'] if aggregated_data[key][
                                                                                               'all_calls'] <= 70 else 0
        for author_id, data in aggregated_data.items():
            if author_id in opportunity_data:
                data['opportunity'] = opportunity_data[author_id]
        finally_data = {}
        for key, val in aggregated_data.items():
            assigned_by_id = int(key)
            cursor.execute(
                "SELECT name FROM public.user where external_id = %s", (assigned_by_id,)
            )
            user_name = cursor.fetchone()[0]
            finally_data[user_name] = val
        return finally_data
    finally:
        if conn:
            conn.close()
