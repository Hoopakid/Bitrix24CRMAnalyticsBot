import datetime, math, requests

from fast_bitrix24 import Bitrix
from collections import Counter
from pprint import pprint
from bitrix24 import *


def prepare_params(params, prev=""):
    ret = ""
    if isinstance(params, dict):
        for key, value in params.items():
            if isinstance(value, dict):
                if prev:
                    key = "{0}[{1}]".format(prev, key)
                ret += prepare_params(value, key)
            elif (isinstance(value, list) or isinstance(value, tuple)) and len(
                    value
            ) > 0:
                for offset, val in enumerate(value):
                    if isinstance(val, dict):
                        ret += prepare_params(
                            val, "{0}[{1}][{2}]".format(prev, key, offset)
                        )
                    else:
                        if prev:
                            ret += "{0}[{1}][{2}]={3}&".format(prev, key, offset, val)
                        else:
                            ret += "{0}[{1}]={2}&".format(key, offset, val)
            else:
                if prev:
                    ret += "{0}[{1}]={2}&".format(prev, key, value)
                else:
                    ret += "{0}={1}&".format(key, value)
    return ret


def create_batch(method, params: dict = {}):
    params['start'] = 0
    r = requests.get(f'https://salesdoctor.bitrix24.kz/rest/625/lz6ee3mzjf86lsu4/{method}',
                     params=prepare_params(params))
    record_count = r.json()['total']
    params['start'] = -1
    batches = []
    cmds = {}
    for i in range(math.ceil(record_count / 50)):
        if i >= 50:
            batches.append({'halt': 0, 'cmd': cmds})
            cmds = {}
        params['start'] = i * 50
        filter_param = prepare_params(params)
        cmds[f'get_{i}'] = f'{method}?{filter_param}'
    batches.append({'halt': 0, 'cmd': cmds})
    return batches


def filter_date(items: dict, date_key: str, date: str):
    date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
    r = []
    for i, item in enumerate(items):
        item_date = datetime.datetime.strptime(item[date_key], '%Y-%m-%dT%H:%M:%S%z').date()
        if item_date >= date:
            r.append(item)
    return r


def get_deals_fast(detailed=True):
    today_date = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    btx = Bitrix('https://salesdoctor.bitrix24.kz/rest/625/lz6ee3mzjf86lsu4/', verbose=False)
    batches = create_batch('crm.deal.list', params={'filter': {'>DATE_MODIFY': today_date, 'CATEGORY_ID': 71},
                                                    'select': [
                                                        'ID',
                                                        'TITLE',
                                                        'ASSIGNED_BY_ID',
                                                        'DATE_CREATE',
                                                        'DATE_MODIFY',
                                                        'STAGE_ID',
                                                        'OPPORTUNITY',
                                                        'CATEGORY_ID',
                                                        'UF_CRM_1707827201374',  # Qayerdan keldi
                                                        'UF_CRM_60127B31D80DE',  # Region
                                                        'UF_CRM_62EE15779ACBB',  # Lavozimi
                                                        'UF_CRM_1707130652726',  # Xodimlar soni
                                                        'UF_CRM_1707828822084',  # Biznes yo'nalishi
                                                        'UF_CRM_1707987959305',  # Mijoz ismi
                                                        'UF_CRM_1708671509796',  # Nomer pro
                                                    ]})
    l = []
    for ind, b in enumerate(batches):
        r: dict = btx.call_batch(b)
        for i in r.values():
            l.extend(i)
        print(f"{ind + 1}/{len(batches)}")

    if not detailed:
        return l

    stages = btx.get_all("crm.status.list")
    stage_dict = {stage["STATUS_ID"]: stage["NAME"] for stage in stages}

    users = btx.get_all("user.get")
    user_dict = {
        user["ID"]: f"{user['NAME']} {user.get('LAST_NAME', '')}" for user in users
    }

    sources = btx.get_all(
        "crm.deal.userfield.list", params={"FIELD_NAME": 'UF_CRM_1707827201374'}
    )[0]["LIST"]

    opportunities = []
    for i in l:
        i["STAGE"] = stage_dict.get(i["STAGE_ID"])
        i["ASSIGNED_BY"] = user_dict.get(i["ASSIGNED_BY_ID"])
        i["SOURCE"] = ""
        for j in sources:
            if j["ID"] == str(i['UF_CRM_1707827201374']):
                i["SOURCE"] = j["VALUE"]
        del i["STAGE_ID"], i['UF_CRM_1707827201374']

    return l


def get_datas():
    deals = get_deals_fast()
    user_data = {}
    for deal in deals:
        user_id = deal['ASSIGNED_BY_ID']
        opportunity = user_data.get(user_id, {}).get('OPPORTUNITY', 0)
        user_data[user_id] = {
            'OPPORTUNITY': opportunity + float(deal['OPPORTUNITY']) if deal['OPPORTUNITY'] is not None else 0,
        }
    today_date = datetime.datetime.today().date()

    params = {
        'filter': {
            'TYPE_ID': 2, 'OWNER_TYPE_ID': 2, '<CREATED': str(today_date)
        },
        'select': ['ID', 'OWNER_ID', 'CREATED', 'AUTHOR_ID', 'END_TIME']
    }

    batches = create_batch('crm.activity.list', params)
    btx = Bitrix('https://salesdoctor.bitrix24.kz/rest/625/lz6ee3mzjf86lsu4/')
    calls = []
    for batch in batches:
        r = btx.call_batch(batch)
        if isinstance(r, dict):
            for i in r.values():
                calls.extend(i)
    grouped_calls = {}
    for i in calls:
        if not grouped_calls.get(i['AUTHOR_ID']):
            grouped_calls[i['AUTHOR_ID']] = []
        end_time = datetime.datetime.strptime(i['END_TIME'], '%Y-%m-%dT%H:%M:%S%z')
        created_time = datetime.datetime.strptime(i['CREATED'], '%Y-%m-%dT%H:%M:%S%z')
        i['DIFFERENCE'] = (end_time - created_time).seconds
        grouped_calls[i['AUTHOR_ID']].append(i)
    for key, val in grouped_calls.items():
        if key in user_data.keys():
            grouped_calls[key][0]['OPPORTUNITY'] = user_data[key]['OPPORTUNITY']

    final_data = {}
    btx = Bitrix24('https://salesdoctor.bitrix24.kz/rest/625/lz6ee3mzjf86lsu4/')
    for key, val in grouped_calls.items():
        user_name = btx.callMethod('user.get', id=key)[0]['NAME']
        durations = [activity['DIFFERENCE'] for activity in val]
        duration_counts = Counter(durations)
        successful_count = duration_counts.get(0, 0)
        unsuccessful_count = sum(duration_counts.values()) - successful_count
        difference = sum(durations)
        qarz_data = 70 - len(durations)
        qarz = qarz_data if qarz_data >= 0 else 0
        opportunity = val[0]['OPPORTUNITY']

        final_data[user_name] = {
            'successful_calls': successful_count,
            'unsuccessful_calls': unsuccessful_count,
            'all_call_durations': difference,
            'qarz_calls': qarz,
            'opportunity': opportunity
        }

    return final_data
