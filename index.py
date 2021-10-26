import os
import sys
import psycopg2
import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import date, datetime, timedelta
from dotenv import load_dotenv


def connect():

    conn = None

    dbname = os.environ['DB_NAME']
    user = os.environ['DB_USER']
    password = os.environ['DB_PASSWORD']
    host = os.environ['DB_HOST']   
    
    try:
        conn = psycopg2.connect(
            dbname = dbname,
            user = user,
            password = password,
            host = host
        )

    except Exception as error:
        print(error)

    finally:
        if conn is not None:
            return conn

def round_date(date):

    try:
        year = date.year
        month = date.month
        day = date.day
        hour = date.hour
        minute = date.minute
        if len(str(date.second)) < 2:
            second = str(date.second)

            if int(second) < 3:
                second = '0'
            elif int(second) > 2 and int(second) < 7:
                second = '5'
            else:
                second = '10'

        else:
            second = str(date.second)[1]

            if date.second > 57:
                minute += 1
                second = '0'
            elif int(second) < 3:
                second = str(date.second)[0] + '0'
            elif int(second) > 2 and int(second) < 8:
                second = str(date.second)[0] + '5'
            else:
                second = int(str(date.second)[0]) + 1
                second = str(second) + '0' 

        second = int(second)

        # datetime(year, month, day, hour, minute, second)
        date_obj = datetime(year, month, day, hour, minute, second)

        return date_obj

    except Exception as error:
        print(error)



def set_date(date):

    date_str = date.split('.')[0]
    date_str = date_str.replace('Z', '')
    date_time_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
    date_time_obj = round_date(date_time_obj)    
    hours = timedelta(hours=3)
    date_time_obj = date_time_obj - hours
    date_time = date_time_obj.strftime('%Y-%m-%dT%H:%M:%S-03:00')

    return date_time



def set_processes(data, tag):

    if len(data) != 0:

        date = set_date(data[0]['Timestamp'])
        update_data_log(date, tag)
        registers = []

        for d in data:

            datetime_read = set_date(d['Timestamp'])

            registers.append({
                'capture_id':tag,
                'datetime_read':datetime_read,
                'p_value':d['Value']
            })

            if len(registers) == 200:
                send_registers(registers)
                registers.clear()

        send_registers(registers)

    else:
        last_register = get_last_processes(tag)
        insert_data_log(last_register, tag)



def set_processes_filters(data, tag):

    registers = []
    cycle = []
    cycle_dict = {}

    if len(data) != 0:

        date = set_date(data[0]['Timestamp'])
        update_data_log(date, tag)

        lot = get_lot()

        if len(lot) > 1:
            lot_1 = lot[-2]
            lot_2 = lot[-1]
        else:
            lot_1 = lot[0]

        cycle_re = get_cycle()

        prev = ''

        for c in cycle_re:
            if c['f_value'] == prev:
                cycle_dict[c['f_value']]['datetime_read'] = c['datetime_read']
            else:
                cycle_dict[c['f_value']] = c
                prev = c['f_value']
                
        for c in cycle_dict:
            cycle.append(cycle_dict[c])

        if len(lot) > 1:
            lot_date_1 = datetime.strptime(lot_1['datetime_read'].split('-03:')[0], '%Y-%m-%dT%H:%M:%S')

            for c in cycle:
                cycle_date = datetime.strptime(c['datetime_read'].split('-03:')[0], '%Y-%m-%dT%H:%M:%S')

                if cycle_date <= lot_date_1:
                    cycle_1 = c
                else:
                    cycle_2 = c
        else:
            lot_date_1 = datetime.strptime(lot_1['datetime_read'].split('-03:')[0], '%Y-%m-%dT%H:%M:%S')
            for c in cycle:
                cycle_date = datetime.strptime(c['datetime_read'].split('-03:')[0], '%Y-%m-%dT%H:%M:%S')
                cycle_1 = c

        for d in data:

            datetime_read = set_date(d['Timestamp'])
            filter_date = datetime.strptime(datetime_read.split('-03:')[0], '%Y-%m-%dT%H:%M:%S')

            if len(lot) > 1:

                if filter_date <= lot_date_1:

                    registers.append({
                            'capture_id':lot_1['capture_id'],
                            'datetime_read':datetime_read,
                            'f_value':str(lot_1['f_value'])
                        })

                    registers.append({
                            'capture_id':cycle_1['capture_id'],
                            'datetime_read':datetime_read,
                            'f_value':str(cycle_1['f_value'])
                        })               

                    registers.append({
                            'capture_id':tag,
                            'datetime_read':datetime_read,
                            'f_value':str(d['Value'])
                        })
                else:
                    registers.append({
                            'capture_id':lot_2['capture_id'],
                            'datetime_read':datetime_read,
                            'f_value':str(lot_2['f_value'])
                        })

                    registers.append({
                            'capture_id':cycle_2['capture_id'],
                            'datetime_read':datetime_read,
                            'f_value':str(cycle_2['f_value'])
                        })   

                    registers.append({
                            'capture_id':tag,
                            'datetime_read':datetime_read,
                            'f_value':str(d['Value'])
                        })
            else:
                registers.append({
                        'capture_id':lot_1['capture_id'],
                        'datetime_read':datetime_read,
                        'f_value':str(lot_1['f_value'])
                    })

                registers.append({
                        'capture_id':cycle_1['capure_id'],
                        'datetime_read':datetime_read,
                        'f_value':str(cycle_1['f_value'])
                    })               

                registers.append({
                        'capture_id':tag,
                        'datetime_read':datetime_read,
                        'f_value':str(d['Value'])
                    })


            if len(registers) > 199 and len(registers) < 205:
                send_registers(registers)
                registers.clear()

        send_registers(registers)

    else:
        date = get_last_filters(tag)
        insert_data_log(date, tag)



def set_measurement(data, tag):

    registers = []
    power = []
    tmp = []
    prev_date = ''
    ctrl = 0

    if len(data) > 0:
        date = set_date(data[0]['Timestamp'])
        update_data_log(date, tag)

        for d in data:
            date = d['Timestamp'].split('.')[0]
            minute = int(date.split(':')[-2])

            if minute == 16 and ctrl != 16:
                ctrl = 16
                tmp.append({
                    'datetime_read':prev_date,
                    'value_active':max(power)
                })
                power.clear()
            elif minute == 31 and ctrl != 31:
                ctrl = 31
                tmp.append({
                    'datetime_read':prev_date,
                    'value_active':max(power)
                })
                power.clear()
            elif minute == 46 and ctrl != 46:
                ctrl = 46
                tmp.append({
                    'datetime_read':prev_date,
                    'value_active':max(power)
                })
                power.clear()
            elif minute == 1 and ctrl != 1:
                ctrl = 1
                tmp.append({
                    'datetime_read':prev_date,
                    'value_active':max(power)
                })
                power.clear()

            prev_date = d['Timestamp']

            power.append(d['Value'])

        tmp = tmp[1:-1]

        for t in tmp:

            datetime_read = set_date(t['datetime_read'])        

            registers.append({
                    'capture_id':tag,
                    'datetime_read': datetime_read,
                    'value_active': t['value_active'],
                    'value_reactive': 0,
                    'consumption': 0,
                    'period': 0,
                    'is_point': True,
                    'tension_phase_neutral_a': 0,
                    'tension_phase_neutral_b': 0,
                    'tension_phase_neutral_c': 0,
                    'current_a': 0,
                    'current_b': 0,
                    'current_c': 0,
                    'thd_tension_a': 0,
                    'thd_tension_b': 0,
                    'thd_tension_c': 0,
                    'thd_current_a': 0,
                    'thd_current_b': 0,
                    'thd_current_c': 0,
                    'power_active': 0,
                    'power_reactive': 0,
                    'consolidation_count': 0
                })

            if len(registers) == 200:
                send_registers(registers)
                registers.clear()

        send_registers(registers)


    else:
        date = get_last_measurement(tag)
        insert_data_log(date, tag)


def get_lot():

    registers = []
    tag = "AR.LGC.Numero_Lote_Processo"
    url = "https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZw3H8BAATUVTLVBJLVBST0RcQVIuTEdDLk5VTUVST19MT1RFX1BST0NFU1NP/recorded?filterexpression=BadVal('.')=0&startTime=-1h"
    res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))

    try:
        body = json.loads(res.text)
        if 'Items' in body.keys():
            data = body['Items']
    except Exception as error:
        print(error)

    if len(data) != 0:

        date = set_date(data[0]['Timestamp'])
        update_data_log(date, tag)
    
        for d in data:
            register = set_data(d, tag)
            if len(register) > 0:
                registers.append(register)

        return registers

    else:
        date = get_last_filters(tag)
        insert_data_log(date, tag)
        return []



def get_cycle():

    registers = []
    tag = "AR.LGC.Ciclo_Lote_Processo"
    url = "https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwIIoBAATUVTLVBJLVBST0RcQVIuTEdDLkNJQ0xPX0xPVEVfUFJPQ0VTU08/recorded?filterexpression=BadVal('.')=0&startTime=-1h"
    res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))

    try:
        body = json.loads(res.text)
        if 'Items' in body.keys():
            data = body['Items']
    except Exception as error:
        print(error) 
    
    if len(data) != 0:

        date = set_date(data[0]['Timestamp'])
        update_data_log(date, tag)
    
        for d in data:
            register = set_data(d, tag)
            if len(register) > 0:
                registers.append(register)

        return registers

    else:
        date = get_last_filters(tag)
        insert_data_log(date, tag)



def set_data(data, tag):

    register = {}

    if len(data['Value']) < 11:
        datetime_read = set_date(data['Timestamp'])
        register['capture_id'] = tag
        register['datetime_read'] = datetime_read
        register['f_value'] = data['Value']

    return register


def send_registers(data):

    headers = {
        'Content-type': 'application/json',
        'Accept': 'text/plain', 
        'x-api-key': '0VmpVLnf7e6E6wZMNS235aPI2N3TOeko24ozYM0h'
    }

    API_ENDPOINT = 'https://cr4ggvm03k.execute-api.us-east-2.amazonaws.com/producao/csn'

    registers = json.dumps(data, indent=2)

    response = requests.post(API_ENDPOINT, data=registers, headers=headers)


def insert_data_log(date, tag):

    data = (date, tag)

    conn = None
    sql = """
            INSERT INTO online_data_log (datetime_without, id_capture)
                VALUES (%s, %s)
            ON CONFLICT ON CONSTRAINT date_capture_uniq
            DO NOTHING;
    """

    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql, data)
        cur.close()
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def update_data_log(date, tag):

    data = (date, tag)
    row = 0

    conn = None
    sql = """
        UPDATE online_data_log
            SET datetime_with=%s
            WHERE id_capture=%s and datetime_with IS NULL;
    """

    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql, data)
        row = cur.rowcount
        cur.close()
        conn.commit()    
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return row



def get_last_processes(tag):

    conn = None
    data = None
    arg = {'id_capture':tag}
    sql = f"""
        SELECT 
        p.datetime_read
        FROM processes p
        LEFT JOIN plant_equipment e ON e.plant_equipment_id = p.plant_equipment_id
        WHERE e.id_capture = %(id_capture)s
        ORDER BY 1 DESC
        LIMIT 1  
    """

    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql, arg)
        data = cur.fetchone()
        cur.close()
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return data



def get_last_measurement(tag):

    conn = None
    data = None
    arg = {'id_capture':tag}
    sql = f"""
        SELECT 
        m.datetime_read
        FROM measurement m
        LEFT JOIN plant_equipment e ON e.plant_equipment_id = m.plant_equipment_id
        WHERE e.id_capture = %(id_capture)s
        ORDER BY 1 DESC
        LIMIT 1  
    """

    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql, arg)
        data = cur.fetchone()
        cur.close()
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return data


def get_last_filters(tag):

    map_tag = {
        'AR.LGC.Largura_Lote_Processo':'width',
        'AR.LGC.REVESTIMENTO_INFERIOR':'inf_coating',
        'AR.LGC.REVESTIMENTO_SUPERIOR':'sup_coating',
        'AR.LGC.Numero_Lote_Processo':'lot',
        'AR.LGC.Ciclo_Lote_Processo':'cycle_re',
        'AR.LGC.ESPESSURA':'thickness'
    }

    column = map_tag[tag]

    conn = None
    data = None
    sql = f"""
        SELECT 
        datetime_read
        FROM filter_processes 
        WHERE {column} IS NOT NULL
        ORDER BY 1 DESC
        LIMIT 1 
    """

    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        data = cur.fetchone()
        cur.close()
    except Exception as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            return data

def change_date_format(date):

    if 'T' in date:
        tmp = date.split('T')[0]
        tmp = tmp.split('-')
        time = date.split('T')[1]

        year = tmp[0]
        month = tmp[1]
        day = tmp[2]
        pi_format = month+'/'+day+'/'+year+' '+time

        return pi_format

    else:
        tmp = date.split('-')
        year = tmp[0]
        month = tmp[1]
        day = tmp[2]
        pi_format = month+'/'+day+'/'+year

        return pi_format


def cal_interval(date_1, date_2):

    if 'T' in date_1 and 'T' in date_2:
        date_from = datetime.strptime(date_1, '%Y-%m-%dT%H:%M')
        date_to = datetime.strptime(date_2, '%Y-%m-%dT%H:%M')
        interval = date_to - date_from

        return interval

    elif 'T' in date_1 and 'T' not in date_2:
        date_from = datetime.strptime(date_1, '%Y-%m-%dT%H:%M')
        date_to = datetime.strptime(date_2, '%Y-%m-%d')
        interval = date_to - date_from

        return interval

    elif 'T' not in date_1 and 'T' in date_2:
        date_from = datetime.strptime(date_1, '%Y-%m-%d')
        date_to = datetime.strptime(date_2, '%Y-%m-%dT%H:%M')
        interval = date_to - date_from

        return interval

    else:
        date_from = datetime.strptime(date_1, '%Y-%m-%d')
        date_to = datetime.strptime(date_2, '%Y-%m-%d')
        interval = date_to - date_from

        return interval


def set_interval(interval):

    days = interval.days
    seconds = interval.seconds
    hours = seconds // 3600
    tmp = seconds % 3600
    minutes = tmp // 60

    day = str(days)
    hour = str(hours)
    minute = str(minutes)

    if days != 0 and hours != 0 and minutes != 0:
        interval_format = '-'+day+'d+'+hour+'h+'+minute+'m'
        return interval_format
    elif days != 0 and hours != 0:
        interval_format = '-'+day+'d+'+hour+'h'
        return interval_format
    elif days != 0 and minutes != 0:
        interval_format = '-'+day+'d+'+minute+'m'
        return interval_format
    elif hours != 0 and minutes != 0:
        interval_format = '-'+hour+'h+'+minute+'m'
        return interval_format
    elif days != 0:
        interval_format = '-'+day+'d'
        return interval_format
    elif hours != 0:
        interval_format = '-'+hour+'h'
        return interval_format
    else:
        interval_format = '-'+minute+'m'
        return interval_format   



if __name__ == '__main__':

    args = sys.argv

    if len(args) == 1:
        date_param = '-5m'

    elif len(args) == 2:
        date = args[1]
        date_param = change_date_format(date)

    else:
        date_1 = args[1]
        date_2 = args[2]
        interval = cal_interval(date_1, date_2)
        pi_interval = set_interval(interval)
        pi_date = change_date_format(date_2)
        date_param = pi_date+' '+pi_interval

    tags = {
        "AR.LGC.Temperatura_Tira_RTS":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqX8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfUlRT/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Velocidade_Processo":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwmn8BAATUVTLVBJLVBST0RcQVIuTEdDLlZFTE9DSURBREVfUFJPQ0VTU08/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Temperatura_Tira_RTH":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqH8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfUlRI/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.REVESTIMENTO_INFERIOR":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwg_EBAATUVTLVBJLVBST0RcQVIuTEdDLlJFVkVTVElNRU5UT19JTkZFUklPUg/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Concentracao_H2":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwsX8BAATUVTLVBJLVBST0RcQVIuTEdDLkNPTkNFTlRSQUNBT19IMg/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Temperatura_Tira_JCS":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwq38BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfSkNT/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.VAZAO_N2_FORNO":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwi_EBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX04yX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.VAZAO_GN_FORNO":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjPEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0dOX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Temperatura_Tira_DFF":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwp38BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfREZG/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Temperatura_Tira_SCS":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqn8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfU0NT/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.ESPESSURA":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwgfEBAATUVTLVBJLVBST0RcQVIuTEdDLkVTUEVTU1VSQQ/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.VAZAO_H2_FORNO":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwivEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0gyX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.PRESSAO_RADIANTE":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhfEBAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fUkFESUFOVEU/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Producao_Atual":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwnH8BAATUVTLVBJLVBST0RcQVIuTEdDLlBST0RVQ0FPX0FUVUFM/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.HNX_SETPOINT_SP":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkvEBAATUVTLVBJLVBST0RcQVIuTEdDLkhOWF9TRVRQT0lOVF9TUA/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.DAMPER_INFERIOR_PV":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwifEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9JTkZFUklPUl9QVg/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Pressao_Forno_DFF":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwzn8BAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fRk9STk9fREZG/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Pressao_Snout":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwz38BAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fU05PVVQ/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.ZONA2_DFF_ON":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkPEBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkEyX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.ZONA1_DFF_ON":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkfEBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkExX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Largura_Lote_Processo":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZw3X8BAATUVTLVBJLVBST0RcQVIuTEdDLkxBUkdVUkFfTE9URV9QUk9DRVNTTw/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.Vazao_HNX":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwsH8BAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0hOWA/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.DAMPER_PRINCIPAL_PV":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhvEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9QUklOQ0lQQUxfUFY/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.ELETRICIDADE_LGC":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwk_EBAATUVTLVBJLVBST0RcQVIuTEdDLkVMRVRSSUNJREFERV9MR0M/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.DAMPER_PRINCIPAL_SP":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwh_EBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9QUklOQ0lQQUxfU1A/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.REVESTIMENTO_SUPERIOR":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwgvEBAATUVTLVBJLVBST0RcQVIuTEdDLlJFVkVTVElNRU5UT19TVVBFUklPUg/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.VAZAO_GN_LGC":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjfEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0dOX0xHQw/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.PRESSAO_POST_COMBUSTION":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhPEBAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fUE9TVF9DT01CVVNUSU9O/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.DAMPER_SUPERIOR_PV":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwiPEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9TVVBFUklPUl9QVg/recorded?filterexpression=BadVal('.')=0&startTime={date_param}",
        "AR.LGC.ZONA3_DFF_ON":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwj_EBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkEzX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime={date_param}"
    }

    user = os.environ['USER']
    password = os.environ['PASSWORD']

    registers = []

    filters = ['AR.LGC.ESPESSURA', 'AR.LGC.REVESTIMENTO_INFERIOR', 'AR.LGC.REVESTIMENTO_SUPERIOR']

    for t in tags:
        url = tags[t]
        if t == 'AR.LGC.ELETRICIDADE_LGC':
            if len(args) == 1:
                now = datetime.now()
                if now.minute in [16, 31, 46, 1]:
                    url = "https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwk_EBAATUVTLVBJLVBST0RcQVIuTEdDLkVMRVRSSUNJREFERV9MR0M/recorded?filterexpression=BadVal('.')=0&startTime=-17m"
                    res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))
            else:
                res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))

        else:
            res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))

        try:
            body = json.loads(res.text)
            if 'Items' in body.keys():
                data = body['Items']

                if t == 'AR.LGC.ELETRICIDADE_LGC':
                    set_measurement(data, t)
                elif t in filters:
                    set_processes_filters(data, t)
                else:
                    set_processes(data, t)
        except Exception as error:
            print(error) 
             
        
        

