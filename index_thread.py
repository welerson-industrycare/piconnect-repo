import os
import sys
from time import strftime
import psycopg2
import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import date, datetime, timedelta
from dotenv import load_dotenv
import urllib3 
import traceback
import threading
import math
import arrow
import pytz



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
        print(traceback.format_exc())

    finally:
        if conn is not None:
            return conn

def round_date(date):

    try:
        second = date.second
        if second < 5:
            step = 5 - second
        elif second > 5 and second <= 10:
            step = 10 - second
        elif second > 10 and second <= 15:
            step = 15 - second
        elif second > 15 and second <= 20:
            step = 20 - second
        elif second > 20 and second <= 25:
            step = 25 - second            
        elif second > 25 and second <= 30:
            step = 30 - second
        elif second > 30 and second <= 35:
            step = 35 - second
        elif second > 35 and second <= 40:
            step = 40 - second
        elif second > 40 and second <= 45:
            step = 45 - second
        elif second > 45 and second <= 50:
            step = 50 - second
        elif second > 50 and second <= 55:
            step = 55 - second   
        else:
            step = 60 - second

        return date+timedelta(seconds=step)

    except Exception as error:
        print(error)
        print(traceback.format_exc())



def set_date(date):

    try:

        date_str = date.split('.')[0]
        date_str = date_str.replace('Z', '')
        date_time_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
        date_time_obj = round_date(date_time_obj)    
        hours = timedelta(hours=3)
        date_time_obj = date_time_obj - hours
        date_time = date_time_obj.strftime('%Y-%m-%dT%H:%M:%S-03:00')

        return date_time
    
    except Exception as error:
        print(error)
        print(traceback.format_exc())



def set_processes(data, tag):

    try:

        if len(data) != 0:
            registers = []
            i = 1
            total = len(data)
            array_threads = []
            for d in data:
                print(f'Processando {i} de {total}', end='\r')
                i+= 1

                datetime_read = set_date(d['Timestamp'])

                registers.append({
                    'capture_id':tag,
                    'datetime_read':datetime_read,
                    'p_value':d['Value']
                })

                if len(registers) == 50:
                    current = threading.Thread(target=send_registers, args=(registers.copy(),))
                    array_threads.append(current)
                    current.start()
                    registers.clear()

            if len(registers) > 0:
                current = threading.Thread(target=send_registers, args=(registers.copy(),))
                array_threads.append(current)
                current.start()

            for a in array_threads:
                a.join()


    except Exception as error:
        print(error)
        print(traceback.format_exc())



def set_processes_filters(data, tag):

    registers = []
    array_threads = []

    try:
        if data: 
            date_from = datetime.strptime(set_date(data[0]['Timestamp'])[0:19], '%Y-%m-%dT%H:%M:%S')
            date_to = datetime.strptime(set_date(data[-1]['Timestamp'])[0:19], '%Y-%m-%dT%H:%M:%S')
            current_date = date_from
            current_value = data[0]['Value']
            index = 1
            while(current_date < date_to):
                if datetime.strptime(set_date(data[index]['Timestamp'])[0:19], '%Y-%m-%dT%H:%M:%S') <= current_date:
                    current_value = data[index]['Value']
                    index += 1
                    
                print(f'Enviando dados: {current_date}', end='\r')  
                registers.append({
                    'capture_id':tag,
                    'datetime_read':current_date.strftime('%Y-%m-%dT%H:%M:%S-03:00'),
                    'f_value':str(current_value)
                })
                
                current_date = current_date + timedelta(seconds=5)



                if len(registers) == 50:
                    current = threading.Thread(target=send_registers, args=(registers.copy(),))
                    array_threads.append(current)
                    current.start()
                    registers.clear()

            if len(registers) > 0:
                current = threading.Thread(target=send_registers, args=(registers.copy(),))   
                array_threads.append(current)
                current.start()

            for a in array_threads:
                a.join()

    except Exception as error:
        print(error)
        print(traceback.format_exc())


def set_measurement(data, tag):

    registers = []

    min_date = arrow.get(data[0]['Timestamp']).datetime
    max_date = arrow.get(data[-1]['Timestamp']).datetime

    min_date, max_date = measurement_interval(min_date, max_date)
    array_thread = []

    try:

        if len(data) > 0:

            current_date = min_date
            index_from = 0
            while(current_date < max_date):
                next_interval = current_date + timedelta(minutes=15)
                if next_interval >= max_date:
                    index_to = len(data) - 1
                else:
                    index_to = next(i for i , x in enumerate(data) if i > index_from and arrow.get(data[i]['Timestamp']).datetime > next_interval) 
                # sample = list(filter(lambda x: arrow.get(x['Timestamp']).datetime > current_date and arrow.get(x['Timestamp']).datetime <= next_interval, data))
                sample = data[index_from:index_to]
                value_active = max([ s['Value'] for s in sample ])

                index_from = index_to

                registers.append({
                    'capture_id':tag,
                    'datetime_read': next_interval.strftime('%Y-%m-%dT%H:%M:%S-03:00'),
                    'value_active': value_active,
                    'value_reactive': float(0.0000001),
                    # 'consumption': float(0.0),
                    # 'period': float(0.0),
                    # 'is_point': True,
                    'tension_phase_neutral_a': float(0.0000001),
                    'tension_phase_neutral_b': float(0.0000001),
                    'tension_phase_neutral_c': float(0.0000001),
                    'current_a': float(0.0000001),
                    'current_b': float(0.0000001),
                    'current_c': float(0.0000001),
                    'thd_tension_a': float(0.0000001),
                    'thd_tension_b': float(0.0000001),
                    'thd_tension_c': float(0.0000001),
                    'thd_current_a': float(0.0000001),
                    'thd_current_b': float(0.0000001),
                    'thd_current_c': float(0.0000001),
                    # 'power_active': float(0.0000001),
                    # 'power_reactive': float(0.0000001),
                    # 'consolidation_count': float(0.0000001)
                    })

                current_date = next_interval

                if len(registers) == 50:
                    current = threading.Thread(target=send_registers, args=(registers.copy(),))
                    array_thread.append(current)
                    current.start()
                    registers.clear()

            if len(registers) > 0:
                current = threading.Thread(target=send_registers, args=(registers.copy(),))
                array_thread.append(current)
                current.start()

            for a in array_thread:
                a.join()

    except Exception as error:
        print(error)
        print(traceback.format_exc())


def get_lot(date_from, date_to):

    date_1_obj = arrow.get(date_from).datetime
    date_2_obj = arrow.get(date_to).datetime

    date_1 = (date_1_obj - timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M')
    date_2 = (date_2_obj - timedelta(hours=3)).strftime('%Y-%m-%dT%H:%M')
 
    try:
        tag = 'AR.LGC.Numero_Lote_Processo'
        url = f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZw3H8BAATUVTLVBJLVBST0RcQVIuTEdDLk5VTUVST19MT1RFX1BST0NFU1NP/recorded?filterexpression=BadVal('.')=0&startTime="
        data = paginate_pi_call(date_1, date_2, url, second_size=600)    
       
        if len(data) != 0:
            return data

        else:
            return []

    except Exception as error:
        print(error)
        print(traceback.format_exc())


def get_cycle(date_from, date_to):

    date_1_obj = arrow.get(date_from).datetime
    date_2_obj = arrow.get(date_to).datetime

    date_1 = (date_1_obj - timedelta(hours=7)).strftime('%Y-%m-%dT%H:%M')
    date_2 = (date_2_obj - timedelta(hours=3)).strftime('%Y-%m-%dT%H:%M')


    try:
        tag = "AR.LGC.Ciclo_Lote_Processo"
        url = f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwIIoBAATUVTLVBJLVBST0RcQVIuTEdDLkNJQ0xPX0xPVEVfUFJPQ0VTU08/recorded?filterexpression=BadVal('.')=0&startTime="
        data = paginate_pi_call(date_1, date_2, url, second_size=60)    
       
        if len(data) != 0:
            return data

        else:
            return []

    except Exception as error:
        print(error)
        print(traceback.format_exc())



def set_data(data, tag):

    register = {}

    try:

        if len(data['Value']) < 11:
            datetime_read = set_date(data['Timestamp'])
            register['capture_id'] = tag
            register['datetime_read'] = datetime_read
            register['f_value'] = data['Value']

        return register

    except Exception as error:
        print(error)
        print(traceback.format_exc())


def send_registers(data):

    message_error = ''

    try:

        headers = {
            'Content-type': 'application/json',
            'Accept': 'text/plain', 
            'x-api-key': '0VmpVLnf7e6E6wZMNS235aPI2N3TOeko24ozYM0h'
        }

        API_ENDPOINT = 'https://cr4ggvm03k.execute-api.us-east-2.amazonaws.com/producao/csn'

        registers = json.dumps(data)

        response = requests.post(API_ENDPOINT, data=registers, headers=headers)

        res = json.loads(response.content)

        if res is not None and 'statusCode' in res and res['statusCode'] == 400:
            message_error = res['body']
            raise Exception('corpo da mensagem') 

    except Exception as error:
        print(message_error)
        print(traceback.format_exc())


def insert_data_log(date, tag):

    try:

        data = (date, tag)

        conn = None
        sql = """
                INSERT INTO online_data_log (datetime_last, id_capture)
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

    except Exception as error:
        print(error)
        print(traceback.format_exc())


def update_data_log(date, tag):

    data = (date, tag)
    row = 0

    conn = None
    sql = """
        UPDATE online_data_log
            SET datetime_return=%s
            WHERE id_capture=%s and datetime_return IS NULL;
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
        print(traceback.format_exc())
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
        print(traceback.format_exc())
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
        print(traceback.format_exc())
    finally:
        if conn is not None:
            conn.close()
            return data


def get_last_filters(tag):

    try:

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

        
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        data = cur.fetchone()
        cur.close()
    except Exception as error:
        print(error)
        print(traceback.format_exc())
    finally:
        if conn is not None:
            conn.close()
            return data

def change_date_format(date):

    try:

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

    except Exception as error:
        print(error)
        print(traceback.format_exc())


def cal_interval(date_1, date_2):

    try:

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

    except Exception as error:
        print(error)
        print(traceback.format_exc())


def set_interval(interval):

    try:

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

    except Exception as error:
        print(error)
        print(traceback.format_exc())


def paginate_pi_call(start, end_date, url, second_size=5):

    # user = os.environ['USER']
    # password = os.environ['PASSWORD']

    user = 'CSNCARE_PI'
    password = 'Nsd#sf23V4*kF'

    try:

        data = []

        if 'T' in start and 'T' in end_date:
            date_from = datetime.strptime(start, '%Y-%m-%dT%H:%M')
            date_to = datetime.strptime(end_date, '%Y-%m-%dT%H:%M')

        elif 'T' in start:
            date_from = datetime.strptime(start, '%Y-%m-%dT%H:%M')
            date_to = datetime.strptime(end_date, '%Y-%m-%d')  

        elif 'T' in end_date:
            date_from = datetime.strptime(start, '%Y-%m-%d')
            date_to = datetime.strptime(end_date, '%Y-%m-%dT%H:%M')

        else:
            date_from = datetime.strptime(start, '%Y-%m-%d')
            date_to = datetime.strptime(end_date, '%Y-%m-%d')   

        seconds = second_size
        window_size = 1000
        date_seconds = (date_to - date_from).total_seconds() 

        data_length = date_seconds / seconds
        fit_second_size = False
        if data_length > window_size:
            current_date = date_from

            step = math.ceil(0.7*window_size*5) 

            while(current_date < date_to):
                step = step if current_date + timedelta(seconds=step) < date_to else (date_to - current_date).total_seconds()
                new_date = current_date + timedelta(seconds=step) 
                date_param =  'startTime='+current_date.strftime('%m/%d/%Y %H:%M:%S')+'&endTime='+new_date.strftime('%m/%d/%Y %H:%M:%S')
                new_url = url.split('startTime=')[0]+date_param
                res = requests.get(new_url, verify=False, auth=HTTPBasicAuth(user, password))
                body = json.loads(res.text)
                if 'Items' in body:
                    data.extend(body['Items'])
                    current_date = new_date
                if not fit_second_size and len(data) > 0:
                    if len(data) > 1:
                        interval = math.floor(sum([(arrow.get(data[i]['Timestamp']).datetime - arrow.get(data[i-1]['Timestamp']).datetime).total_seconds()  for i in range(1, len(data))])/len(data))
                    else:
                        interval = second_size    
                    if interval < second_size:
                        data = paginate_pi_call(start, end_date, url, second_size=interval)
                        return data
                    fit_second_size = True

            return data
        else:

            date_1 = change_date_format(start)
            date_2 = change_date_format(end_date)
            date_param =  'startTime='+date_1+'&endTime='+date_2
            new_url = url.split('startTime=')[0]+date_param


            res = requests.get(new_url, verify=False, auth=HTTPBasicAuth(user, password))
            body = json.loads(res.text)
            return body['Items'] if 'Items' in body else []

    except Exception as error:
        print(error)
        print(traceback.format_exc())

def measurement_interval(date_from, date_to):

    d_from = datetime(date_from.year, date_from.month, date_from.day, date_from.hour, date_from.minute, 0, 0 ,pytz.UTC)
    d_to = datetime(date_to.year, date_to.month, date_to.day, date_to.hour, date_to.minute, 0, 0, pytz.UTC)

    if d_from.minute < 15:
        d_from = d_from - timedelta(minutes=d_from.minute)
    elif d_from.minute > 15 and d_from.minute <= 30:
        d_from = d_from - timedelta(minutes=d_from.minute-15)                  
    elif d_from.minute > 30 and d_from.minute <= 45:
        d_from = d_from - timedelta(minutes=d_from.minute-30)
    else:
        d_from = d_from - timedelta(minutes=d_from.minute-45)  

    if d_to.minute < 15:
        d_to = d_to + timedelta(minutes=15-d_to.minute)
    elif d_to.minute > 15 and d_to.minute <= 30:
        d_to = d_to + timedelta(minutes=30-d_to.minute)                   
    elif d_to.minute > 30 and d_to.minute <= 45:
        d_to = d_to + timedelta(minutes=45-d_to.minute)
    else:
        d_to = d_to + timedelta(minutes=60-d_to.minute) 

    return d_from, d_to


if __name__ == '__main__':

    args = sys.argv

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    if len(args) == 1:
        date_to = datetime.now()
        date_from = date_to - timedelta(minutes=5)

        date_1 = date_from.strftime('%Y-%m-%dT%H:%M')
        date_2 = date_to.strftime('%Y-%m-%dT%H:%M')

    elif len(args) == 2:
        
        date_str = args[1]

        if 'T' in date_str:
            date_from = datetime.strptime(date_str, '%Y-%m-%dT%H:%M')
            date_to = date_from + timedelta(days=1)

        else:
            date_from = datetime.strptime(date_str, '%Y-%m-%d')
            date_to = date_from + timedelta(days=1)

        date_1 =  date_from.strftime('%Y-%m-%dT%H:%M')
        date_2 =  date_to.strftime('%Y-%m-%dT%H:%M')       


    else:
        
        date_map = {
            '10':'T00:00',
            '13':':00',
            '16':''
        }
        date_1 = args[1]+date_map[str(len(args[1]))]
        date_2 = args[2]+date_map[str(len(args[2]))]


        


    tags = {
        "AR.LGC.Temperatura_Tira_RTS":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqX8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfUlRT/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Velocidade_Processo":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwmn8BAATUVTLVBJLVBST0RcQVIuTEdDLlZFTE9DSURBREVfUFJPQ0VTU08/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Temperatura_Tira_RTH":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqH8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfUlRI/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.REVESTIMENTO_INFERIOR":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwg_EBAATUVTLVBJLVBST0RcQVIuTEdDLlJFVkVTVElNRU5UT19JTkZFUklPUg/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Concentracao_H2":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwsX8BAATUVTLVBJLVBST0RcQVIuTEdDLkNPTkNFTlRSQUNBT19IMg/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Temperatura_Tira_JCS":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwq38BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfSkNT/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.VAZAO_N2_FORNO":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwi_EBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX04yX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.VAZAO_GN_FORNO":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjPEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0dOX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Temperatura_Tira_DFF":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwp38BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfREZG/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Temperatura_Tira_SCS":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqn8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfU0NT/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.ESPESSURA":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwgfEBAATUVTLVBJLVBST0RcQVIuTEdDLkVTUEVTU1VSQQ/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.VAZAO_H2_FORNO":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwivEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0gyX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.PRESSAO_RADIANTE":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhfEBAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fUkFESUFOVEU/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Producao_Atual":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwnH8BAATUVTLVBJLVBST0RcQVIuTEdDLlBST0RVQ0FPX0FUVUFM/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.HNX_SETPOINT_SP":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkvEBAATUVTLVBJLVBST0RcQVIuTEdDLkhOWF9TRVRQT0lOVF9TUA/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.DAMPER_INFERIOR_PV":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwifEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9JTkZFUklPUl9QVg/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Pressao_Forno_DFF":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwzn8BAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fRk9STk9fREZG/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Pressao_Snout":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwz38BAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fU05PVVQ/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.ZONA2_DFF_ON":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkPEBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkEyX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.ZONA1_DFF_ON":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkfEBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkExX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Largura_Lote_Processo":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZw3X8BAATUVTLVBJLVBST0RcQVIuTEdDLkxBUkdVUkFfTE9URV9QUk9DRVNTTw/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Vazao_HNX":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwsH8BAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0hOWA/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.DAMPER_PRINCIPAL_PV":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhvEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9QUklOQ0lQQUxfUFY/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.ELETRICIDADE_LGC":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwk_EBAATUVTLVBJLVBST0RcQVIuTEdDLkVMRVRSSUNJREFERV9MR0M/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.DAMPER_PRINCIPAL_SP":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwh_EBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9QUklOQ0lQQUxfU1A/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.REVESTIMENTO_SUPERIOR":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwgvEBAATUVTLVBJLVBST0RcQVIuTEdDLlJFVkVTVElNRU5UT19TVVBFUklPUg/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.VAZAO_GN_LGC":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjfEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0dOX0xHQw/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.PRESSAO_POST_COMBUSTION":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhPEBAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fUE9TVF9DT01CVVNUSU9O/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.DAMPER_SUPERIOR_PV":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwiPEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9TVVBFUklPUl9QVg/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.ZONA3_DFF_ON":f"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwj_EBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkEzX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Ciclo_Lote_Processo":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwIIoBAATUVTLVBJLVBST0RcQVIuTEdDLkNJQ0xPX0xPVEVfUFJPQ0VTU08/recorded?filterexpression=BadVal('.')=0&startTime=",
        "AR.LGC.Numero_Lote_Processo":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZw3H8BAATUVTLVBJLVBST0RcQVIuTEdDLk5VTUVST19MT1RFX1BST0NFU1NP/recorded?filterexpression=BadVal('.')=0&startTime="
    }

    registers = []

    filters = ['AR.LGC.ESPESSURA', 'AR.LGC.REVESTIMENTO_INFERIOR', 'AR.LGC.REVESTIMENTO_SUPERIOR', 'AR.LGC.Largura_Lote_Processo', 'AR.LGC.Numero_Lote_Processo', 'AR.LGC.Ciclo_Lote_Processo']

    
    current_date = datetime.strptime(date_1, '%Y-%m-%dT%H:%M')
    date_to = datetime.strptime(date_2, '%Y-%m-%dT%H:%M')
    step = 24*60*60
    while(current_date < date_to):
        step = step if current_date + timedelta(seconds=step) < date_to else (date_to - current_date).total_seconds()
        date_from = current_date
        new_date = current_date + timedelta(seconds=step)

        date_1 = date_from.strftime('%Y-%m-%dT%H:%M')
        date_2 = new_date.strftime('%Y-%m-%dT%H:%M')

        print(f'Extraindo dados do intervalo:{date_1} -- {date_2}')

        for t in tags:
            print(f'\nProcessando tag: {t}\t{datetime.now().isoformat()}\n')
            url = tags[t]
            if t == 'AR.LGC.ELETRICIDADE_LGC':
                                  
                d_from, d_to = measurement_interval(date_from, new_date)
                
                if len(args) == 1:
                    d_from = d_from - timedelta(minutes=15)
                    d_to = d_to - timedelta(minutes=15)  
                
                    date_1 = d_from.strftime('%Y-%m-%dT%H:%M')
                    date_2 = d_to.strftime('%Y-%m-%dT%H:%M')
                    url = "https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwk_EBAATUVTLVBJLVBST0RcQVIuTEdDLkVMRVRSSUNJREFERV9MR0M/recorded?filterexpression=BadVal('.')=0&startTime=-17m"
                    res = paginate_pi_call(date_1, date_2, url)
                else:
                    date_1 = d_from.strftime('%Y-%m-%dT%H:%M')
                    date_2 = d_to.strftime('%Y-%m-%dT%H:%M')
                    res = paginate_pi_call(date_1, date_2, url) 

            elif t in filters:
                if (new_date - current_date).total_seconds() < 2400:
                    date_3 = (new_date - timedelta(minutes=40)).strftime('%Y-%m-%dT%H:%M')
                
                else:
                    date_3 = date_1


                if t == 'AR.LGC.Ciclo_Lote_Processo':
                    res = paginate_pi_call(date_3, date_2, url,60)
                else:    
                    res = paginate_pi_call(date_3, date_2, url,1800)

            else:
                res = paginate_pi_call(date_1, date_2, url)

            try:
                if len(res) > 0:

                    print(f'Registros recuperados: {len(res)}')

                    if t == 'AR.LGC.ELETRICIDADE_LGC':
                        set_measurement(res, t)
                    elif t in filters:
                        set_processes_filters(res, t)
                    else:
                        set_processes(res, t)
            except Exception as error:
                print(error)
                print(traceback.format_exc())
             
        
        current_date = new_date

