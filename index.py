import os
import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import date, datetime, timedelta
from dotenv import load_dotenv


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

    if len(data) > 0:

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


def set_processes_filters(data, tag):

    registers = []
    cycle = []
    cycle_dict = {}

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


    if len(data) > 0: 

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



def set_measurement(data, tag, now):

    registers = []
    power = []

    if len(data) > 0:

        date = now - timedelta(minutes=1)
        datetime_read = date.strftime('%Y-%m-%dT%H:%M:%S')

        for d in data:
            date_str = d['Timestamp'].replace('Z', '')
            date_str = date_str.split('.')[0]
            date_object = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')
            if now.minute == 16 and (date_object.minute > 0 and date_object.minute < 16):
                power.append(d['Value'])
            elif now.minute == 31 and (date_object.minute > 16 and date_object.minute < 31):
                power.append(d['Value'])        
            elif now.minute == 46 and  (date_object.minute > 30 and date_object.minute < 46):
                power.append(d['Value'])
            elif now.minute == 1 and  (date_object.minute > 45 or date_object.minute == 0):
                power.append(d['Value'])  


        max_power = max(power)    

        datetime_read = set_date(datetime_read)        

        registers.append({
                'capture_id':tag,
                'datetime_read': datetime_read,
                'value_active': max_power,
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

        send_registers(registers)



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
    
    for d in data:
        register = set_data(d, tag)
        if len(register) > 0:
            registers.append(register)

    return registers



def get_cycle():

    tag = "AR.LGC.Ciclo_Lote_Processo"

    url = "https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwIIoBAATUVTLVBJLVBST0RcQVIuTEdDLkNJQ0xPX0xPVEVfUFJPQ0VTU08/recorded?filterexpression=BadVal('.')=0&startTime=-1h"

    res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))

    try:
        body = json.loads(res.text)
        if 'Items' in body.keys():
            data = body['Items']
    except Exception as error:
        print(error) 
    
    for d in data:
        register = set_data(d, tag)
        if len(register) > 0:
            registers.append(register)

    return registers



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




if __name__ == '__main__':

    load_dotenv()

    user = os.environ['USER']
    password = os.environ['PASSWORD']


    tags = {
        "AR.LGC.Temperatura_Tira_RTS":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqX8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfUlRT/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Velocidade_Processo":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwmn8BAATUVTLVBJLVBST0RcQVIuTEdDLlZFTE9DSURBREVfUFJPQ0VTU08/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Temperatura_Tira_RTH":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqH8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfUlRI/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.REVESTIMENTO_INFERIOR":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwg_EBAATUVTLVBJLVBST0RcQVIuTEdDLlJFVkVTVElNRU5UT19JTkZFUklPUg/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Concentracao_H2":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwsX8BAATUVTLVBJLVBST0RcQVIuTEdDLkNPTkNFTlRSQUNBT19IMg/recorded?filterexpression=BadVal('.')=0&startTime=--5m",
        "AR.LGC.Temperatura_Tira_JCS":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwq38BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfSkNT/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.VAZAO_N2_FORNO":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwi_EBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX04yX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.VAZAO_GN_FORNO":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjPEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0dOX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Temperatura_Tira_DFF":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwp38BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfREZG/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Temperatura_Tira_SCS":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqn8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfU0NT/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.ESPESSURA":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwgfEBAATUVTLVBJLVBST0RcQVIuTEdDLkVTUEVTU1VSQQ/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.VAZAO_H2_FORNO":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwivEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0gyX0ZPUk5P/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.PRESSAO_RADIANTE":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhfEBAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fUkFESUFOVEU/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Producao_Atual":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwnH8BAATUVTLVBJLVBST0RcQVIuTEdDLlBST0RVQ0FPX0FUVUFM/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.HNX_SETPOINT_SP":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkvEBAATUVTLVBJLVBST0RcQVIuTEdDLkhOWF9TRVRQT0lOVF9TUA/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.DAMPER_INFERIOR_PV":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwifEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9JTkZFUklPUl9QVg/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Pressao_Forno_DFF":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwzn8BAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fRk9STk9fREZG/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Pressao_Snout":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwz38BAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fU05PVVQ/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.ZONA2_DFF_ON":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkPEBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkEyX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.ZONA1_DFF_ON":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkfEBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkExX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Largura_Lote_Processo":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZw3X8BAATUVTLVBJLVBST0RcQVIuTEdDLkxBUkdVUkFfTE9URV9QUk9DRVNTTw/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.Vazao_HNX":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwsH8BAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0hOWA/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.DAMPER_PRINCIPAL_PV":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhvEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9QUklOQ0lQQUxfUFY/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.ELETRICIDADE_LGC":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwk_EBAATUVTLVBJLVBST0RcQVIuTEdDLkVMRVRSSUNJREFERV9MR0M/recorded?filterexpression=BadVal('.')=0&startTime=-17m",
        "AR.LGC.DAMPER_PRINCIPAL_SP":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwh_EBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9QUklOQ0lQQUxfU1A/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.REVESTIMENTO_SUPERIOR":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwgvEBAATUVTLVBJLVBST0RcQVIuTEdDLlJFVkVTVElNRU5UT19TVVBFUklPUg/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.VAZAO_GN_LGC":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjfEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0dOX0xHQw/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.PRESSAO_POST_COMBUSTION":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhPEBAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fUE9TVF9DT01CVVNUSU9O/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.DAMPER_SUPERIOR_PV":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwiPEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9TVVBFUklPUl9QVg/recorded?filterexpression=BadVal('.')=0&startTime=-5m",
        "AR.LGC.ZONA3_DFF_ON":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwj_EBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkEzX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0&startTime=-5m"
    }

    registers = []

    filters = ['AR.LGC.ESPESSURA', 'AR.LGC.REVESTIMENTO_INFERIOR', 'AR.LGC.REVESTIMENTO_SUPERIOR']

    for t in tags:
        url = tags[t]
        if t == 'AR.LGC.ELETRICIDADE_LGC':
            now = datetime.now()
            if now.minute in [16, 31, 46, 1]:
                res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))
        else:
            res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))

        try:
            body = json.loads(res.text)
            if 'Items' in body.keys():
                data = body['Items']

                if t == 'AR.LGC.ELETRICIDADE_LGC':
                    set_measurement(data, t, now)
                elif t in filters:
                    set_processes_filters(data, t)
                else:
                    set_processes(data, t)
        except Exception as error:
            print(error) 
             
        
        

