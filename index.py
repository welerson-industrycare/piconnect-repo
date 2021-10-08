import os
import requests
import json
from requests.auth import HTTPBasicAuth
from datetime import datetime, timedelta
from dotenv import load_dotenv


def set_date(date):

    date_str = date.split('.')[0]

    date_str = date_str.replace('Z', '')

    date_time_obj = datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S')

    hours = timedelta(hours=3)

    date_time_obj = date_time_obj - hours

    date_time = date_time_obj.strftime('%Y-%m-%dT%H:%M:%S-03:00')

    return date_time



def set_processes(data, tag):

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


def set_measurement(data, tag):

    registers = []

    for d in data:

        datetime_read = set_date(d['Timestamp'])        

        registers.append({
                'capture_id':tag,
                'datetime_read': datetime_read,
                'value_active': d['Value'],
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

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')


    tags = {
        "AR.LGC.Temperatura_Tira_RTS":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqX8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfUlRT/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Velocidade_Processo":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwmn8BAATUVTLVBJLVBST0RcQVIuTEdDLlZFTE9DSURBREVfUFJPQ0VTU08/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Temperatura_Tira_RTH":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqH8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfUlRI/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.REVESTIMENTO_INFERIOR":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwg_EBAATUVTLVBJLVBST0RcQVIuTEdDLlJFVkVTVElNRU5UT19JTkZFUklPUg/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Concentracao_H2":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwsX8BAATUVTLVBJLVBST0RcQVIuTEdDLkNPTkNFTlRSQUNBT19IMg/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Temperatura_Tira_JCS":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwq38BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfSkNT/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.VAZAO_N2_FORNO":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwi_EBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX04yX0ZPUk5P/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.VAZAO_GN_FORNO":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjPEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0dOX0ZPUk5P/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Temperatura_Tira_DFF":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwp38BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfREZG/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Temperatura_Tira_SCS":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwqn8BAATUVTLVBJLVBST0RcQVIuTEdDLlRFTVBFUkFUVVJBX1RJUkFfU0NT/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.ESPESSURA":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwgfEBAATUVTLVBJLVBST0RcQVIuTEdDLkVTUEVTU1VSQQ/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.VAZAO_H2_FORNO":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwivEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0gyX0ZPUk5P/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.PRESSAO_RADIANTE":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhfEBAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fUkFESUFOVEU/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Producao_Atual":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwnH8BAATUVTLVBJLVBST0RcQVIuTEdDLlBST0RVQ0FPX0FUVUFM/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.HNX_SETPOINT_SP":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkvEBAATUVTLVBJLVBST0RcQVIuTEdDLkhOWF9TRVRQT0lOVF9TUA/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.DAMPER_INFERIOR_PV":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwifEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9JTkZFUklPUl9QVg/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Pressao_Forno_DFF":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwzn8BAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fRk9STk9fREZG/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Pressao_Snout":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwz38BAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fU05PVVQ/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.ZONA2_DFF_ON":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkPEBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkEyX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.ZONA1_DFF_ON":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwkfEBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkExX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Largura_Lote_Processo":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZw3X8BAATUVTLVBJLVBST0RcQVIuTEdDLkxBUkdVUkFfTE9URV9QUk9DRVNTTw/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.Vazao_HNX":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwsH8BAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0hOWA/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.DAMPER_PRINCIPAL_PV":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhvEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9QUklOQ0lQQUxfUFY/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.ELETRICIDADE_LGC":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwk_EBAATUVTLVBJLVBST0RcQVIuTEdDLkVMRVRSSUNJREFERV9MR0M/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.DAMPER_PRINCIPAL_SP":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwh_EBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9QUklOQ0lQQUxfU1A/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.REVESTIMENTO_SUPERIOR":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwgvEBAATUVTLVBJLVBST0RcQVIuTEdDLlJFVkVTVElNRU5UT19TVVBFUklPUg/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.CICLO_FORNO":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjvEBAATUVTLVBJLVBST0RcQVIuTEdDLkNJQ0xPX0ZPUk5P/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.VAZAO_GN_LGC":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwjfEBAATUVTLVBJLVBST0RcQVIuTEdDLlZBWkFPX0dOX0xHQw/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.PRESSAO_POST_COMBUSTION":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwhPEBAATUVTLVBJLVBST0RcQVIuTEdDLlBSRVNTQU9fUE9TVF9DT01CVVNUSU9O/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.DAMPER_SUPERIOR_PV":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwiPEBAATUVTLVBJLVBST0RcQVIuTEdDLkRBTVBFUl9TVVBFUklPUl9QVg/recorded?filterexpression=BadVal('.')=0",
        "AR.LGC.ZONA3_DFF_ON":"https://pivr.csn.com.br/piwebapi/streams/F1DPuIGA3ZNCXkyVOdMdUR1vZwj_EBAATUVTLVBJLVBST0RcQVIuTEdDLlpPTkEzX0RGRl9PTg/recorded?filterexpression=BadVal('.')=0"
    }

    registers = []


    for t in tags:
        url = tags[t]
        res = requests.get(url, verify=False, auth=HTTPBasicAuth(user, password))
        body = json.loads(res.text)
        data = body['Items']


        if t == 'AR.LGC.ELETRICIDADE_LGC':
            set_measurement(data, t)
        else:
            set_processes(data, t)
        

