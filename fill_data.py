import os
import sys
import psycopg2
import traceback
import logging


def connect():

    conn = None

    try:
        dbname = os.environ['DB_NAME']
        user = os.environ['DB_USER']
        host = os.environ['DB_HOST']
        password = os.environ['DB_PASSWORD']

        conn = psycopg2.connect(
            dbname=dbname,
            user=user,
            host=host,
            password=password
        )

    except Exception as error:
        print(error) 
        print(traceback.format_exc())

    finally:
        if conn is not None:
            return conn


def filled_processes():

    conn = None

    rows = -1

    sql = """
        SELECT
            DISTINCT d
        FROM generate_series((now() - interval '1 days')::date, now(), '5 seconds') d
        LEFT JOIN  processes p ON p.datetime_read = d
        WHERE
            p.p_value is null
        ORDER BY 1 ASC    
    """

    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        rows = list(cur.fetchall())
        current = rows[0][0]
        count = 1
        date_from = current
        while(current != rows[-1][0]):
           if int((rows[count][0] - current).total_seconds()) != 5:
               date_to = rows[count-1][0]
               date_1 = current.strftime('%Y:%m-%dT%H:%M')
               date_2 = date_to.strftime('%Y:%m-%dT%H:%M')
               date_from = date_to 
           current = rows[count][0]    
           count += 1    
        cur.close()

    except Exception as error:
        print(error) 
        print(traceback.format_exc())
    
    finally:
        if conn is not None:
            conn.close()
            return rows


def filled_measurement():

    conn = None

    rows = -1

    sql = """
        SELECT
            DISTINCT d
        FROM generate_series((now() - interval '30 days')::date, now(), '15 minutes') d
        LEFT JOIN  measurement m ON m.datetime_read = d
        WHERE
            p.p_value is null
        ORDER BY 1 ASC    
    """

    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        rows = list(cur.fetchall())
        current = rows[0][0]
        count = 1
        date_from = current
        while(current != rows[-1][0]):
           if int((rows[count][0] - current).total_seconds()) != 5:
               date_to = rows[count-1][0]
            
               #TODO realizar extração do date_from até o date_to
               date_from = date_to 
           current = rows[count][0]    
           count += 1    
        cur.close()

    except Exception as error:
        print(error) 
        print(traceback.format_exc())
    
    finally:
        if conn is not None:
            conn.close()
            return rows


def filled_filter_processes():

    conn = None

    rows = -1

    sql = """
        SELECT
            DISTINCT d
        FROM generate_series((now() - interval '30 days')::date, now(), '5 seconds') d
        LEFT JOIN  filter_processes f ON f.datetime_read = d
        WHERE
            p.p_value is null
        ORDER BY 1 ASC    
    """

    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql)
        rows = list(cur.fetchall())
        current = rows[0][0]
        count = 1
        date_from = current
        while(current != rows[-1][0]):
           if int((rows[count][0] - current).total_seconds()) != 5:
               date_to = rows[count-1][0]
            
               #TODO realizar extração do date_from até o date_to
               date_from = date_to 
           current = rows[count][0]    
           count += 1    
        cur.close()

    except Exception as error:
        print(error) 
        print(traceback.format_exc())
    
    finally:
        if conn is not None:
            conn.close()
            return rows


if __name__ == '__main__':

    filled_processes()