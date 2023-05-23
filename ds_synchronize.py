
import pandas as pd
import datetime as dt
import numpy as np
import psycopg2
from datetime import datetime

first_time = dt.datetime.now()
dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 

# source
conn_src = psycopg2.connect('postgresql://username:password@host:port/database')
cur_src = conn_src.cursor()

# dest
conn_dest = psycopg2.connect('postgresql://username:password@host:port/database')
cur_dest = conn_dest.cursor()


# sequence process
wh_table_list = ['table_1', 'table_2', '....']
primary_key_list = ['pkey_1','pkey_2','....']
# optional, u can delete this variable. and delete remove column process
columns_to_remove = ['column_1', 'column_2', '....']

# cron_status, optional if u create table for monitoring
cur_src.execute("UPDATE wh_pipeline_status SET status = 'running' where pipeline = 'ds_synchronize'")
conn_src.commit()

for wh_table,primary_key in zip(wh_table_list,primary_key_list):
    flag = True
    while flag:
            
            # write_date_sa_last
            select_query_lwd_sa = "SELECT  write_date,{} FROM {} ORDER BY write_date desc LIMIT 1".format(primary_key,wh_table)
            cur_dest.execute(select_query_lwd_sa)
            write_date_sa_last = cur_dest.fetchall()
            # id_sa_checking = write_date_sa_last[0][1]
            write_date_sa_last = "'"+str(write_date_sa_last[0][0].strftime('%Y-%m-%d %H:%M:%S'))+"'"
           
            # get_data_cs2_wh
            select_query = "SELECT * FROM {} where write_date >= {} ORDER BY write_date asc LIMIT 2000 ".format(wh_table,write_date_sa_last)
            cur_src.execute(select_query)
            data = cur_src.fetchall()

            # extract and remove columns
            column_indexes = [i for i, x in enumerate(cur_src.description) if x.name in columns_to_remove]
            data_without_columns = [tuple(col for i, col in enumerate(row) if i not in column_indexes) for row in data]
            column_names = [col[0] for col in cur_src.description]

            values = data_without_columns 
            columns = list(filter(lambda x: x not in columns_to_remove, column_names))
            strpercent = "".join(['%s',','])* len(columns)
            # print("ok")
            # upsert_data
            if len(values) > 0:
                   sql = "INSERT INTO {} ({}) VALUES({}) ON CONFLICT ({}) DO UPDATE SET {}".format(wh_table,
                    ", ".join(columns),strpercent[:-1],primary_key, ", ".join(["{} = EXCLUDED.{}".format(column, column) for column in columns]))
                   cur_dest.executemany(sql, values)
                   inserted_rows = cur_dest.rowcount
                   conn_dest.commit()

            # get write_date_sa_checking and id
            result = [dict(zip(columns, item)) for item in values]
            result = sorted(result, key=lambda x: x['id'])
            
            datetime_obj = str(result[len(result)-1]['write_date'].replace(microsecond=0))
            write_date_sa_checking = "'"+datetime_obj+"'"
            id_sa_checking = result[len(result)-1]['{}'.format(primary_key)]

            # checking_wh2
            select_query2 = "SELECT {} FROM {} where write_date >= {} ORDER BY write_date desc, id desc LIMIT 2 ".format(primary_key,wh_table,write_date_sa_checking)
            cur_src.execute(select_query2)
            data_checking = cur_src.fetchall()
            id_wh2_checking = data_checking[0][0]

            # print(id_sa_checking,id_wh2_checking)
            # print(write_date_sa_last,write_date_sa_checking)

            if  write_date_sa_last == write_date_sa_checking and id_sa_checking == id_wh2_checking:
                  print("201 Created", "{}".format(wh_table))
                  print("Total rows inserted: ",inserted_rows)
                  flag = False
            else: 
                  print("200 OK", "{}".format(wh_table))
                  print("Total rows inserted: ",inserted_rows)
                  flag = True
            

# cron_status_2, optional if u create table for monitoring.
later_time = dt.datetime.now()
difference = later_time - first_time
seconds_in_day = 24 * 60 * 60
save_date_cron = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S") 
x_time = divmod(difference.days * seconds_in_day + difference.seconds, 60)
x_ms = str(round(difference.total_seconds() * (10 ** 3),2)) + ' ms'
cur_src.execute("UPDATE wh_pipeline_status SET status = 'done', write_date = '{}', peformance = '{}'  where pipeline = 'ds_synchronize' ;".format(save_date_cron,x_ms)) 
conn_src.commit()
print(" processing_time, {}:{} seconds".format(x_time[0],x_time[1]))

print("done")