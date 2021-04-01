import pymysql
import pandas as pd
import datetime as dt
import logging
from tqdm import tqdm
import time
def table_diff():
        logging.basicConfig(filename = "db_table_test.log",level = "DEBUG")
        month_tab_list=[]
        db = pymysql.connect('localhost','root','sqladmin','czentrix_campaign_manager')
        logging.info(db)
        d_list=[]
        cur = db.cursor()
        table_Dict = {'month':['barge_sample','cdr_sample','escalation_sample','feedback_sample','ivr_sample','magic_call_sample','magic_call_sample_stats','mail_report_sample','misscall_sample','sms_report_sample','voicemail_sample','agent_side_reporting_sample'],"campaign":['customer_sample','crm_sample','dial_Lead_lookup_sample','dial_Lead_sample','dial_in_progress_sample','dial_state_sample','extended_customer_sample','sess_history_sample']}
        samp_tables = ['ACD_sample','agent_sample','mail_report_sample']
#--------------Getting the sample tables from db----------#
        cur.execute("show tables like '%sample%'")
        sample_tables = cur.fetchall()
        logging.info(sample_tables)
#--------------Month Tables---------------------------------#
        date_list = []
        date = dt.datetime.now()
        date2 = dt.date.today() - pd.offsets.DateOffset(months=2)
        date1 = dt.date.today() - pd.offsets.DateOffset(months=1)
        date_2  = dt.date.today() + pd.offsets.DateOffset(months=2)
        date_1  = dt.date.today() + pd.offsets.DateOffset(months=1)
        date = str(date)
        date2 = str(date2)
        date1 = str(date1)
        date_2 = str(date_2)
        date_1 = str(date_1)
#----------getting month table name from dates--------------#
        date = date[:7].replace("-","_")
        date_1 = date_1[:7].replace("-","_")
        date_2 = date_2[:7].replace("-","_")
        date1 = date1[:7].replace("-","_")
        date2 = date2[:7].replace("-","_")
        date_list.append(date_1)
        date_list.append(date_2)
        date_list.append(date)
        date_list.append(date1)
        date_list.append(date2)
#-------------------------------------#

        for val_ in table_Dict.get('month'):
              for date_val_ in tqdm(date_list):
                       if val_ == 'barge_sample':
                                 emp_val = val_[:6]+str(date_val_)
                                 month_tab_list.append(emp_val)

                       elif val_ == 'cdr_sample':
                                 emp_val = val_[:4]+str(date_val_)
                                 month_tab_list.append(emp_val)

                       elif val_ == 'escalation_sample':

                                 emp_val = val_[:11]+str(date_val_)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'feedback_sample':
                                 emp_val = val_[:9]+str(date_val_)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'ivr_sample':
                                 emp_val = val_[:4]+"report_"+str(date_val_)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'voicemail_sample':
                                 emp_val = val_[:10]+str(date_val_)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'sms_report_sample':
                                 emp_val = val_[:11]+str(date_val_)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'misscall_sample':
                                 emp_val = val_[:9]+str(date_val_)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'mail_report_sample':
                                 emp_val = val_[:12]+str(date_val_)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'magic_call_sample_stats':
                                 emp_val = val_[:11]+'stats_'+str(date_val_)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'magic_call_sample':
                                 emp_val = val_[:11]+str(emp_val)
                                 month_tab_list.append(emp_val)
                       elif val_ == 'agent_side_reporting_sample':
                                 emp_val = val_[:21]+str(date_val_)
                                 month_tab_list.append(emp_val)
        logging.info(month_tab_list)
        agent_val = 'agent_state_analysis_'
        for s_table in tqdm(samp_tables):
             for date_table in tqdm(date_list):

               try:
                  if s_table == 'agent_sample':
                       date_table = agent_val+str(date_table)
                  else:
                       pass
                  cur.execute(f"select table_name,column_name,column_type,column_default,is_nullable from information_schema.columns where table_name in('{s_table}', '{date_table}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                  logging.info(f"select table_name,column_name,column_type,column_default,is_nullable from information_schema.columns where table_name in('{s_table}', '{date_table}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                  diff_in_tables = cur.fetchall()
                  if diff_in_tables or diff_in_tables != None or diff_in_tables != '':
                     for diff_table_data in diff_in_tables:
                        table_name = diff_table_data[0]
                        column = diff_table_data[1]
                        data_type = diff_table_data[2]
                        column_default = diff_table_data[3]
                        is_nullable = diff_table_data[4]
                        if table_name == s_table:
                                table_name = date_table
                                logging.info(table_name)
                                if is_nullable == "YES" and column_default is None:
                                    column_default = "Null"
                                    logging.info(
                                        f"EXECUTED FOR --> {table_name} ADD column {column} {data_type} DEFAULT {column_default};")
                                    cur.execute(
                                        f"ALTER TABLE {table_name} ADD column {column} {data_type} DEFAULT {column_default};")
                                else:
                                    if column_default == "CURRENT_TIMESTAMP":
                                        logging.info(
                                            f"EXECUTED FOR --> {table_name} ADD column {column} {data_type} DEFAULT {column_default};")
                                        cur.execute(
                                            f"ALTER TABLE {table_name} ADD column {column} {data_type} DEFAULT {column_default};")
                                    else:
                                            logging.info(f"EXECUTED FOR -->{table_name} ADD column {column} {data_type} not null DEFAULT '{column_default}';")
                                            cur.execute(f"ALTER TABLE {table_name} ADD column {column} {data_type}  not null DEFAULT  '{column_default} ';")

                        else:
                              pass
               except Exception as e:
                           logging.info(e)

        for sample_table in tqdm(table_Dict.get('month')):
                for month_table in tqdm(month_tab_list):
                        cur.execute(f"select table_name,column_name,column_type,column_default,is_nullable from information_schema.columns where table_name in('{sample_table}', '{month_table}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                        diff_in_tables = cur.fetchall()
                        if diff_in_tables or diff_in_tables!= None or diff_in_tables != '':
                                for diff_table_data in diff_in_tables:
                                        table_name = diff_table_data[0]
                                        column = diff_table_data[1]
                                        data_type = diff_table_data[2]
                                        column_default = diff_table_data[3]
                                        is_nullable = diff_table_data[4]

                                        if table_name == sample_table:
                                                table_name = month_table
                                                try:
                                                                logging.info(table_name)
                                                                if is_nullable == "YES" and column_default is None:
                                                                        column_default = "Null"
                                                                        logging.info(f"EXECUTED FOR --> {table_name} ADD column {column} {data_type} DEFAULT {column_default};")
                                                                        cur.execute(f"ALTER TABLE {table_name} ADD column {column} {data_type} DEFAULT {column_default};")
                                                                else:
                                                                        if column_default == "CURRENT_TIMESTAMP":
                                                                                logging.info(f"EXECUTED FOR --> {table_name} ADD column {column} {data_type} DEFAULT {column_default};")
                                                                                cur.execute(f"ALTER TABLE {table_name} ADD column {column} {data_type} DEFAULT {column_default};")
                                                                        else:
                                                                                logging.info(f"EXECUTED FOR -->{table_name} ADD column {column} {data_type} not null DEFAULT '{column_default}';")
                                                                                cur.execute(f"ALTER TABLE {table_name} ADD column {column} {data_type}  not null DEFAULT  '{column_default} ';")



                                                except Exception as e:
                                                                logging.info(e)
                                        else:
                                                pass

#------------Now doing it for campaign_id wise tables--------------#


        c_list=[]
        logging.info("Now doing it for campaign_id wise tables")
        cur.execute("select campaign_id from campaign;")
        campaign_id = cur.fetchall()
        campaign_id_table = []
        campaign_id_Dict = {"customer":[],"dial_Lead_lookup_sample":[],"dial_Lead_sample":[],"crm_sample":[],'dial_in_progress_sample':[],'dial_state_sample':[],'extended_customer_sample':[],'sess_history_sample':[]}

        try:
                for sample_table in tqdm(table_Dict.get('campaign')):

                          for c_id in campaign_id:
                                campaign_id_val = str(c_id[0])
                                campaign_id_tables = []
                                if sample_table == "customer_sample":
                                        campaign_id_Dict["customer"].append(sample_table.split("_")[0]+"_"+campaign_id_val)

                                elif sample_table == "crm_sample":
                                        campaign_id_Dict["crm_sample"].append(sample_table.split("_")[0]+"_"+campaign_id_val)

                                elif sample_table == "dial_Lead_lookup_sample":
                                        campaign_id_Dict["dial_Lead_lookup_sample"].append(sample_table[:16]+"_"+campaign_id_val)

                                elif sample_table =="dial_Lead_sample":
                                        campaign_id_Dict["dial_Lead_sample"].append(sample_table[:9]+"_"+campaign_id_val)

                                elif sample_table == "dial_in_progress_sample":
                                        campaign_id_Dict["dial_in_progress_sample"].append(sample_table[:16]+"_"+campaign_id_val)

                                elif sample_table == "dial_state_sample":
                                        campaign_id_Dict["dial_state_sample"].append(sample_table[:10]+"_"+ campaign_id_val)

                                elif sample_table == "extended_customer_sample":
                                        campaign_id_Dict["extended_customer_sample"].append(sample_table[:17] + "_" + campaign_id_val)

                                elif sample_table == "sess_history_sample":
                                        campaign_id_Dict["sess_history_sample"].append(sample_table[:12] + "_" + campaign_id_val)

        except Exception as e:
                                logging.info("info",e)
        logging.info(campaign_id_Dict)
####################################
        c = table_Dict.get("campaign")
        for l in c:
            try:
                 if l == "customer_sample":
                        for k in campaign_id_Dict.get('customer'):
                                cur.execute(f"select table_name,column_name,column_type,column_default, is_nullable from information_schema.columns where table_name in ('{k}','{l}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                                diff_data = cur.fetchall()
                                ok_to_proceed = 0
                                if diff_data and diff_data is not None and diff_data is not '' and diff_data is not ' ':
                                        ok_to_proceed = 1
                                if diff_data and ok_to_proceed == 1:
                                        for i in diff_data:

                                                column_name = i[1]
                                                table_name = i[0]
                                                data_type = i[2]
                                                column_default = i[3]
                                                is_nullable = i[4]
                                                if table_name == l:
                                                        table_name = k
                                                        if is_nullable == "YES" and column_default is None:
                                                                column_default = "Null"
                                                                logging.info(f" yes alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                cur.execute(f"alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                        else:
                                                                if column_default == 'CURRENT_TIMESTAMP':
                                                                        logging.info(f"no alter table {table_name} add column {column_name} {data_type} not null default {column_default};")
                                                                        cur.execute(f"alter table {table_name} add column {column_name} {data_type} not null default {column_default}")
                                                                else:
                                                                        logging.info(f"no alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")
                                                                        cur.execute(f"alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")

                                                else:
                                                        pass


                 elif l == 'dial_Lead_lookup_sample':

                             for k in campaign_id_Dict.get('dial_Lead_lookup_sample'):
                                        cur.execute(
                                                f"select table_name,column_name,column_type,column_default, is_nullable from information_schema.columns where table_name in ('{k}','{l}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                                        diff_data = cur.fetchall()
                                        ok_to_proceed = 0
                                        if diff_data and diff_data is not None and diff_data is not '' and diff_data is not ' ':
                                                ok_to_proceed = 1
                                        if diff_data and ok_to_proceed == 1:
                                                for i in diff_data:
                                                        column_name = i[1]
                                                        table_name = i[0]
                                                        data_type = i[2]
                                                        column_default = i[3]
                                                        is_nullable = i[4]
                                                        if table_name == l:
                                                                table_name = k
                                                                if is_nullable == "YES" and column_default is None:
                                                                        column_default = "Null"
                                                                        logging.info(f" yes alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                        cur.execute(f"alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                else:
                                                                        if column_default == 'CURRENT_TIMESTAMP':
                                                                                logging.info(f"no alter table {table_name} add column {column_name} {data_type} not null default {column_default};")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default {column_default}")
                                                                        else:
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")

                                                        else:
                                                                pass

#------------------------------------#

                 elif l == 'dial_Lead_sample':
                         for k in campaign_id_Dict.get('dial_Lead_sample'):
                                        cur.execute(
                                                f"select table_name,column_name,column_type,column_default, is_nullable from information_schema.columns where table_name in ('{k}','{l}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                                        diff_data = cur.fetchall()
                                        ok_to_proceed = 0
                                        if diff_data and diff_data is not None and diff_data is not '' and diff_data is not ' ':
                                                ok_to_proceed = 1
                                        if diff_data and ok_to_proceed == 1:
                                                for i in diff_data:
                                                        column_name = i[1]
                                                        table_name = i[0]
                                                        data_type = i[2]
                                                        column_default = i[3]
                                                        is_nullable = i[4]
                                                        if table_name == l:
                                                                table_name = k
                                                                if is_nullable == "YES" and column_default is None:
                                                                        column_default = "Null"
                                                                        logging.info(
                                                                                f" yes alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                        cur.execute(
                                                                                f"alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                else:
                                                                        if column_default == 'CURRENT_TIMESTAMP':
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default {column_default};")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default {column_default}")
                                                                        else:
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")

                                                        else:
                                                                pass
#----------------------------------------#


                 elif l == 'crm_sample':
                           for k in campaign_id_Dict.get('crm_sample'):
                                        cur.execute(
                                                f"select table_name,column_name,column_type,column_default, is_nullable from information_schema.columns where table_name in ('{k}','{l}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                                        diff_data = cur.fetchall()
                                        ok_to_proceed = 0
                                        if diff_data and diff_data is not None and diff_data is not '' and diff_data is not ' ':
                                                ok_to_proceed = 1
                                        if diff_data and ok_to_proceed == 1:
                                                for i in diff_data:
                                                        column_name = i[1]
                                                        table_name = i[0]
                                                        data_type = i[2]
                                                        column_default = i[3]
                                                        is_nullable = i[4]
                                                        if table_name == l:
                                                                table_name = k
                                                                if is_nullable == "YES" and column_default is None:
                                                                        column_default = "Null"
                                                                        logging.info(f" yes alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                        cur.execute(f"alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                else:
                                                                        if column_default == 'CURRENT_TIMESTAMP':
                                                                                logging.info(f"no alter table {table_name} add column {column_name} {data_type} not null default {column_default};")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default {column_default}")
                                                                        else:
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")

                                                        else:
                                                                pass

#------------------------------------#

                 elif l == 'dial_state_sample':
                         for k in campaign_id_Dict.get('dial_state_sample'):
                                        cur.execute(f"select table_name,column_name,column_type,column_default, is_nullable from information_schema.columns where table_name in ('{k}','{l}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                                        diff_data = cur.fetchall()
                                        ok_to_proceed = 0
                                        if diff_data and diff_data is not None and diff_data is not '' and diff_data is not ' ':
                                                ok_to_proceed = 1
                                        if diff_data and ok_to_proceed == 1:
                                                for i in diff_data:
                                                        column_name = i[1]
                                                        table_name = i[0]
                                                        data_type = i[2]
                                                        column_default = i[3]
                                                        is_nullable = i[4]
                                                        if table_name == l:
                                                                table_name = k
                                                                if is_nullable == "YES" and column_default is None:
                                                                        column_default = "Null"
                                                                        logging.info(f" yes alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                        cur.execute(f"alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                else:
                                                                        if column_default == 'CURRENT_TIMESTAMP':
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default {column_default};")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default {column_default}")
                                                                        else:
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")

                                                        else:
                                                                pass

                 elif  l == 'extended_customer_sample':

                         for k in campaign_id_Dict.get('extended_customer_sample'):

                                        cur.execute(
                                                f"select table_name,column_name,column_type,column_default, is_nullable from information_schema.columns where table_name in ('{k}','{l}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                                        diff_data = cur.fetchall()
                                        ok_to_proceed = 0
                                        if diff_data and diff_data is not None and diff_data is not '' and diff_data is not ' ':
                                                ok_to_proceed = 1
                                        if diff_data and ok_to_proceed == 1:
                                                for i in diff_data:
                                                        column_name = i[1]
                                                        table_name = i[0]
                                                        data_type = i[2]
                                                        column_default = i[3]
                                                        is_nullable = i[4]
                                                        if table_name == l:
                                                                table_name = k
                                                                if is_nullable == "YES" and column_default is None:
                                                                        column_default = "Null"
                                                                        logging.info(
                                                                                f" yes alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                        cur.execute(
                                                                                f"alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                else:
                                                                        if column_default == 'CURRENT_TIMESTAMP':
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default {column_default};")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default {column_default}")
                                                                        else:
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")

                                                        else:
                                                                pass
#-------------------------------------------#


                 elif l == 'dial_in_progress_sample':
                      for k in campaign_id_Dict.get('dial_in_progress_sample'):
                                        cur.execute(
                                                f"select table_name,column_name,column_type,column_default, is_nullable from information_schema.columns where table_name in ('{k}','{l}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                                        diff_data = cur.fetchall()
                                        ok_to_proceed = 0
                                        if diff_data and diff_data is not None and diff_data is not '' and diff_data is not ' ':
                                                ok_to_proceed = 1
                                        if diff_data and ok_to_proceed == 1:
                                                for i in diff_data:
                                                        column_name = i[1]
                                                        table_name = i[0]
                                                        data_type = i[2]
                                                        column_default = i[3]
                                                        is_nullable = i[4]
                                                        if table_name == l:
                                                                table_name = k
                                                                if is_nullable == "YES" and column_default is None:
                                                                        column_default = "Null"
                                                                        logging.info(
                                                                                f" yes alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                        cur.execute(
                                                                                f"alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                else:
                                                                        if column_default == 'CURRENT_TIMESTAMP':
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default {column_default};")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default {column_default}")
                                                                        else:
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")

                                                        else:
                                                                logging.info(f"Alter query did not executed for {table_name} table")

                 elif  l == 'sess_history_sample':
                      for k in campaign_id_Dict.get('sess_history_sample'):

                                        cur.execute(f"select table_name,column_name,column_type,column_default, is_nullable from information_schema.columns where table_name in ('{k}','{l}') and table_schema = 'czentrix_campaign_manager' group by column_name having count(*) = 1;")
                                        diff_data = cur.fetchall()
                                        ok_to_proceed = 0
                                        if diff_data and diff_data is not None and diff_data is not '' and diff_data is not ' ':
                                                ok_to_proceed = 1
                                        if diff_data and ok_to_proceed == 1:
                                                for i in diff_data:
                                                        column_name = i[1]
                                                        table_name = i[0]
                                                        data_type = i[2]
                                                        column_default = i[3]
                                                        is_nullable = i[4]
                                                        if table_name == l:
                                                                table_name = k
                                                                if is_nullable == "YES" and column_default is None:
                                                                        column_default = "Null"
                                                                        logging.info(
                                                                                f" yes alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                        cur.execute(
                                                                                f"alter table {table_name} add column {column_name} {data_type} default {column_default};")
                                                                else:
                                                                        if column_default == 'CURRENT_TIMESTAMP':
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default {column_default};")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default {column_default}")
                                                                        else:
                                                                                logging.info(
                                                                                        f"no alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")
                                                                                cur.execute(
                                                                                        f"alter table {table_name} add column {column_name} {data_type} not null default '{column_default}';")

                                                        else:
                                                                pass
            except Exception as e:
                   logging.info(e)
###################3#########################





table_diff()
