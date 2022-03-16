#!/usr/bin/python

#Импорт библиотек для работы с БД 

import jaydebeapi

#Создание подключения к БД

conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:DE2TM/balinfundinson@de-oracle.chronosavant.ru:1521/deoracle',
['DE2TM', 'balinfundinson'],
'/home/DE2TM/ojdbc8.jar')

curs = conn.cursor()

conn.jconn.setAutoCommit(False)

#Создание таблиц фактов в БД для загрузки данных из "плоских" файлов

curs.execute("""CREATE TABLE SMAL_DWH_FACT_TRANSACTIONS
						(trans_id int, 
						trans_date timestamp, 
						card_num varchar2(50), 
						oper_type varchar2(50),
						amt	decimal,					
						oper_result varchar2(50), 
						terminal varchar2(50))
			""")
			
curs.execute("""CREATE TABLE SMAL_DWH_FACT_PASSPORT_BLACKLIST 
						(entry_dt date,
						passport_num varchar2 (15))
			""")
			
#Создание стейджинг-таблиц в БД для загрузки данных из "плоских" файлов

curs.execute("""CREATE TABLE SMAL_STG_TERMINALS 
						(terminal_id varchar2(50), 
						terminal_type varchar2(50), 
						terminal_city varchar2(100), 
						terminal_address varchar2(100))
			""")

curs.execute("""CREATE TABLE SMAL_STG_TRANSACTIONS
						(transaction_id varchar2(20), 
						transaction_date timestamp, 
						amount decimal(10,2), 
						card_num varchar2(20), 
						oper_type varchar2(20), 
						oper_result varchar2(20), 
						terminal varchar2(20))
			""")
			
curs.execute("""CREATE TABLE SMAL_STG_PASSPORT_BLACKLIST 
						(entry_dt date,
						passport_num varchar2 (50))
			""")

#Создание стейджинг-таблиц в БД для загрузки данных из схемы Bank

curs.execute("""CREATE TABLE SMAL_STG_ACCOUNTS 
						(account varchar2(20),
						valid_to date,
						client varchar2(20),
						create_dt date,
						update_dt date)
			""")

curs.execute("""CREATE TABLE SMAL_STG_CARDS 
						(card_num varchar2(20),
						account varchar2(20),
						create_dt date,
						update_dt date)
			""")
			
curs.execute("""CREATE TABLE SMAL_STG_CLIENTS 
						(client_id varchar2(30),
						last_name varchar2(100),
						first_name varchar2(100),
						patronymic varchar2(100),
						date_of_birth date,
						passport_num varchar2(15),
						passport_valid_to date,
						phone varchar2(20),
						create_dt date,
						update_dt date)
			""")		

#Создание таблиц удалений в БД 

curs.execute("""CREATE TABLE SMAL_STG_ACCOUNTS_DEL 
						(account varchar2(20))
			""")

curs.execute("""CREATE TABLE SMAL_STG_CARDS_DEL 
						(card_num varchar2(20))
			""")
			
curs.execute("""CREATE TABLE SMAL_STG_CLIENTS_DEL 
						(client_id varchar2(20))
			""")

#Создание таблиц с метаданными в БД

curs.execute("""CREATE TABLE SMAL_META_DATA 
						(last_update_dt date,
						dbname varchar2(10),
						table_name varchar2(50))
			""")

#Первоначальное заполнение таблиц с метаданными в БД

curs.execute("""INSERT INTO SMAL_META_DATA 
						(last_update_dt, DBNAME, TABLE_NAME)
						VALUES 
						(TO_DATE ('1899-01-01', 'YYYY-MM-DD'), 
						'DE2TM', 
						'SMAL_DWH_DIM_ACCOUNTS_HIST')
			""")		
			
curs.execute("""INSERT INTO SMAL_META_DATA 
						(last_update_dt, DBNAME, TABLE_NAME)
						VALUES 
						(TO_DATE ('1899-01-01', 'YYYY-MM-DD'), 
						'DE2TM', 
						'SMAL_DWH_DIM_CARDS_HIST')
			""")

curs.execute("""INSERT INTO SMAL_META_DATA 
						(last_update_dt, DBNAME, TABLE_NAME)
						VALUES 
						(TO_DATE ('1899-01-01', 'YYYY-MM-DD'), 
						'DE2TM', 
						'SMAL_DWH_DIM_CLIENTS_HIST')
			""")

curs.execute("""INSERT INTO SMAL_META_DATA 
						(last_update_dt, DBNAME, TABLE_NAME)
						VALUES 
						(TO_DATE ('1899-01-01', 'YYYY-MM-DD'), 
						'DE2TM', 
						'SMAL_DWH_DIM_TERMINALS_HIST')
			""")

curs.execute("""INSERT INTO SMAL_META_DATA 
						(last_update_dt, DBNAME, TABLE_NAME)
						VALUES 
						(TO_DATE ('1899-01-01', 'YYYY-MM-DD'), 
						'DE2TM', 
						'SMAL_DWH_FACT_TRANSACTIONS')
			""")

curs.execute("""INSERT INTO SMAL_META_DATA 
						(last_update_dt, DBNAME, TABLE_NAME)
						VALUES 
						(TO_DATE ('1899-01-01', 'YYYY-MM-DD'), 
						'DE2TM', 
						'SMAL_DWH_FACT_PASSPORT_BLACKLIST')
			""")

#Создание таблиц измерений в БД

curs.execute("""CREATE TABLE SMAL_DWH_DIM_ACCOUNTS_HIST 
						(account_num varchar2(20), 
						valid_to date, 
						client varchar2(30), 
						effective_from date, 
						effective_to date, 
						deleted_flg varchar2(1))
			""")

curs.execute("""CREATE TABLE SMAL_DWH_DIM_CARDS_HIST 
						(card_num varchar2(30),
						account_num varchar2(20), 
						effective_from date, 
						effective_to date, 
						deleted_flg varchar2(1))
			""")
			
curs.execute("""CREATE TABLE SMAL_DWH_DIM_CLIENTS_HIST 
						(client_id varchar2(30), 
						last_name varchar2(100), 
						first_name varchar2(100), 
						patronymic varchar2(100), 
						date_of_birth date, 
						passport_num varchar2(15), 
						passport_valid_to date, 
						phone varchar2(20), 
						effective_from date, 
						effective_to date, 
						deleted_flg varchar2(1))
			""")

curs.execute("""CREATE TABLE SMAL_DWH_DIM_TERMINALS_HIST 
						(terminal_id varchar2(50), 
						terminal_type varchar2(50), 
						terminal_city varchar2(100), 
						terminal_address varchar2(100), 
						effective_from date, 
						effective_to date, 
						deleted_flg varchar2(1))
			""")

#Создание таблицы отчета в БД

curs.execute("""CREATE TABLE SMAL_REP_FRAUD 
						(event_dt timestamp, 
						passport varchar2(15), 
						fio varchar2(150), 
						phone varchar2(20), 
						event_type varchar2(150), 
						report_dt timestamp)
			""")


#Коммит изменений в таблицах БД
conn.commit()


#Закрытие соединения с БД

curs.close()
conn.close()