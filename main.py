#!/usr/bin/python

# Все таблицы в БД создаются скриптом .\Py_scripts\create_tables.py)

#Импорт библиотек для работы с БД и загрузки файлов с информацией

import os
import glob
import datetime

import jaydebeapi
import pandas as pd

#Создание подключения к БД

conn = jaydebeapi.connect(
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:DE2TM/balinfundinson@de-oracle.chronosavant.ru:1521/deoracle',
['DE2TM', 'balinfundinson'],
'/home/DE2TM/ojdbc8.jar')

curs = conn.cursor()
conn.jconn.setAutoCommit(False)

#Загрузка данных из "плоских" файлов в датафреймы для обработки перед загрузкой в БД.

passport_path = glob.glob('Plain_files/pas*.xlsx')[0]
terminals_path = glob.glob('Plain_files/ter*.xlsx')[0]
transactions_path = glob.glob('Plain_files/tra*.csv')[0]

passport_blacklist_df = pd.read_excel(passport_path)
passport_blacklist_df['date'] = passport_blacklist_df['date'].astype(str)

terminals_df = pd.read_excel(terminals_path)

transactions_df = pd.read_csv(transactions_path, delimiter = ';', decimal=",")
transactions_df['transaction_date'] = transactions_df['transaction_date'].astype(str)

#Определение даты загрузки данных (выделение даты из имени "плоского" файла) и даты следующего дня

terminals_dt = terminals_path.split('_')[2].split('.')[0]
terminals_dt = datetime.datetime.strptime(terminals_dt, '%d%m%Y')
terminals_dt = datetime.datetime.date(terminals_dt)

terminals_dt_next = terminals_dt + datetime.timedelta(days=1)

terminals_dt_str = str(terminals_dt)
terminals_dt_next_str = str(terminals_dt_next)

#Перемещение загруженных "плоских" файлов в каталог Archive

os.replace(passport_path,  os.path.join('Archive', os.path.basename(passport_path + '.backup')))
os.replace(terminals_path,  os.path.join('Archive', os.path.basename(terminals_path + '.backup')))
os.replace(transactions_path,  os.path.join('Archive', os.path.basename(transactions_path + '.backup')))

#Очистка стейджинг-таблиц в БД

curs.execute("""DELETE FROM DE2TM.SMAL_STG_ACCOUNTS""")
curs.execute("""DELETE FROM DE2TM.SMAL_STG_CARDS""")
curs.execute("""DELETE FROM DE2TM.SMAL_STG_CLIENTS""")

#Очистка таблиц фактов в БД

curs.execute("""DELETE FROM DE2TM.SMAL_STG_PASSPORT_BLACKLIST""")
curs.execute("""DELETE FROM DE2TM.SMAL_STG_TERMINALS""")
curs.execute("""DELETE FROM DE2TM.SMAL_STG_TRANSACTIONS""")

#Очистка таблиц удалений в БД

curs.execute("""DELETE FROM DE2TM.SMAL_STG_ACCOUNTS_DEL""")
curs.execute("""DELETE FROM DE2TM.SMAL_STG_CARDS_DEL""")
curs.execute("""DELETE FROM DE2TM.SMAL_STG_CLIENTS_DEL""")

#Загрузка даты последнего апдейта из таблицы метаданных

curs.execute("""SELECT last_update_dt FROM DE2TM.SMAL_META_DATA
                WHERE dbname = 'DE2TM' 
				AND table_name = 'SMAL_DWH_FACT_PASSPORT_BLACKLIST' """)
				
meta_passport_last_update_dt = curs.fetchone()[0]
meta_passport_last_update_dt = datetime.datetime.strptime(meta_passport_last_update_dt, '%Y-%m-%d %H:%M:%S')
passport_blacklist_df = passport_blacklist_df[passport_blacklist_df.date > meta_passport_last_update_dt]

#Загрузка датафреймов (данные из "плоских" файлов) в стейджинг-таблицы БД 

curs.executemany("""INSERT INTO SMAL_STG_TRANSACTIONS 
						 VALUES 
							(?,
							to_date(?, 'YYYY-MM-DD HH24:MI:SS'),
							?,
							?,
							?,
							?,
							?)""", 
				transactions_df.values.tolist())
				
curs.executemany("""INSERT INTO DE2TM.SMAL_STG_TERMINALS 
						VALUES 
							(?,
							?,
							?,
							?)""", 
				terminals_df.values.tolist())
				
curs.executemany("""INSERT INTO DE2TM.SMAL_STG_PASSPORT_BLACKLIST 
						VALUES 
							(?,
							TO_DATE(?, 'YYYY-MM-DD'))""", 
				passport_blacklist_df.values.tolist())

#Загрузка данных в стейджинг-таблицы в БД

curs.execute("""INSERT INTO DE2TM.SMAL_STG_ACCOUNTS
					SELECT 
						account,
						valid_to,
						client,
						create_dt,
						update_dt
					FROM 
						BANK.ACCOUNTS
					WHERE
						COALESCE(update_dt, create_dt) >
						(SELECT
							last_update_dt
						FROM
							DE2TM.SMAL_META_DATA
						WHERE
							dbname = 'DE2TM'
							AND
							table_name = 'SMAL_DWH_DIM_ACCOUNTS_HIST')""")

curs.execute("""INSERT INTO DE2TM.SMAL_STG_CARDS
					SELECT 
						card_num,
						account,
						create_dt,
						update_dt
					FROM 
						BANK.CARDS
					WHERE
						COALESCE(update_dt, create_dt) >
						(SELECT
							last_update_dt
						FROM
							DE2TM.SMAL_META_DATA
						WHERE
							dbname = 'DE2TM'
							AND
							table_name = 'SMAL_DWH_DIM_CARDS_HIST')""")

curs.execute("""INSERT INTO DE2TM.SMAL_STG_CLIENTS
					SELECT 
						client_id,
						last_name,
						first_name,
						patronymic,
						date_of_birth,
						passport_num,
						passport_valid_to,
						phone,
						create_dt,
						update_dt		
					FROM 
						BANK.CLIENTS
					WHERE
						COALESCE(update_dt, create_dt) >
						(SELECT
							last_update_dt
						FROM
							DE2TM.SMAL_META_DATA
						WHERE
							dbname = 'DE2TM'
							AND
							table_name = 'SMAL_DWH_DIM_CLIENTS_HIST')""")


#Загрузка данных в таблицы удалений в БД

curs.execute("""INSERT INTO DE2TM.SMAL_STG_ACCOUNTS_DEL
                    SELECT
                        account
                    FROM
                        bank.accounts""")

curs.execute("""INSERT INTO DE2TM.SMAL_STG_CARDS_DEL
                    SELECT
                        card_num
                    FROM
                        bank.cards""")
    
curs.execute("""INSERT INTO DE2TM.SMAL_STG_CLIENTS_DEL
                SELECT
                    client_id
                FROM
                    bank.clients""")

#Загрузка данных в таблицы измерений в БД

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_ACCOUNTS_HIST 
                (account_num, valid_to, client, effective_from, effective_to, deleted_flg)
                    SELECT
                        account,
                        valid_to,
                        client,
                        create_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM 
                        DE2TM.SMAL_STG_ACCOUNTS
                    WHERE
                        update_dt IS NULL""")

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_ACCOUNTS_HIST
                (account_num, valid_to, client, effective_from, effective_to, deleted_flg)
                    SELECT
                        account,
                        valid_to,
                        client,
                        update_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM 
                        DE2TM.SMAL_STG_ACCOUNTS
                    WHERE
                        update_dt IS NOT NULL""")

curs.execute("""MERGE INTO DE2TM.SMAL_DWH_DIM_ACCOUNTS_HIST sddah
                USING DE2TM.SMAL_STG_ACCOUNTS ssa
                ON (sddah.account_num = ssa.account 
				AND sddah.effective_from < COALESCE(ssa.update_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD')))
                WHEN MATCHED THEN UPDATE SET 
                    sddah.effective_to = ssa.update_dt - 1
                        WHERE sddah.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_CARDS_HIST
                (card_num, account_num, effective_from, effective_to, deleted_flg)
                    SELECT
                        card_num,
                        account,
                        create_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                        DE2TM.SMAL_STG_CARDS
                    FROM 
                    WHERE
                        update_dt IS NULL""")

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_CARDS_HIST
                (card_num, account_num, effective_from, effective_to, deleted_flg)
                    SELECT
                        card_num,
                        account,
                        update_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM 
                        DE2TM.SMAL_STG_CARDS
                    WHERE
                        update_dt IS NOT NULL""")

curs.execute("""MERGE INTO DE2TM.SMAL_DWH_DIM_CARDS_HIST sddch
                USING DE2TM.SMAL_STG_CARDS ssc
                ON (sddch.card_num = ssc.card_num 
				AND sddch.effective_from < COALESCE(ssc.update_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD')))
                WHEN MATCHED THEN UPDATE SET 
                    sddch.effective_to = ssc.update_dt - 1
                        WHERE sddch.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_CLIENTS_HIST
                (client_id, last_name, first_name, patronymic, date_of_birth, 
                passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                    SELECT
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        create_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM 
                        DE2TM.SMAL_STG_CLIENTS
                    WHERE
                        update_dt IS NULL""")

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_CLIENTS_HIST
                (client_id, last_name, first_name, patronymic, date_of_birth, 
                passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                    SELECT
                        client_id,
                        last_name,
                        first_name,
                        patronymic,
                        date_of_birth,
                        passport_num,
                        passport_valid_to,
                        phone,
                        update_dt,
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM 
                        DE2TM.SMAL_STG_CLIENTS
                    WHERE
                        update_dt IS NOT NULL""")

curs.execute("""MERGE INTO DE2TM.SMAL_DWH_DIM_CLIENTS_HIST dwh_cl
                USING DE2TM.SMAL_STG_CLIENTS stg_cl
                ON (dwh_cl.client_id = stg_cl.client_id 
				AND dwh_cl.effective_from < COALESCE(stg_cl.update_dt, TO_DATE('1899-01-01', 'YYYY-MM-DD')))
                WHEN MATCHED THEN UPDATE SET 
                    dwh_cl.effective_to = stg_cl.update_dt - 1
                        WHERE dwh_cl.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""")

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_TERMINALS_HIST
                (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)	
                    SELECT 
                        term.terminal_id,
                        term.terminal_type,
                        term.terminal_city,
                        term.terminal_address,
                        TO_DATE(?, 'YYYY-MM-DD'),
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM
                        DE2TM.SMAL_STG_TERMINALS term
                    LEFT JOIN
                        DE2TM.SMAL_DWH_DIM_TERMINALS_HIST dwh_term
                        ON
                        term.terminal_id = dwh_term.terminal_id
                    WHERE
                        dwh_term.terminal_id IS NULL""", (terminals_dt_next_str,))

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_TERMINALS_HIST
                (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)	
                    SELECT 
                        term.terminal_id,
                        term.terminal_type,
                        term.terminal_city,
                        term.terminal_address,
                        TO_DATE(?, 'YYYY-MM-DD'),
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'N'
                    FROM
                        DE2TM.SMAL_STG_TERMINALS term
                    LEFT JOIN
                        DE2TM.SMAL_DWH_DIM_TERMINALS_HIST dwh_term
                        ON
                        term.terminal_id = dwh_term.terminal_id
                    WHERE
                        term.terminal_city <> dwh_term.terminal_city 
						OR term.terminal_address <> dwh_term.terminal_address
                        AND dwh_term.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""", (terminals_dt_next_str,))
    
curs.execute("""MERGE INTO DE2TM.SMAL_DWH_DIM_TERMINALS_HIST dwh_term
                USING DE2TM.SMAL_STG_TERMINALS term
                ON (dwh_term.terminal_id = term.terminal_id 
					AND dwh_term.effective_from < TO_DATE(?, 'YYYY-MM-DD'))
                WHEN MATCHED THEN UPDATE SET 
                    dwh_term.effective_to = TO_DATE(?, 'YYYY-MM-DD')
                        WHERE dwh_term.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')""", (terminals_dt_next_str, terminals_dt_str,))


curs.execute("""INSERT INTO DE2TM.SMAL_DWH_FACT_PASSPORT_BLACKLIST (entry_dt, passport_num)
                    SELECT 
                        entry_dt,
						passport_num
                    FROM 
                        DE2TM.SMAL_STG_PASSPORT_BLACKLIST""")
    
 
curs.execute("""INSERT INTO SMAL_DWH_FACT_TRANSACTIONS
                (trans_id, trans_date, card_num, oper_type, amt, oper_result, terminal)
                    SELECT 
                        CAST(transaction_id AS INT),
                        transaction_date,
                        card_num,
                        oper_type,
                        CAST(amount AS DECIMAL(10,2)),
                        oper_result,
                        terminal
                    FROM 
                        SMAL_STG_TRANSACTIONS""")


#Удаление данных из таблиц измерений в БД

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_ACCOUNTS_HIST
                (account_num, valid_to, client, effective_from, effective_to, deleted_flg)
                    SELECT
                        dwh_acс.account_num,
                        dwh_acс.valid_to,
                        dwh_acс.client,
                        TO_DATE(?, 'YYYY-MM-DD'),
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'Y'
                    FROM 
                        DE2TM.SMAL_DWH_DIM_ACCOUNTS_HIST dwh_acс
                    LEFT JOIN
                        DE2TM.SMAL_STG_ACCOUNTS_DEL ssad
                        ON
                        dwh_acс.account_num = ssad.account
                    WHERE
                        ssad.account IS NULL
                        AND
                        dwh_acс.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                        AND
                        dwh_acс.deleted_flg = 'N' """, (terminals_dt_str,))

curs.execute("""UPDATE DE2TM.SMAL_DWH_DIM_ACCOUNTS_HIST
                SET
                    effective_to = TO_DATE(?, 'YYYY-MM-DD')
                WHERE
                    account_num IN
                        (
                            SELECT
                                dwh_acс.account_num
                            FROM
                                DE2TM.SMAL_DWH_DIM_ACCOUNTS_HIST dwh_acс
                            LEFT JOIN
                                DE2TM.SMAL_STG_ACCOUNTS_DEL ssad
                                ON
                                dwh_acс.account_num = ssad.account
                            WHERE
                                ssad.account IS NULL
                                AND
                                dwh_acс.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                                AND
                                dwh_acс.deleted_flg = 'N'
                        )
                    AND
                    effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                    AND
                    deleted_flg = 'N' """, (terminals_dt_next_str,))


curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_CARDS_HIST
                (card_num, account_num, effective_from, effective_to, deleted_flg)
                    SELECT
                        dwh_cards.card_num,
                        dwh_cards.account_num,
                        TO_DATE(?, 'YYYY-MM-DD'),
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'Y'
                    FROM 
                        DE2TM.SMAL_DWH_DIM_CARDS_HIST dwh_cards
                    LEFT JOIN
                        DE2TM.SMAL_STG_CARDS_DEL stg_cards_d
                        ON
                        dwh_cards.card_num = stg_cards_d.card_num
                    WHERE
                        stg_cards_d.card_num IS NULL
                        AND
                        dwh_cards.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                        AND
                        dwh_cards.deleted_flg = 'N' """, (terminals_dt_str,))
    
curs.execute("""UPDATE DE2TM.SMAL_DWH_DIM_CARDS_HIST
                SET
                    effective_to = TO_DATE(?, 'YYYY-MM-DD')
                WHERE
                    card_num IN
                        (
                            SELECT
                                dwh_cards.card_num
                            FROM
                                DE2TM.SMAL_DWH_DIM_CARDS_HIST dwh_cards
                            LEFT JOIN
                                DE2TM.SMAL_STG_CARDS_DEL stg_cards_d
                                ON
                                dwh_cards.card_num = stg_cards_d.card_num
                            WHERE
                                stg_cards_d.card_num IS NULL
                                AND
                                dwh_cards.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                                AND
                                dwh_cards.deleted_flg = 'N'
                        )
                    AND
                    effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                    AND
                    deleted_flg = 'N' """, (terminals_dt_next_str,))

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_CLIENTS_HIST
                (client_id, last_name, first_name, patronymic, date_of_birth, 
                passport_num, passport_valid_to, phone, effective_from, effective_to, deleted_flg)
                    SELECT
                        dwh_cl.client_id,
                        dwh_cl.last_name,
                        dwh_cl.first_name,
                        dwh_cl.patronymic,
                        dwh_cl.date_of_birth,
                        dwh_cl.passport_num,
                        dwh_cl.passport_valid_to,
                        dwh_cl.phone,
                        TO_DATE(?, 'YYYY-MM-DD'),
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'Y'
                    FROM 
                        DE2TM.SMAL_DWH_DIM_CLIENTS_HIST dwh_cl
                    LEFT JOIN
                        DE2TM.SMAL_STG_CLIENTS_DEL stg_cl_d
                        ON
                        dwh_cl.client_id = stg_cl_d.client_id
                    WHERE
                        stg_cl_d.client_id IS NULL
                        AND
                        dwh_cl.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                        AND
                        dwh_cl.deleted_flg = 'N' """, (terminals_dt_str,))
    
curs.execute("""UPDATE DE2TM.SMAL_DWH_DIM_CLIENTS_HIST
                SET
                    effective_to = TO_DATE(?, 'YYYY-MM-DD')
                WHERE
                    client_id IN
                        (
                            SELECT
                                dwh_cl.client_id
                            FROM
                                DE2TM.SMAL_DWH_DIM_CLIENTS_HIST dwh_cl
                            LEFT JOIN
                                DE2TM.SMAL_STG_CLIENTS_DEL stg_cl_d
                                ON
                                dwh_cl.client_id = stg_cl_d.client_id
                            WHERE
                                stg_cl_d.client_id IS NULL
                                AND
                                dwh_cl.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                                AND
                                dwh_cl.deleted_flg = 'N'
                        )
                    AND
                    effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                    AND
                    deleted_flg = 'N' """, (terminals_dt_next_str,))

curs.execute("""INSERT INTO DE2TM.SMAL_DWH_DIM_TERMINALS_HIST 
                (terminal_id, terminal_type, terminal_city, terminal_address, effective_from, effective_to, deleted_flg)
                    SELECT
                        dwh_term.terminal_id,
                        dwh_term.terminal_type,
                        dwh_term.terminal_city,
                        dwh_term.terminal_address,
                        TO_DATE(?, 'YYYY-MM-DD'),
                        TO_DATE('5999-12-31', 'YYYY-MM-DD'),
                        'Y'
                    FROM 
                        DE2TM.SMAL_DWH_DIM_TERMINALS_HIST dwh_term
                    LEFT JOIN
                        DE2TM.SMAL_STG_TERMINALS stg_term
                        ON
                        dwh_term.terminal_id = stg_term.terminal_id
                    WHERE
                        stg_term.terminal_id IS NULL
                        AND
                        dwh_term.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                        AND
                        dwh_term.deleted_flg = 'N' """, (terminals_dt_str,))
    
	
curs.execute("""UPDATE DE2TM.SMAL_DWH_DIM_TERMINALS_HIST 
                SET
                    effective_to = TO_DATE(?, 'YYYY-MM-DD')
                WHERE
                    terminal_id IN
                        (
                            SELECT
                                dwh_term.terminal_id
                            FROM
                                DE2TM.SMAL_DWH_DIM_TERMINALS_HIST dwh_term
                            LEFT JOIN
                                DE2TM.SMAL_STG_TERMINALS stg_term
                                ON
                                dwh_term.terminal_id = stg_term.terminal_id
                            WHERE
                                stg_term.terminal_id IS NULL
                                AND
                                dwh_term.effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                                AND
                                dwh_term.deleted_flg = 'N'
                        )
                    AND
                    effective_to = TO_DATE('5999-12-31', 'YYYY-MM-DD')
                    AND
                    deleted_flg = 'N' """, (terminals_dt_next_str,))

#Обновление таблиц метаданных

curs.execute("""UPDATE DE2TM.SMAL_META_DATA 
                SET last_update_dt = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM DE2TM.SMAL_STG_ACCOUNTS)
                WHERE dbname = 'DE2TM' AND table_name = 'SMAL_DWH_DIM_ACCOUNTS_HIST'
                    AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM DE2TM.SMAL_STG_ACCOUNTS) IS NOT NULL""")
    
curs.execute("""UPDATE DE2TM.SMAL_META_DATA 
                SET last_update_dt = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM DE2TM.SMAL_STG_CARDS)
                WHERE dbname = 'DE2TM' AND table_name = 'SMAL_DWH_DIM_CARDS_HIST'
                    AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM DE2TM.SMAL_STG_CARDS) IS NOT NULL""")

curs.execute("""UPDATE DE2TM.SMAL_META_DATA 
                SET last_update_dt = (SELECT MAX(COALESCE(update_dt, create_dt)) FROM DE2TM.SMAL_STG_CLIENTS)
                WHERE dbname = 'DE2TM' AND table_name = 'SMAL_DWH_DIM_CLIENTS_HIST'
                    AND (SELECT MAX(COALESCE(update_dt, create_dt)) FROM DE2TM.SMAL_STG_CLIENTS) IS NOT NULL""")
    
curs.execute("""UPDATE DE2TM.SMAL_META_DATA 
                SET last_update_dt = TO_DATE(?, 'YYYY-MM-DD')
                WHERE dbname = 'DE2TM' AND table_name = 'SMAL_DWH_DIM_TERMINALS_HIST' """, (terminals_dt_next_str,))

curs.execute("""UPDATE DE2TM.SMAL_META_DATA 
                SET last_update_dt = (SELECT MAX(entry_dt) FROM DE2TM.SMAL_STG_PASSPORT_BLACKLIST)
                WHERE dbname = 'DE2TM' AND table_name = 'SMAL_DWH_FACT_PASSPORT_BLACKLIST' """)
    
curs.execute("""UPDATE DE2TM.SMAL_META_DATA 
                SET last_update_dt = (SELECT MAX(transaction_date) FROM DE2TM.SMAL_STG_TRANSACTIONS)
                WHERE dbname = 'DE2TM' AND table_name = 'SMAL_DWH_FACT_TRANSACTIONS' """)

#Формирование отчета о мошеннических операциях

curs.execute("""INSERT INTO SMAL_REP_FRAUD (event_dt, passport, fio, phone, event_type, report_dt)	
                    SELECT
                        event_dt,
                        passport,
                        fio,
                        phone,
                        event_type,
                        report_dt
                    FROM
                        (SELECT
                            dwh_trans.trans_date AS event_dt,
                            dwh_cl.passport_num AS passport,
                            dwh_cl.last_name || ' ' || dwh_cl.first_name || ' ' || dwh_cl.patronymic AS fio,
                            dwh_cl.phone AS phone,
                            CASE
                                WHEN
                                    dwh_cl.passport_valid_to IS NOT NULL
                                    AND
                                    dwh_cl.passport_num IN (SELECT passport_num FROM SMAL_DWH_FACT_PASSPORT_BLACKLIST)
                                    OR
                                    dwh_cl.passport_valid_to < TO_DATE(?, 'YYYY-MM-DD')
                                THEN 'Совершение операции при просроченном или заблокированном паспорте'
                                WHEN
                                    dwh_ac.valid_to < TO_DATE(?, 'YYYY-MM-DD')
                                THEN 'Совершение операции при недействующем договоре'
                                WHEN
                                    dwh_trans.trans_date IN 
                                        (SELECT
                                            MAX(dwh_trans_a.trans_date)
                                        FROM
                                           SMAL_DWH_FACT_TRANSACTIONS dwh_trans_a
                                        INNER JOIN
                                           SMAL_DWH_DIM_TERMINALS_HIST dwh_term_a
                                            ON
                                            dwh_trans_a.terminal = dwh_term_a.terminal_id
                                        INNER JOIN
                                           SMAL_DWH_FACT_TRANSACTIONS dwh_trans_b
                                            ON
                                            dwh_trans_a.card_num = dwh_trans_b.card_num
                                            AND
                                            dwh_trans_a.trans_date < dwh_trans_b.trans_date
                                        INNER JOIN
                                           SMAL_DWH_DIM_TERMINALS_HIST dwh_term_b
                                            ON
                                            dwh_trans_b.terminal = dwh_term_b.terminal_id
                                        WHERE
                                            dwh_trans_a.card_num = dwh_trans_b.card_num
                                            AND
                                            dwh_term_a.terminal_city <> dwh_term_b.terminal_city
                                            AND
                                            (dwh_trans_b.trans_date - dwh_trans_a.trans_date) < INTERVAL '1' HOUR)
                                THEN 'Совершение операции в разных городах в течение одного часа'
                                WHEN
                                    dwh_trans.trans_date IN 
                                        (SELECT
                                            MAX(dwh_trans_a.trans_date)
                                        FROM
                                           SMAL_DWH_FACT_TRANSACTIONS dwh_trans_a
                                        INNER JOIN
                                           SMAL_DWH_FACT_TRANSACTIONS dwh_trans_b
                                            ON
                                            dwh_trans_a.card_num = dwh_trans_b.card_num
                                            AND
                                            dwh_trans_a.trans_date < dwh_trans_b.trans_date
                                        INNER JOIN
                                           SMAL_DWH_FACT_TRANSACTIONS dwh_trans_c
                                            ON
                                            dwh_trans_b.card_num = dwh_trans_c.card_num
                                            AND
                                            dwh_trans_b.trans_date < dwh_trans_c.trans_date
                                        WHERE
                                            dwh_trans_a.oper_result = 'REJECT' AND dwh_trans_b.oper_result = 'REJECT' AND dwh_trans_c.oper_result = 'SUCCESS'
                                            AND
                                            dwh_trans_a.amt > dwh_trans_b.amt AND dwh_trans_b.amt > dwh_trans_c.amt
                                            AND
                                            (dwh_trans_c.trans_date - dwh_trans_a.trans_date) < INTERVAL '20' MINUTE)
                                THEN 'Попытка подбора суммы'
                                ELSE 'Корректная транзакция'
                            END AS event_type,
                            TO_DATE(?, 'YYYY-MM-DD') AS report_dt
                        FROM
                            SMAL_DWH_FACT_TRANSACTIONS dwh_trans
                        LEFT JOIN
                            SMAL_DWH_DIM_CARDS_HIST dwh_cards
                            ON
                            dwh_trans.card_num = RTRIM(dwh_cards.card_num)
                        LEFT JOIN
                            SMAL_DWH_DIM_ACCOUNTS_HIST dwh_ac
                            ON
                            dwh_cards.account_num = dwh_ac.account_num
                        LEFT JOIN
                            SMAL_DWH_DIM_CLIENTS_HIST dwh_cl
                            ON
                            dwh_ac.client = dwh_cl.client_id
                        WHERE
                            dwh_trans.trans_date > TO_DATE(?, 'YYYY-MM-DD'))
                    WHERE
                        event_type <> 'Корректная транзакция' 
                    ORDER BY
                        event_dt""", (terminals_dt_str, terminals_dt_str, terminals_dt_next_str, terminals_dt_str, ))


#Коммит всех изменений в таблицах

conn.commit()

#Закрытие соединения с БД

curs.close()
conn.close()