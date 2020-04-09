##########################################################
#
#	IDENTIFICAÇÃO DA ULTIMA ACÇÃO COM ENTIDADE X
#
#	Qual a data e tabela da ultima acção/interacção
#	que a entidade X teve.
#
#	Objectivo final: eliminação de entidades obsoletas
#
##########################################################



import os

import cx_Oracle
import mysql.connector

import datetime

ORACLE_CONN_INFO = {
    'host': '127.0.0.1',
    'port': 1521,
    'user': 'YYY',
    'password': 'YYY',
    'service': 'orcl.168.1.77'				#'orcl.168.1.77'
}

MYSQL_CONN_INFO = {
    'host': '127.0.0.1',
    'user': 'root',
    'password': '',
    'database': 'XXX'
}

queries = []





		
queries.append({'table':'amostras_fiscalizacoes', 'column':'DT_COLHEITA', 'query':'''
				SELECT 
				--	MAX(DT_COLHEITA)
					DT_COLHEITA,
					FISC_ENT_ID_ALVO
				FROM 
					amostras_fiscalizacoes 
				WHERE 
					FISC_ENT_ID_ALVO = :id_entidade
				AND
					rownum < 100
                order by 
					DT_COLHEITA DESC'''})


queries.append({'table':'apreensoes', 'column':'DATA_APR', 'query':'''
				SELECT 
					-- MAX(DATA_APR) 
					DATA_APR,
					entidade_id_entidade
				FROM 
					apreensoes 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DATA_APR DESC'''})

queries.append({'table':'apreensoes', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					-- MAX(DT_SISTEMA)
					DT_SISTEMA,
					entidade_id_entidade
				FROM 
					apreensoes 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_SISTEMA DESC'''})

queries.append({'table':'campo_ferias', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					-- MAX(DT_SISTEMA)
					DT_SISTEMA,
					id_entidade_promotora
				FROM 
					campo_ferias 
				WHERE 
					id_entidade_promotora = :id_entidade
                order by 
					DT_SISTEMA DESC'''})

queries.append({'table':'campo_ferias', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					-- MAX(DT_SISTEMA)
					DT_SISTEMA,
					id_entidade_organizadora
				FROM 
					campo_ferias 
				WHERE 
					id_entidade_organizadora = :id_entidade
				order by 
					DT_SISTEMA DESC'''})


				
queries.append({'table':'correspondencias', 'column':'DT_LOCK', 'query':'''
				SELECT 
					DT_LOCK,
					entidade_id_entidade
				FROM 
					correspondencias 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_LOCK DESC'''})
	
	

queries.append({'table':'correspondencias', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					-- MAX(DT_SISTEMA)
					DT_SISTEMA,
					entidade_id_entidade
				FROM 
					correspondencias 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_SISTEMA DESC'''})
					
queries.append({'table':'correspondencias', 'column':'DT_REGISTO', 'query':'''
				SELECT 
					-- MAX(DT_REGISTO)
					DT_REGISTO,
					entidade_id_entidade
				FROM 
					correspondencias 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_REGISTO DESC'''})
					
queries.append({'table':'correspondencias', 'column':'DT_EMISSAO_ORIGEM', 'query':'''
				SELECT 
					-- MAX(DT_EMISSAO_ORIGEM)
					DT_EMISSAO_ORIGEM,
					entidade_id_entidade
				FROM 
					correspondencias 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_EMISSAO_ORIGEM DESC'''})
					
queries.append({'table':'correspondencias', 'column':'DT_INICIAL', 'query':'''
				SELECT 
					-- MAX(DT_INICIAL)
					DT_INICIAL,
					entidade_id_entidade
				FROM 
					correspondencias 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_INICIAL DESC'''})

queries.append({'table':'correspondencias', 'column':'DT_SITUACAO', 'query':'''
				SELECT 
					-- MAX(DT_SITUACAO)
					DT_SITUACAO,
					entidade_id_entidade
				FROM 
					correspondencias 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_SITUACAO DESC'''})



queries.append({'table':'correspondencias', 'column':'DT_LAST_LRE_HIST_CHECK', 'query':'''
				SELECT 
					-- MAX(DT_LAST_LRE_HIST_CHECK)
					DT_LAST_LRE_HIST_CHECK,
					entidade_id_entidade
				FROM 
					correspondencias 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_LAST_LRE_HIST_CHECK DESC'''})	
					

queries.append({'table':'decisoes_ent', 'column':'DT_DECISAO', 'query':'''
				SELECT 
					-- MAX(DT_DECISAO)
					DT_DECISAO,
					ent_decisora_id
				FROM 
					decisoes_ent 
				WHERE 
					ent_decisora_id = :id_entidade
                order by 
					DT_DECISAO DESC'''})

queries.append({'table':'declaracoes_de_transaccoes', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					-- MAX(DT_SISTEMA)
					DT_SISTEMA,
					entidade_id_entidade
				FROM 
					declaracoes_de_transaccoes 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_SISTEMA DESC'''})
					
queries.append({'table':'entidade', 'column':'DT_INICIAL', 'query':'''
				SELECT 
					DT_INICIAL,
					ID_ENTIDADE
				FROM 
					ENTIDADE 
				WHERE 
					ID_ENTIDADE = :id_entidade 
				order by 
					DT_INICIAL DESC'''})
					
queries.append({'table':'entidade', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					DT_SISTEMA,
					ID_ENTIDADE
				FROM 
					ENTIDADE 
				WHERE 
					ID_ENTIDADE = :id_entidade 
				order by 
					DT_SISTEMA DESC'''})
					
queries.append({'table':'entidade', 'column':'DT_VALIDACAO', 'query':'''
				SELECT 
					DT_VALIDACAO,
					ID_ENTIDADE
				FROM 
					ENTIDADE 
				WHERE 
					ID_ENTIDADE = :id_entidade 
				order by 
					DT_VALIDACAO DESC'''})






queries.append({'table':'entidade_processo', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					-- MAX(DT_SISTEMA)
					DT_SISTEMA,
					entidade_id_entidade
				FROM 
					entidade_processo 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_SISTEMA DESC'''})


queries.append({'table':'fisc_entidade', 'column':'DT_LOCK', 'query':'''
				SELECT 
					DT_LOCK,
					entidade_id_entidade
				FROM 
					fisc_entidade 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_LOCK DESC'''})


queries.append({'table':'fisc_entidade', 'column':'DT_AVERIG', 'query':'''
				SELECT 
					-- MAX(DT_AVERIG)
					DT_AVERIG,
					entidade_id_entidade
				FROM 
					fisc_entidade 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_AVERIG DESC'''})


queries.append({'table':'fisc_entidade', 'column':'DT_INICIAL', 'query':'''
				SELECT 
					-- MAX(DT_INICIAL)
					DT_INICIAL,
					entidade_id_entidade
				FROM 
					fisc_entidade 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_INICIAL DESC'''})
	

queries.append({'table':'fisc_entidade', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					-- MAX(DT_SISTEMA)
					DT_SISTEMA,
					entidade_id_entidade
				FROM 
					fisc_entidade 
				WHERE 
					entidade_id_entidade = :id_entidade
                order by 
					DT_SISTEMA DESC'''})

	
queries.append({'table':'processos', 'column':'DT_SISTEMA', 'query':'''
				SELECT 
					-- MAX(DT_SISTEMA)
					DT_SISTEMA,
					ent_destino_id
				FROM 
					processos 
				WHERE 
					ent_destino_id = :id_entidade
                order by 
					DT_SISTEMA DESC'''})


queries.append({'table':'reclamacao', 'column':'RECLAM_DATE', 'query':'''
				SELECT 
					-- MAX(RECLAM_DATE)
					RECLAM_DATE,
					reclamante_id
				FROM 
					reclamacao 
				WHERE 
					reclamante_id = :id_entidade
                order by 
					RECLAM_DATE DESC'''})


queries.append({'table':'reclamacao', 'column':'RECLAM_DATE', 'query':'''
				SELECT 
					-- MAX(RECLAM_DATE)
					RECLAM_DATE,
					entidade_visada_id
				FROM 
					reclamacao 
				WHERE 
					entidade_visada_id = :id_entidade
                order by 
					RECLAM_DATE DESC'''})

                 
cur = None
conn = None
mycursor = None
mydb = None

def OracleConnect():
	cur = None
	conn = None
	try:
		conn_str = '{user}/{password}@{host}:{port}/{service}'.format(**ORACLE_CONN_INFO)
		conn = cx_Oracle.connect(conn_str)
		v = None
		
		if conn:
			v = conn.version
			cur = conn.cursor()
			print ("## CONNECTED TO ORACLE DB ##")
			print ("Oracle version: " + v)
		else:
			print ("## UNABLE TO CONNECTO TO ORACLE ##")
	except Exception as e:
		print ("ERROR - ## UNABLE TO CONNECTO TO ORACLE ##", e)
	
	return cur, conn

def MySQLConnect():
	mycursor = None
	mydb = None
	try:
		mydb = mysql.connector.connect(**MYSQL_CONN_INFO)
		
		if mydb:
			print ("## CONNECTED TO MYSQL DB ##")
			mycursor = mydb.cursor()
		else:
			print ("## UNABLE TO CONNECTO TO MYSQL ##")
	except Exception as e:
		print ("ERROR - ## UNABLE TO CONNECTO TO MYSQL ##", e)
	
	return mycursor, mydb


# get the most recent date as [datetime object, table, column]
def getLastDate(id_entidade):
	data = [None, None, None, None, None, None, 'F']
#	print ("\n\n\n\n\n\n\n_")
	for query in  queries:
		cur.execute(query['query'], {"id_entidade": id_entidade})
		results =  cur.fetchone()		
		
		if results:
			if results[0]:
				row = datetime.datetime(results[0].year,results[0].month, results[0].day)
				if not data[0] or row > data[0]:
					data[0] = row
					data[1] = query['table']
					data[2] = query['column']
				if not data[3] or row < data[3]:
					data[3] = row
					data[4] = query['table']
					data[5] = query['column']	
			# entidade mencionada noutra tabela alem da entidade (indep. of date)		
			if results[1]:
				if query['table'] != 'entidade':
					data[6] = 'T'

#		print (query['table'])
#		print (results)
#		print (data[6])
#		print ("-------------------------------------------------")
		
#	if data[6] == 'T':
#		os.system("pause")
	
	
	return data



print ("Attempting to connect to Oracle database ...")
cur,conn = OracleConnect()
if not cur:
	print ("**************************************")
	print ("CHECK YOUR ORACLE DATABASE CONNECTION ")
	print ("AND RESTART THE APLICATION ... ")
	print ("**************************************")
	exit()
else:
	print ("ORACLE: ALLES GUT")

print ("\n\nAttempting to connect to MySql database ...")
mycursor,mydb = MySQLConnect()
if not mycursor:
	print ("*************************************")
	print ("CHECK YOUR MYSQL DATABASE CONNECTION!")
	print ("AND RESTART THE APLICATION ... ")
	print ("*************************************")
	exit()
else:
	print ("MYSQL: ALLES GUT")	


mycursor.execute("select id_entidade from entidade_ultimo_update order by id_entidade desc limit 1")
last_id = mycursor.fetchone()
if not last_id:
	last_id = 0
else:
	last_id = last_id[0]

print (last_id)


print ("\n\nfetching all entities")
#cur.execute('select id_entidade from entidade where id_entidade > ' + str(last_id) + ' and rownum < 100000 ORDER BY id_entidade')
cur.execute('select id_entidade from entidade where id_entidade > ' + str(last_id) + ' and rownum < 1000000 ORDER BY id_entidade')
#cur.execute('select id_entidade from entidade where id_entidade =5853086 or id_entidade = 2998627')
entities =  cur.fetchall()
counter = 0
print ("starting ... ")
for ent in entities:
	counter += 1
	if counter % 1000 == 0:
		print (counter, ent[0])
		mydb.commit()
	if ent[0] >= 0:
#		print ("ENT >>>> ", ent[0])
		date = getLastDate(ent[0])
#		print (date)
		mycursor.execute('''
				INSERT INTO 
					entidade_ultimo_update 
						(id_entidade, 
						data_recente, 
						tabela_recente, 
						coluna_recente, 
						data_primeira, 
						tabela_primeira, 
						coluna_primeira, 
						outra) 
					VALUES 
						(%s, %s, %s, %s, %s, %s, %s, %s)
				''',
               (ent[0], date[0], date[1], date[2], date[3], date[4], date[5], date[6]))
		
mydb.commit()
		
		
