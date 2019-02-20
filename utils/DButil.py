# encoding=utf-8
import sys
import pymysql
import cx_Oracle
import pandas as pd
import random
# from fdfsUtil import fdfs_Util
from DBUtils.PooledDB import PooledDB
import traceback
import logging
import os

path = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename=os.path.join(path, 'log.txt'))
logger = logging.getLogger(__name__)
# 设置系统编码格式为utf-8
# reload(sys)
# sys.setdefaultencoding('utf8')

#与数据库交互的相关函数，包含模型文件插入获取，训练数据获取，日志数据插入
class db_Util:

    # 初始化连接orcale的url
    @staticmethod
    def create_table(connection,jdbctype='1'):
        connection.ping()
        cursor = connection.cursor()
        sql="CREATE TABLE WWWWW(DNAME VARCHAR2(14),LOC VARCHAR2(13))"
        cursor.execute(sql)
        connection.commit()
        cursor.close()

    @staticmethod
    def intial_dsn(ip, port, sid):
        # dsn = cx_Oracle.makedsn(ip, port, sid)
        # dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.11.150)(PORT=1521))(CONNECT_DATA=(SID=orcl)))"
        # dsn="(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=10.0.11.235)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=ipcc)))"
        # dsn="10.0.11.235:1521/ipcc"
        dsn = "(DESCRIPTION=(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.0.11.235 )(PORT = 1521)) )(CONNECT_DATA =(SERVICE_NAME = ipcc)))"
        print(dsn)
        return dsn

    # 初始化连接orcale的连接
    # 数据库用户名 username
    # 数据库密码  userpwd
    # 数据库地址 url
    @staticmethod
    def intial_cursor(username, userpwd,url='',host='',port='',db='',jdbctype='2'):
        if jdbctype=='1':
            connection = cx_Oracle.connect(username, userpwd, url, encoding="UTF-8", nencoding="UTF-8")
        elif jdbctype=='2':
            # connection=pymysql.connect(url=url,user=username, passwd=userpwd, charset='utf8')
            # print( 'username',username)
            # print( 'username', userpwd)
            connection = pymysql.connect(host=host, port=port, user=username, passwd=userpwd, db=db, charset='utf8')
            # connection = pymysql.connect(host='10.0.11.209:3306', user=username, passwd=userpwd,charset='utf8', db='uicsr_admin')
        return connection

    # 初始化连接orcale的连接池
    # 数据库类型 jdbctype '1':oracle | '2':mysql
    # 数据库地址 url
    # 数据库用户名 username
    # 数据库密码  userpwd
	# 数据库连接池最小连接数5，最大连接数20
    @staticmethod
    def intial_pool(username, userpwd,url='',host='',port='',db='',jdbctype='2'):
        if jdbctype=='1':
            pool = PooledDB(cx_Oracle, mincached=5, maxcached=20, maxconnections=200, blocking=False, user=username,
                                   password=userpwd, dsn=url, encoding="UTF-8", nencoding="UTF-8")
        elif jdbctype=='2':
            pool = PooledDB(pymysql, mincached=5, maxcached=20, maxconnections=200, blocking=False, user=username,
                                   password=userpwd, host=host, port=port,db=db, charset='utf8')
        return pool

    # 关闭orcale的连接
    @staticmethod
    def close_cursor(cursor, connection):
        cursor.close()
        connection.close()
        return connection

    # 更新未知问题表中文本内容对应的聚类编码id
    @staticmethod
    def update_cluster_type(connection, cluster_type, unknown_fq_id):
        # print('params_value params_value params_value',params_value)
        connection.ping()
        cursor = connection.cursor()
        updatesql = "UPDATE UICSR_UNKNOWN_FQ set cluster_type= '" + cluster_type + "' where unknown_fq_id='" + unknown_fq_id + "'"
        # print( updatesql)
        cursor.execute(updatesql)
        # print( 'updatesql')
        connection.commit()
        cursor.close()


    # 选出后期积累的不理解样本
    @staticmethod
    def select_unknown(connection):
        connection.ping()
        cursor = connection.cursor()
        selectsql = "SELECT unknown_fq_id,content,cluster_type from UICSR_UNKNOWN_FQ where UNKNOWN_REASON!='0'"
        # #print( selectsql)
        cursor.execute(selectsql)
        result = cursor.fetchall()
        # print( 'unknown', result)
        result = pd.DataFrame(list(result), columns=['unknown_fq_id','context','cluster_type'])
        # result = pd.DataFrame(result, columns={'label', 'context'})
        # print( result.head(5))
        # print( 'unknown', result.head(5))
        cursor.close()
        return result

    @staticmethod
    def select_all(connection, table_name="UICS_MASKQUESTION"):
        connection.ping()
        cursor = connection.cursor()
        selectsql = "SELECT * FROM UICS_MASKQUESTION"
        cursor.execute(selectsql)
        connection.commit()
        result = cursor.fetchall()
        cursor.close()
        return list(result)

    @staticmethod
    def create_result_table(connection, table_name="UICS_RESULT_UNKNOWN_FQ"):
        flag = False
        connection.ping()
        cursor = connection.cursor()
        # delete_sql = "drop table %s" % table_name
        # cursor.execute(delete_sql)
        create_sql = "CREATE TABLE UICS_RESULT_UNKNOWN_FQ (QUESTION_ID VARCHAR2(64), UNKNOWN VARCHAR2(1024))"
        # print(create_sql)
        try:
            cursor.execute(create_sql)
            connection.commit()
            flag = True
        except Exception :
            logger.warning(traceback.format_exc())
            cursor.close()
        cursor.close()
        return flag

    @staticmethod
    def insert_model_file(connection, jdbctype='1'):
        # try:
        # print( (77771))
        connection.ping()
        cursor = connection.cursor()
        question_id = '655'
        unknown_question = '企业的基本户要多久能完成开户'
        param = [question_id, unknown_question]
        # print( 'param',[model_id,file_ver, file_number,file_size])
        if jdbctype == '1':
            updatesql = "INSERT INTO UICS_MASKQUESTION  " \
                        "(QUESTION_ID,MASKQUESTION) " \
                        "values(:1,:2)"
        elif jdbctype == '2':
            updatesql = "INSERT INTO WWWWW  " \
                        "(DNAME,LOC) " \
                        "values(%s,%s)"
        connection.ping()
        cursor = connection.cursor()
        # print( (77774))
        # import datetime
        # starttime = datetime.datetime.now()
        # print(('starttime',starttime))
        cursor.execute(updatesql, param)
        connection.commit()
        cursor.close()

    @staticmethod
    def create_maskquestion_table(connection):
        flag = False
        connection.ping()
        cursor = connection.cursor()
        create_sql = "CREATE TABLE UICS_MASKQUESTION(QUESTION_ID VARCHAR2(64) PRIMARY KEY, MASKQUESTION VARCHAR2(1024))"
        # print(create_sql)
        try:
            cursor.execute(create_sql)
            connection.commit()
            flag = True
        except Exception:
            logger.warning(traceback.format_exc())
            cursor.close()
        cursor.close()
        return flag

    @staticmethod
    def drop_table(connection, table_name=""):
        flag = False
        connection.ping()
        cursor = connection.cursor()
        drop_sql = "DROP TABLE UICS_MASKQUESTION"
        try:
            cursor.execute(drop_sql)
            connection.commit()
            flag = True
        except Exception:
            logger.warning(traceback.format_exc())
            cursor.close()
        cursor.close()
        # print("删表成功!!!")
        return flag

    @staticmethod
    def insert_maskquestion_table(connection, result_list=[]):
        flag = False
        if len(result_list) == 0:
            return True
        connection.ping()
        cursor = connection.cursor()
        try:
            temp_sql = "INSERT INTO UICS_MASKQUESTION (QUESTION_ID, MASKQUESTION)"
            for res in result_res:
                insert_sql = temp_sql + " VALUES (:1, :2)"
                print(insert_sql)
                cursor.execute(insert_sql, res)
                connection.commit()
            flag = True
        except Exception:
            logger.warning("insert into table failed!!!")
            logger.warning(traceback.format_exc())
            cursor.close()
        cursor.close()
        return flag


if __name__ == '__main__':
    jdbctype = '1'
    username = "uicsr_admin_test_33"  # 用户名
    userpwd = "utry1234"  # 密码
    url = "(DESCRIPTION=(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = 10.0.11.235)(PORT = 1521)) )(CONNECT_DATA =(SERVICE_NAME = ipcc)))"  # url

    pool = db_Util.intial_pool(username=username, userpwd=userpwd, url=url,jdbctype=jdbctype)
    connection = pool.connection()
    result_res = [["655", "企业的基本户要多久能完成开户"], ["656", "企业开办基本户需要多少时间"], ["657", "我想了解一下怎么办理信用卡的现金分期"]]
    # print(db_Util.select_all(connection))
    # db_Util.drop_table(connection, table_name="UICS_MARKQUESTION_FQ")
    # print(db_Util.select_all(connection))
    # if db_Util.drop_table(connection, table_name="UICS_RESULT_UNKNOWN_FQ"):
    #     print("删除表成功!!!")
    if db_Util.drop_table(connection, table_name="UICS_MASKQUESTION"):
        print("删除表成功!!!")
    if db_Util.create_maskquestion_table(connection):
        print("创建数据表成功!!!")
    # if db_Util.create_result_table(connection, table_name="UICS_RESULT_UNKNOWN_FQ"):
    #     print("创建数据表成功!!!")
    db_Util.insert_model_file(connection)
    # db_Util.insert_model_file(connection)
    print(db_Util.select_all(connection))
    # if db_Util.drop_table(connection, table_name="UICS_MASKQUESTION"):
    #     print("删除表成功!!!")
    print(db_Util.select_all(connection))
    # if db_Util.insert_maskquestion_table(connection, result_res):
    #     print("插入成功!!!")
    # if db_Util.drop_table(connection, table_name="UICS_MARKQUESTION_FQ"):
    #     print("删除表成功!!!")

    # result = db_Util.select_unknown(connection)
    # print(result)
    # up_ = int(len(result['cluster_type'])/2)
    # print(random.randint(0,up_))
    # result['cluster_type'] = result['context'].apply(lambda x:str(random.randint(0,up_)))
    # print(result)
    # for index in range(len(result['cluster_type'])):
    #     print('unknown_fq_id',result['unknown_fq_id'][index],'context',result['context'][index],'cluster_type',result['cluster_type'][index])
    #     db_Util.update_cluster_type(connection, result['cluster_type'][index], result['unknown_fq_id'][index])