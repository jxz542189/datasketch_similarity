from utils.util import get_question_list
from datasketch import MinHash, MinHashLSH
import os
import pandas as pd
from utils.DButil import db_Util
import traceback
import logging
import pkuseg


path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'result')
log_path = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'log.txt')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename=log_path)
logger = logging.getLogger(__name__)
stop_words = set(['的', '我', '你', '我们', '他们', '你们', '是'])
seg = pkuseg.pkuseg()


def similarity(question_list,
               username="uicsr_admin_test_33",
               userpwd="utry1234",
               ip="10.0.11.235",
               jdbctype="1"):
    flag = False
    lsh = MinHashLSH(threshold=0.3, num_perm=1024)
    maskquestion_flag, maskquestion_dict, connection = get_maskquestion_dict(username=username, userpwd=userpwd, ip=ip, jdbctype=jdbctype)
    new_mask_question_dict = {}
    result_list = []
    # print(maskquestion_dict)
    if maskquestion_flag:
        new_mask_question_dict, result_list = get_result(lsh, question_list, maskquestion_dict, connection)
        if insert_result_table(connection, result_list):
            logger.info("insert result table success!!!")
            flag = True
    return flag, new_mask_question_dict, result_list


def get_maskquestion_dict(username="uicsr_admin_test_33",
                          userpwd="utry1234",
                          ip="10.0.11.235",
                          jdbctype="1"):
    flag = False
    url = "(DESCRIPTION=(ADDRESS_LIST =(ADDRESS = (PROTOCOL = TCP)(HOST = %s)(PORT = 1521)) )(CONNECT_DATA =(SERVICE_NAME = ipcc)))" % ip
    try:
        pool = db_Util.intial_pool(username=username, userpwd=userpwd, url=url, jdbctype=jdbctype)
        connection = pool.connection()
    except Exception:
        logger.warning("init db pool failed!!!")
        logger.warning(traceback.format_exc())
        return flag, None, None
    result = []
    try:
        result = db_Util.select_all(connection)
    except Exception:
        logger.warning("select table failed!!!")
        logger.warning(traceback.format_exc())
        return flag, None, None
    maskquestion_dict = {}
    for res in result:
        maskquestion_dict[res[0]] = res[1]
    flag = True
    return flag, maskquestion_dict, connection



def get_result(lsh, question_list, maskquestion_dict, connection):
    """
    :param question_list: 需要处理的未知问题
    :param maskquestion_dict: 已知的问题，最开始没有已知问题，随着时间推移不断加入已知问题，
    maskquestion_dict是一个字典类型，键为问题id，类型为字符串，值为问题
    :return:
    """
    current_max_question_id = 0
    for key in maskquestion_dict.keys():
        value = maskquestion_dict[key]
        mask_question = set(seg.cut(value))
        intersection_set = mask_question & stop_words
        mask_question = mask_question - intersection_set
        m = MinHash(num_perm=1024)
        for d in mask_question:
            m.update(d.encode("utf8"))
        lsh.insert(str(key), m)
        current_max_question_id = max(int(key), current_max_question_id)
    new_mask_question_dict = {}
    result_list = []
    for question in question_list:
        question_set = set(seg.cut(question))
        intersection_set = question_set & stop_words
        question_set = question_set - intersection_set
        m = MinHash(num_perm=1024)
        for d in question_set:
            m.update(d.encode("utf8"))
        result = lsh.query(m)
        if len(result) == 0:
            current_max_question_id += 1
            new_mask_question_dict[str(current_max_question_id)] = question
            result_list.append([str(current_max_question_id), question])
            try:
                connection.ping()
                cursor = connection.cursor()
                insert_sql = "INSERT INTO UICS_MASKQUESTION  " \
                            "(QUESTION_ID,MASKQUESTION) " \
                            "values(:1,:2)"
                cursor.execute(insert_sql, [str(current_max_question_id), question])
                connection.commit()
            except Exception:
                logger.warning("insert into UICS_MASKQUESTION failed!!!")
                logger.warning(traceback.format_exc())
                return {}, []
                # cursor.close()
            cursor.close()

            lsh.insert(str(current_max_question_id), m)
        elif len(result) == 1:
            result_list.append([str(result[0]), question])
        else:
            select_max_similarity_question_id = ''
            jaccard_score = 0
            for key in result:
                value = maskquestion_dict[key] if key in maskquestion_dict else new_mask_question_dict[key]
                value_set = set(seg.cut(value))
                intersection_set = value_set & stop_words
                value_set = value_set - intersection_set
                temp_m = MinHash(num_perm=1024)
                for d in value_set:
                    temp_m.update(d.encode("utf8"))
                temp_score = m.jaccard(temp_m)
                if temp_score > jaccard_score:
                    jaccard_score = temp_score
                    select_max_similarity_question_id = key
            result_list.append([select_max_similarity_question_id, question])
    return new_mask_question_dict, result_list


def write_csv(csv_filename, unprocessed_dict, column_names, key_first=True):
    csv_path = os.path.join(path, csv_filename)
    data_list = []
    for key in unprocessed_dict.keys():
        temp_list = []
        value = unprocessed_dict[key]
        if key_first:
            temp_list.append(key)
            temp_list.append(value)
        else:
            temp_list.append(value)
            temp_list.append(key)
        data_list.append(temp_list)
    res = pd.DataFrame(columns=column_names, data=data_list)
    res.to_csv(csv_path, encoding='utf-8')


def list_write_csv(csv_filename, unprocessed_list, column_names):
    csv_path = os.path.join(path, csv_filename)
    res = pd.DataFrame(columns=column_names, data=unprocessed_list)
    res.to_csv(csv_path, encoding="utf-8")


def insert_result_table(connection, result_list):
    flag = False
    try:
        connection.ping()
        cursor = connection.cursor()
        insert_sql = "INSERT INTO UICS_RESULT_UNKNOWN_FQ  " \
                     "(QUESTION_ID,UNKNOWN_FQ) " \
                     "values(:1,:2)"
        # for res in result_list:
        cursor.executemany(insert_sql, result_list)
        connection.commit()
        flag = True
    except Exception:
        logger.warning("insert into UICS_MASKQUESTION failed!!!")
        logger.warning(traceback.format_exc())
    return flag




if __name__ == '__main__':
    question_list = get_question_list("FQ_WEEK.csv")
    flag, new_mask_question_dict, result_list = similarity(question_list)
    column_names = ['question_id', 'question']
    write_csv('new_mask_question_dict.csv', new_mask_question_dict, column_names)
    result_list = sorted(result_list, key=lambda res: int(res[0]))
    list_write_csv('result_list.csv', result_list, column_names)

    

    # flag, maskquestion_dict, connection = get_maskquestion_dict()
    # print(flag)
    # print(maskquestion_dict)
    # question_list = get_question_list("FQ_WEEK.csv")
    # question_list = ["我想知道我贷记卡的自动还款账号",
    #                  "我想知道我贷记卡的自动还款账号",
    #                  "我想知道我贷记卡哪个账号在自动扣款",
    #                  "我贷记卡哪个账号在自动还款",
    #                  "贷记卡查询自扣还款账号",
    #                  "贷记卡自动还款的是哪个账号",
    #                  "帮忙查一下我贷记卡的自动还款账号",
    #                  "查一下贷记卡的自动还款账号",
    #                  "查一下我哪个贷记卡账号在自动还款",
    #                  "我需要知道我贷记卡的自动还款账号",
    #                  "我的贷记卡在自动还款的是哪个账号"]
    # flag, new_mask_question_dict, result_list = similarity(question_list)
    # lsh = MinHashLSH(threshold=0.3, num_perm=1024)
    # flag, maskquestion_dict, connection = get_maskquestion_dict()
    # print(maskquestion_dict)
    # if flag:
    #     new_mask_question_dict, result_list = get_result(lsh, question_list, maskquestion_dict, connection)
    #     if insert_result_table(connection, result_list):
    #         print("insert result table success!!!")
        # print(result_list)
    # column_names = ['question_id', 'question']
    # write_csv('new_mask_question_dict.csv', new_mask_question_dict, column_names)
    # result_list = sorted(result_list, key=lambda res: int(res[0]))
    # list_write_csv('result_list.csv', result_list, column_names)
        # write_csv('result_dict.csv', result_list, column_names)
    # print(new_mask_question_dict)
    # print("===============================")
    # print(result_dict)"我想知道我贷记卡的自动还款账号"





