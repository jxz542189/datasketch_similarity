import pandas as pd
import os


path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')


def get_question_list(csv_filename):
    csv_data = pd.read_csv(os.path.join(path, csv_filename))
    question_list = csv_data['QUESTION'].values.tolist()
    return question_list


# question_list = get_question_list("FQ_WEEK.csv")
# print(question_list[:5])


