import math
import os
import pickle
import numpy as np
import pandas as pd


with open('data/table_col_type_serialized.pkl', 'rb') as f:
    data = pickle.load(f)

EXTEND_PATH = "./extend_col_class_checked_fg.csv"
TOTAL_SCORE_PATH = "./total_score.txt"
SOURCE_PATH = './uploads/'
RESULT_PATH = './result/'

def read_tables(source):
    with os.scandir(source) as files:
        file_list = []
        for file in files:
            file_list.append(str(file.name))
    return file_list



label_ids = [0] * 255
class_list = list(data['mlb'].classes_)
# class_position = int(np.where(data['mlb'].classes_== 'location.location')[0][0])

files = read_tables(SOURCE_PATH)

# количество таблиц в датафреймах
count_train, count_dev, count_test = 0, 0, 0

# количество таблиц для каждого датафрейма
average_count_train_table = math.ceil(len(files) * 0.6)
average_count_table = math.ceil((len(files) * 0.4) / 2)

for file in range(len(files)):
    if count_train < average_count_train_table:
        train_file = pd.read_csv(SOURCE_PATH + files[file])
        count_train += 1
        train = pd.DataFrame({'table_id' : [], 'labels' : [], 'data' : [], 'label_ids' : []})

        for i in train_file.values:
            for j in range(len(train_file.columns)):

                label_ids[int(np.where(data['mlb'].classes_== train_file.columns[j].split('(')[1].strip(')'))[0][0])] = 1
                new_train_row = pd.DataFrame([
                    {'table_id': "1-" + str(j+1), 'labels': train_file.columns[j].split('(')[1].strip(')'), 'data': i[j], 'label_ids': label_ids}
                ])

                train = train.append(new_train_row, ignore_index=True)
                label_ids = [0] * 255

    elif count_dev < average_count_table:
        dev_file = pd.read_csv(SOURCE_PATH + files[file])
        count_dev += 1
        dev = pd.DataFrame({'table_id' : [], 'labels' : [], 'data' : [], 'label_ids' : []})

        for i in dev_file.values:
            for j in range(len(dev_file.columns)):

                label_ids[int(np.where(data['mlb'].classes_== dev_file.columns[j].split('(')[1].strip(')'))[0][0])] = 1
                new_dev_row = pd.DataFrame([
                    {'table_id': "2-" + str(j+1), 'labels': dev_file.columns[j].split('(')[1].strip(')'), 'data': i[j], 'label_ids': label_ids}
                ])

                dev = dev.append(new_dev_row, ignore_index=True)
                label_ids = [0] * 255

    elif count_test < average_count_table:
        test_file = pd.read_csv(SOURCE_PATH + files[file])
        count_test += 1
        test = pd.DataFrame({'table_id' : [], 'labels' : [], 'data' : [], 'label_ids' : []})

        for i in test_file.values:
            for j in range(len(test_file.columns)):

                label_ids[int(np.where(data['mlb'].classes_== test_file.columns[j].split('(')[1].strip(')'))[0][0])] = 1
                new_test_row = pd.DataFrame([
                    {'table_id': "3-" + str(j+1), 'labels': test_file.columns[j].split('(')[1].strip(')'), 'data': i[j], 'label_ids': label_ids}
                ])

                test = test.append(new_test_row, ignore_index=True)
                label_ids = [0] * 255


result = dict()
result['train'] = train
result['dev'] = dev
result['test'] = test
result['mlb'] = data['mlb']


with open('result.pkl', 'wb') as f:
    pickle.dump(result, f)









