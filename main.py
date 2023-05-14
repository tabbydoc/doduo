import argparse
import os
from collections import defaultdict
import json
import pandas as pd
from doduo.doduo import Doduo

def write_json(path, filename,  result):

    if not os.path.exists(path):
        os.makedirs(path)

    with open(path + "/" + filename[:-3] + "json", "w", encoding="utf-8") as file:
        json.dump(result, file, indent=4, ensure_ascii=False)

def read_tables(source):
    with os.scandir(source) as files:
        file_list = []
        for file in files:
            file_list.append(str(file.name))
    return file_list

def write_total_score(path,  result):

    if not os.path.exists(path):
        os.makedirs(path)
    with open(path, "a", encoding="utf-8") as file:
        file.write(result)



SOURCE_PATH = './uploads/'
RESULT_PATH = './result/'
TOTAL_SCORE_PATH = "./total_score.txt"



files = read_tables(SOURCE_PATH)

args = argparse.Namespace
args.model = "viznet" # or args.model = "viznet" wikitable
doduo = Doduo(args)
str_type = ""


for file in files:
    df = pd.read_csv(SOURCE_PATH + file)
    res = doduo.annotate_columns(df)
    result = defaultdict()

    for i in range(len(res.coltypes)):
        result[df.columns[i]] = res.coltypes[i]
        str_type += " " + res.coltypes[i]


    write_total_score(TOTAL_SCORE_PATH, file +": "+ str_type + "\n")
    write_json(RESULT_PATH, file, result)
    result.clear()
    str_type = ""
