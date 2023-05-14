import argparse
import os
import time
from collections import defaultdict
import json
import pandas as pd
from doduo.doduo import Doduo

total_start = time.time()

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

def write_json(path, filename,  result):

    if not os.path.exists(path):
        os.makedirs(path)
    with open(path + "/" + filename[:-3] + "json", "w", encoding="utf-8") as file:
        json.dump(result, file, indent=4, ensure_ascii=False)

def write_total_score(path,  result):

    if not os.path.exists(path):
        os.makedirs(path)
    with open(path, "a", encoding="utf-8") as file:
        file.write(result)


def type_mapping(type):
    if type == "film.film":
        return "film"
    if type == "people.person":
        return "person"
    if type == "time.event":
        return "event"
    if type == "organization.organization":
        return "organisation"
    if type == "location.location":
        return "place"
    if type == "metropolitan_transit.transit_line":
        return "routeoftransportation"
    if type == "business.business_operation":
        return "Company".lower()
    if type == "cvg.computer_videogame":
        return "VideoGame".lower()
    if type == "cvg.cvg_developer":
        return "Writer".lower()
    if type == "cvg.cvg_genre":
        return "Genre".lower()
    if type == "broadcast.radio_format":
        return "RadioProgram".lower()
    if type == "finance.currency":
        return "Currency".lower()
    if type == "language.human_language":
        return "Language".lower()
    if type == "fictional_universe.fictional_character":
        return "FictionalCharacter".lower()
    if type == "sports.sport":
        return "Sport".lower()
    # внимание
    if type == "biology.organism_classification":
        return "biology.organism_classification"
    if type == "tv.tv_series_season":
        return "TelevisionSeason".lower()
    if type == "boats.ship":
        return "Ship".lower()
    if type == "education.athletics_brand":
        return "Athletics".lower()
    if type == "music.composition":
        return "MusicalWork".lower()
    if type == "music.artist":
        return "MusicalArtist".lower()
    if type == "music.genre":
        return "MusicGenre".lower()
    if type == "education.educational_institution":
        return "EducationalInstitution".lower()
    if type == "rail.locomotive_class":
        return "Locomotive".lower()
    if type == "sports.sports_position":
        return "SportsTeamMember".lower()
    # внимание
    if type == "religion.religion":
        return "religion.religion".lower()

    if type == "sports.sports_team":
        return "SportsTeam".lower()
    if type == "government.legislative_session":
        return "Legislature".lower()
    if type == "media_common.media_genre":
        return "Genre".lower()
    # внимание
    if type == "book.periodical":
        return "book.periodical".lower()

    if type == "tv.tv_program":
        return "TelevisionSeason".lower()
    if type == "astronomy.celestial_object":
        return "CelestialBody".lower()
    if type == "sports.sports_league_draft":
        return "SportsLeague".lower()
    if type == "broadcast.broadcast":
        return "Broadcaster".lower()
    if type == "cvg.cvg_platform":
        return "Software".lower()
    if type == "government.politician":
        return "Politician".lower()
    if type == "government.political_party":
        return "PoliticalParty".lower()
    if type == "music.media_format":
        return "Software".lower()
    if type == "aviation.aircraft_model":
        return "Aircraft".lower()
    if type == "business.brand":
        return "Company".lower()

    # внимание
    if type == "government.governmental_body":
        return "government.governmental_body".lower()

    if type == "music.musical_group":
        return "Band".lower()
    if type == "award.award_category":
        return "award".lower()


files = read_tables(SOURCE_PATH)
table = pd.DataFrame(pd.read_csv(EXTEND_PATH))

args = argparse.Namespace
args.model = "wikitable" # or args.model = "viznet"
doduo = Doduo(args)

total_ah, total_ap = 0,0


for file in files:
    start = time.time()
    df = pd.read_csv(SOURCE_PATH + file)
    res = doduo.annotate_columns(df)
    result = defaultdict()
    perfect, good, wrong, ah_score, ap_score, total = 0, 0, 0, 0, 0, 0


    for i in range(len(res.coltypes)):
        result[df.columns[i]] = res.coltypes[i]

    for i in table.index:
        if table['name'][i] == file[:-4]:
            total += 1
            # col_type = res.coltypes[table['column'][i]].split(".")[len(res.coltypes[table['column'][i]].split(".")) - 1]
            col_type = type_mapping(res.coltypes[table['column'][i]])
            col_perfect = str([table['perfectAnnotation'][i]]).split("/")[4][:-1].lower()[:-1]
            if (col_type == col_perfect) or (col_type == "biology.organism_classification" and col_perfect == "eukaryote") or (col_type == "biology.organism_classification" and col_perfect == "species") or (col_type == "religion.religion" and col_perfect == "religiousorganisation") or (col_type == "religion.religion" and col_perfect == "ethnicgroup") or (col_type == "book.periodical" and col_perfect == "periodicalliterature") or (col_type == "book.periodical" and col_perfect == "book") or (col_type == "government.governmental_body" and col_perfect == "governmenttype") or (col_type == "government.governmental_body" and col_perfect == "governmentagency"):
                perfect += 1
            else:
                wrong += 1
                if table['goodAnnotation'] is not None:
                    good_annotation_list = ''.join(word.lower() for word in (str([table['goodAnnotation'][i]]).split('http://dbpedia.org/ontology/')) if len(word) > 2)[:-2].split(",")

                    for good_annotation in good_annotation_list:
                        if perfect + good < total:
                            if (col_type == good_annotation) or ((col_type == "biology.organism_classification" and good_annotation == "eukaryote") or (col_type == "biology.organism_classification" and good_annotation == "species") or (col_type == "religion.religion" and good_annotation == "ethnicgroup") or (col_type == "religion.religion" and good_annotation == "religiousorganisation") or (col_type == "book.periodical" and good_annotation == "book") or (col_type == "book.periodical" and good_annotation == "periodicalliterature") or (col_type == "government.governmental_body" and good_annotation == "governmentagency") or (col_type == "government.governmental_body" and good_annotation == "governmenttype")):
                                good += 1
                                wrong -= 1


    if total > 0:
        ah_score = ((1 * perfect) + (0.5 * good) - (1 * wrong)) / total
        ap_score = perfect / total


        total_ah += ah_score if ah_score > 0 else 0
        total_ap += ap_score if ap_score > 0 else 0
    else:
        ah_score = 555
        ap_score = 555
    end = time.time() - start

    if ah_score >= 0:
        comment = ""
    else: comment = ", AH score = " + str(ah_score)
    write_total_score(TOTAL_SCORE_PATH, file[:-4] + ", " + str(ah_score) + ", " + str(ap_score) + ", " + str(end) + comment + '\n')

    print("******************************************************************")
    print(file[:-4])
    print(f"\nПерфект: " + str(perfect), "Гуд: " + str(good), "Бэд: " + str(wrong))
    print("AH score: " + str(ah_score) + '\n' "AP score: " + str(ap_score))
    print("******************************************************************")

    write_json(RESULT_PATH, file, result)
    result.clear()

total_end = time.time() - total_start
write_total_score(TOTAL_SCORE_PATH, "Total time: " + str(total_end) + " c." + '\n')
print("Total time: " + str(total_end) + " c." + '\n')
print("Total AH: " + str(total_ah / 236) + '\n')
print("Total AP: " + str(total_ap / 236) + '\n')


