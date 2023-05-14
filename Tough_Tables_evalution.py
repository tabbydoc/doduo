import argparse
import re
from enum import Enum
from itertools import combinations
from typing import Any, Dict, List, Tuple
from urllib.error import URLError
import os
import time
from collections import defaultdict
import json
import pandas as pd

from SPARQLWrapper import JSON, SPARQLWrapper

from doduo import Doduo


class DBPediaConfig(str, Enum):
    ENDPOINT_NAME = "http://dbpedia.org/sparql"
    BASE_RESOURCE_URI = "http://dbpedia.org/resource/"
    BASE_ONTOLOGY_URI = "http://dbpedia.org/ontology/"

def get_parent_classes(dbpedia_class: str = "", short_name: bool = False) -> Tuple[str, ...]:
    """
    Get a set of parent classes for a target class based on the direct SPARQL query to DBpedia
    :param dbpedia_class: a target class from DBpedia
    :param short_name: flag to enable or disable short class name display mode (without full URI)
    :return: a list of found parent classes
    """
    sparql = SPARQLWrapper(DBPediaConfig.ENDPOINT_NAME)
    sparql.setQuery("""
        SELECT DISTINCT ?type
        WHERE {
            <%s> rdfs:subClassOf* ?type .
            FILTER (strstarts(str(?type), "http://dbpedia.org/ontology/")) .
        }
    """ % dbpedia_class)
    sparql.setTimeout(300)
    sparql.setReturnFormat(JSON)
    response = sparql.query().convert()
    class_uris = []
    for rs in response["results"]["bindings"]:
        class_uris.append(rs["type"]["value"].replace(DBPediaConfig.BASE_ONTOLOGY_URI, "") if short_name else rs["type"]["value"])
    return tuple(class_uris)

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
    if type == "location.country":
        return "country".lower()
    if type == "location.administrative_division":
        return "AdministrativeRegion".lower()
    if type == "music.album":
        return "Album".lower()
    if type == "broadcast.artist":
        return "artist".lower()
    if type == "government.government_office_or_title":
        return "GovernmentType".lower()
    if type == "architecture.structure":
        return "ArchitecturalStructure".lower()
    if type == "baseball.baseball_team":
        return "BaseballTeam".lower()
    if type == "sports.pro_athlete":
        return "Athlete".lower()
    if type == "location.hud_county_place":
        return "place".lower()
    if type == "medicine.drug":
        return "drug".lower()
    if type == "internet.website":
        return "Website".lower()



total_start = time.time()

EXTEND_PATH = "./CTA_2T_gt.csv"
TOTAL_SCORE_PATH = "./total_score.txt"
SOURCE_PATH = './uploads/'
RESULT_PATH = './result/'

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
            col_type = type_mapping(res.coltypes[table['column'][i]])
            col_perfect = str([table['perfectAnnotation'][i]]).split("/")[4][:-1].lower()[:-1]
            # str(table['perfectAnnotation'][i]) == "http://www.w3.org/2002/07/owl#Thing") or
            if (str(table['perfectAnnotation'][i]) == "http://www.w3.org/2002/07/owl#Thing") or (col_type == col_perfect) or (col_type == "biology.organism_classification" and col_perfect == "eukaryote") or (col_type == "biology.organism_classification" and col_perfect == "species") or (col_type == "religion.religion" and col_perfect == "religiousorganisation") or (col_type == "religion.religion" and col_perfect == "ethnicgroup") or (col_type == "book.periodical" and col_perfect == "periodicalliterature") or (col_type == "book.periodical" and col_perfect == "book") or (col_type == "government.governmental_body" and col_perfect == "governmenttype") or (col_type == "government.governmental_body" and col_perfect == "governmentagency"):
                perfect += 1
            else:
                wrong += 1
                time.sleep(1)
                good_annotation_list = get_parent_classes(str(table['perfectAnnotation'][i]))
                if len(good_annotation_list) > 0:
                    # good_annotation_list = ''.join(word.lower() for word in (str([table['goodAnnotation'][i]]).split('http://dbpedia.org/ontology/')) if len(word) > 2)[:-2].split(",")
                    for good_annotation in good_annotation_list:
                        good_annotation = good_annotation.split("/")[4].lower()
                        if perfect + good < total:
                            if (col_type == good_annotation) or ((
                                                                         col_type == "biology.organism_classification" and good_annotation == "eukaryote") or (
                                                                         col_type == "biology.organism_classification" and good_annotation == "species") or (
                                                                         col_type == "religion.religion" and good_annotation == "ethnicgroup") or (
                                                                         col_type == "religion.religion" and good_annotation == "religiousorganisation") or (
                                                                         col_type == "book.periodical" and good_annotation == "book") or (
                                                                         col_type == "book.periodical" and good_annotation == "periodicalliterature") or (
                                                                         col_type == "government.governmental_body" and good_annotation == "governmentagency") or (
                                                                         col_type == "government.governmental_body" and good_annotation == "governmenttype")):
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
print("Total AH: " + str(total_ah / 180) + '\n')
print("Total AP: " + str(total_ap / 180) + '\n')