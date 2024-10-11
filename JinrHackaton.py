import csv
import json
import re
from pathlib import Path
from typing import List, Dict
from elasticsearch import Elasticsearch


def save_json(data: List[Dict], file_path: str):
    """Saves data to a JSON file.

    Args:
        data (List[Dict]): List of dictionaries containing the data.
        file_path (str): Path to the file for saving JSON.
    """
    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4, ensure_ascii=False)


def process_csv_data(file_path: str, china_keywords: List[str], hep_keywords: List[str]) -> List[Dict]:
    data = []
    data_org = []
    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            organizations = row.get("Организации", "")
            index_keywords = row.get("Ключевые слова указателя", "")
            author_keywords = row.get("Ключевые слова автора", "")
            doi = row.get("DOI", "")

            print(f"Processing row: {row}")
            print(f"Organizations: {organizations}, DOI: {doi}")

            if any(re.search(rf"\b{keyword.lower()}\b", organizations.lower()) for keyword in china_keywords):
                print(f"Found China-related organization: {organizations}")

                if any(re.search(rf"\b{keyword.lower()}\b", index_keywords.lower()) for keyword in hep_keywords) or \
                        any(re.search(rf"\b{keyword.lower()}\b", author_keywords.lower()) for keyword in hep_keywords):
                    print(f"Found high energy physics related keywords: {index_keywords}, {author_keywords}")

                    data_dict = {
                        "organizations": organizations,
                        "index_keywords": index_keywords,
                        "author_keywords": author_keywords,
                        "DOI": doi,
                    }
                    data_org_dict = {
                        "organizations": organizations,
                        "index_keywords": index_keywords.split('; '),
                    }
                    data.append(data_dict)
                    data_org.append(data_org_dict)

    return data, data_org


def process_files_in_directory(directory: Path, china_keywords: List[str], hep_keywords: List[str]) -> List[Dict]:
    """Processes all CSV files in the given directory.

    Args:
        directory (Path): Path to the directory containing CSV files.
        china_keywords (List[str]): Keywords for filtering organizations.
        hep_keywords (List[str]): Keywords related to high energy physics.

    Returns:
        List[Dict]: Filtered and processed data from all files.
    """
    result = []
    for file in directory.iterdir():
        if file.suffix == ".csv":
            result += process_csv_data(file, china_keywords, hep_keywords)
    return result

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def read_json(path) -> dict | list[dict]:
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_index(agent: Elasticsearch, index_name: str, settings: dict):
    agent.indices.create(index=index_name, body=settings)


def upload(index, data):
    for i, item in enumerate(data):
        print(f'Обработка {i+1} из {len(data)}')
        yield {
            '_index': index,
            '_source': item
        }

def main():
    main_dir = Path(r"datasets")
    china_keywords = ["China", "Chinese", "Chin"]

    hep_keywords = [
        "high energy physics", "particle physics", "quantum field theory", "standard model",
        "quantum chromodynamics", "QCD", "Higgs boson", "collider physics", "LHC",
        "fermions", "bosons", "quarks", "leptons", "gluons", "neutrinos",
        "supersymmetry", "dark matter", "electroweak interaction", "strong interaction",
        "weak interaction", "cosmic rays", "astroparticle physics", "gravitational waves", 
        "string theory", "extra dimensions", "grand unified theory", "TeV scale", "baryogenesis",
        "neutrino oscillations", "proton decay", "CERN", "muon", "tau", "proton", "antimatter", 
        "dark energy", "inflationary universe", "CP violation", "neutron stars", "black holes", 
        "Hawking radiation", "quantum gravity", "Planck scale", "supergravity", "heavy ion physics", 
        "lattice QCD", "non-abelian gauge theories", "gauge symmetry", "standard model anomalies"
    ]

    result, result_org = process_files_in_directory(main_dir, china_keywords, hep_keywords)[0], process_files_in_directory(main_dir, china_keywords, hep_keywords)[1]
    save_json(result, "data_filtered.json")
    save_json(result_org, 'organisations.json')

    login = 'elastic'
    password = 'jinr_elastic*'
    url = f'http://{login}:{password}@localhost:9200'
    es = Elasticsearch(
        hosts=url
    )
    print(es.info())
    index = "chinan5"
    settings = read_json('mapping.json')
    create_index(es, index, settings)
    data = read_json('data_filtered.json')

    settings_org = read_json('org_mapping.json')
    data_org = read_json('organisations.json')
    index_org = 'chinen5orgs'
    create_index(es, index_org, settings_org)
    bulk(es, upload(index_org, data_org))
    bulk(es, upload(index, data))
    print('bulk complted')

if __name__ == '__main__':
    main()