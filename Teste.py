from apache_atlas.client.ApacheAtlas import ApacheAtlasClient
import random
import json
import os
from apache_atlas.utils.Constants import EndRelations, TypeNames
from apache_atlas.utils.API import API, HTTPMethod
import re
import logging

log_file = "processamento.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s: %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

atlas_section = ApacheAtlasClient(
    "http://10.100.100.61:21000",
    "admin",
    "admin"
)

def extract_sigla(filename):
    return filename.replace('.parquet', '')[:-6]

def extract_alphanumeric_without_extension(filename):
    return filename.replace('.parquet', '')[-4:]

pasta = 'data/arquivos-sih.json'


with open(pasta, 'r', encoding='utf-8') as f:
    dados_json = json.load(f)

sia = atlas_section.entity.get_entity_by_guid('df0b4564-1597-4267-aca6-33edf1b392b2')

print(json.dumps(sia, indent=2))

tables_sia = {}
colunas_sia = {}

def filtro(arquivo):
    sigla = extract_sigla(arquivo['nome'])
    return sigla not in ['AMP']

dados_json = list(filter(filtro, dados_json)) 

for arquivo in dados_json:
    nome = arquivo['name'].upper()
    sigla = extract_sigla(nome)
    colunas = sigla + extract_alphanumeric_without_extension(nome)

    arquivo['description'] = f"Arquivo de {sigla} - {nome}"
    arquivo[EndRelations.END_TABLE_TO_FILE[1]] = {'guid': tables_sia[sigla] }
    arquivo[EndRelations.END_TABLE_FILE_COLUMN[0]] = { 'guid': colunas_sia[sigla][colunas] }

print(json.dumps(dados_json, indent=2))