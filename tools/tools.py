
from langchain_community.tools.tavily_search import TavilySearchResults
from datetime import datetime

def get_news_url_tavily(marca:str, max_results=5):
    '''searches for Linkedin profile pages'''

    search = TavilySearchResults(max_results=max_results)
    
    res = search.run(f'traga as informações da {marca}')

    return res


import re
import json


def find_parttner_communicable_facts(s):
    # Sua string de entrada

    # Expressão regular para extrair o conteúdo JSON entre ```json e ```
    pattern = r'```json\n(.*?)\n```'

    # Usa re.search para encontrar o padrão na string
    match = re.search(pattern, s, re.DOTALL)

    if match:
        json_content = match.group(1)
        # Carrega o conteúdo JSON
        data = json.loads(json_content)
        # Agora você tem uma lista de dicionários Python
        return data
    else:
        print('Nenhum conteúdo JSON encontrado.')



import json
import glob
import os

def get_all_json_files(pasta):
    informacoes_concatenadas = ''
    padrao = os.path.join(pasta, '*.json')
    arquivos_json = glob.glob(padrao)
    for arquivo in arquivos_json:
        with open(arquivo, 'r', encoding='utf-8') as f:
            dados = json.load(f)
            informacoes_concatenadas += json.dumps(dados, ensure_ascii=False)
    return informacoes_concatenadas    

def load_json_(filename, folder_name):
    try:
        with open(os.path.join(folder_name, filename), 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return None


def gerar_lista_fatos_e_razoes(fatos):
    # Lista para armazenar as strings formatadas
    lista_formatada = []
    
    # Loop para adicionar cada fato e sua razão no formato desejado
    for index, item in enumerate(fatos, start=1):
        fato = item['communicable_facts']
        razao = item['communicable_facts_reason']
        lista_formatada.append(f"{index}. {fato}\n   Razão: {razao}")
    
    # Unindo a lista em uma string, separada por quebras de linha
    return "\n\n".join(lista_formatada)


def gerar_json_fatos_e_razoes(fatos):
    # Criando uma lista de dicionários no formato esperado
    dados_formatados = []
    
    for index, item in enumerate(fatos, start=1):
        dados_formatados.append({
            'fato_numero': index,
            'communicable_facts': item['communicable_facts'],
            'communicable_facts_reason': item['communicable_facts_reason']
        })
    
    # Convertendo a lista para formato JSON, com indentação para melhor leitura
    return json.dumps(dados_formatados, indent=4, ensure_ascii=False)


import json

def load_json(file_path):
    """
    Carrega um arquivo JSON e retorna o conteúdo como um objeto Python.

    Args:
        file_path (str): Caminho para o arquivo JSON.

    Returns:
        list: Dados carregados do arquivo JSON (geralmente uma lista de dicionários).
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except FileNotFoundError:
        print(f"Erro: O arquivo {file_path} não foi encontrado.")
        return None
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar o arquivo JSON: {e}")
        return None



def get_status_by_client_name(client_name):
    data = load_json(r'infos_clientes\projects.json')

    for entry in data:
        if entry['clientName'].lower() == client_name.lower().replace('_', ' '):
            return entry['statusProject']
    return "Client not find"