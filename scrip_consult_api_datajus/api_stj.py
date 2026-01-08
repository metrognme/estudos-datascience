import requests
import pandas as pd
import sqlite3
import os
from datetime import datetime
from dotenv import load_dotenv

# load .env
load_dotenv()
API_KEY = os.getenv("CNJ_API_KEY")
if API_KEY is None:
    raise ValueError("A chave da API CNJ_API_KEY não foi encontrada nas variáveis de ambiente.")

# Parâmetros   ---MUDAR AQUI 
TRIBUNAL = "tjgo"
ASSUNTO_CODIGO = 14205  
DATA_INICIO = "2023-05-01T00:00:00.000Z"
DATA_FIM = "2023-05-30T23:59:59.000Z"
URL = f"https://api-publica.datajud.cnj.jus.br/api_publica_{TRIBUNAL}/_search"

#funçoes query
def gerar_nome_db(tribunal, assunto):
    
    data_hoje = datetime.now().strftime("%Y%m%d")
    nome_arquivo = f"extracao_{tribunal}_{assunto}_{data_hoje}.db"
    return nome_arquivo

def extrair_dados_cnj():
    headers = {
        "Authorization": API_KEY,
        "Content-Type": "application/json"
    }

    query = {
        "query": {
            "bool": {
                "must": [
                    {"match": {"assuntos.codigo": ASSUNTO_CODIGO}},
                    {"range": {"dataAjuizamento": {"gte": DATA_INICIO, "lte": DATA_FIM}}}
                ]
            }
        },
        "_source": [
            "numeroProcesso", 
            "classe.nome", 
            "valorCausa", 
            "dataAjuizamento", 
            "orgaoJulgador.nome", 
            "tribunal"
        ],
        "size": 100 
    }

    try:
        print(f"[*] Iniciando extração no {TRIBUNAL.upper()}...")
        response = requests.post(URL, headers=headers, json=query, timeout=30)
        response.raise_for_status() 
        
        data = response.json()
        hits = data.get('hits', {}).get('hits', [])
        
        print(f"[+] {len(hits)} registros recuperados com sucesso.")
        return [h['_source'] for h in hits]

    except Exception as e:
        print(f"[!] Erro na extração: {e}")
        return []

def processar_e_salvar_sql(dados, nome_banco):
    if not dados:
        print("Nenhum dado encontrado.")
        return

    df = pd.DataFrame(dados)

    # limpeza
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].apply(lambda x: x.get('nome') if isinstance(x, dict) else x)

    if 'valorCausa' not in df.columns:
        df['valorCausa'] = 0.0
    else:
        df['valorCausa'] = pd.to_numeric(df['valorCausa'], errors='coerce').fillna(0.0)

    if 'dataAjuizamento' in df.columns:
        df['dataAjuizamento'] = pd.to_datetime(df['dataAjuizamento']).dt.date
        hoje = datetime.now().date()
        df['dias_em_tramitacao'] = (hoje - df['dataAjuizamento']).apply(lambda x: x.days if pd.notnull(x) else 0)

    # banco de dados com nome dinâmico
    try:
        conn = sqlite3.connect(nome_banco)
        df.to_sql('processos_contingenciamento', conn, if_exists='replace', index=False)
        conn.close()
        print(f"[*] Sucesso! Dados salvos no arquivo: {nome_banco}")
    except Exception as e:
        print(f"[!] Erro ao salvar no SQL: {e}")

if __name__ == "__main__":
    print("[*] Iniciando o script...")
    
    db_dinamico = gerar_nome_db(TRIBUNAL, ASSUNTO_CODIGO)
    
    lista_processos = extrair_dados_cnj()
    
    if lista_processos:
        processar_e_salvar_sql(lista_processos, db_dinamico)
    else:
        print("[!] Abortando: Nenhuma informação para processar.")