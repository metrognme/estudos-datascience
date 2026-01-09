import requests
import pandas as pd
import os
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("CNJ_API_KEY")

URL_API = "https://api-publica.datajud.cnj.jus.br/api_publica_tjgo/_search"

def buscar_todos_processos():
    headers = {
        "Authorization": API_KEY,
        "Content-Type": "application/json"
    }

    data_fim = datetime.now().strftime("%Y%m%d%H%M%S")
    data_inicio = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d%H%M%S")
    
    print(f"[*] Iniciando extração total (Janela: 1 ano)")
    print(f"[*] Filtro: LGPD, Dados ou Privacidade")

    todos_processos = []
    
    tamanho_pagina = 100  
    ultimo_sort = None    
    
    while True:
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "dataAjuizamento": {
                                    "gte": data_inicio,
                                    "lte": data_fim
                                }
                            }
                        },
                        {
                            "query_string": {
                                "query": "(LGPD) OR (*Dados*) OR (*Privacidade*)",
                                "fields": ["assuntos.nome", "movimentos.nome"]
                            }
                        }
                    ]
                }
            },
            "size": tamanho_pagina,
            "sort": [
                {"dataAjuizamento": "desc"},
                {"numeroProcesso.keyword": "asc"} 
            ],
            "_source": ["numeroProcesso", "classe.nome", "assuntos", "dataAjuizamento", "orgaoJulgador.nome"]
        }

        if ultimo_sort:
            query["search_after"] = ultimo_sort

        try:
            response = requests.post(URL_API, json=query, headers=headers)
            
            if response.status_code == 200:
                dados = response.json()
                hits = dados.get('hits', {}).get('hits', [])
  
                if not hits:
                    print("[!] Fim dos resultados encontrados.")
                    break
                
                todos_processos.extend(hits)
                
                ultimo_sort = hits[-1].get('sort')
                
                print(f"[+] Baixados {len(hits)} processos. Total acumulado: {len(todos_processos)}")
                
                time.sleep(3)
                
            else:
                print(f"[-] Erro na requisição: {response.status_code} - {response.text}")
                break

        except Exception as e:
            print(f"[!] Erro crítico durante a paginação: {e}")
            break
            
    return todos_processos

def processar_dados(raw_data):
    lista_processada = []
    
    for item in raw_data:
        source = item.get('_source', {})
        
        data_bruta = source.get('dataAjuizamento', '')
        data_formatada = data_bruta
        if len(data_bruta) >= 8:
            data_formatada = f"{data_bruta[6:8]}/{data_bruta[4:6]}/{data_bruta[0:4]}"

        assuntos_lista = source.get('assuntos', [])
        if assuntos_lista is None:
            assuntos_lista = []
        assuntos_texto = ", ".join([a.get('nome', '') for a in assuntos_lista if isinstance(a, dict)])

        processo = {
            "CNJ": source.get('numeroProcesso'),
            "Classe": source.get('classe', {}).get('nome') if isinstance(source.get('classe'), dict) else str(source.get('classe')),
            "Data Ajuizamento": data_formatada,
            "Vara": source.get('orgaoJulgador', {}).get('nome') if isinstance(source.get('orgaoJulgador'), dict) else str(source.get('orgaoJulgador')),
            "Assuntos": assuntos_texto
        }
        lista_processada.append(processo)
        
    return pd.DataFrame(lista_processada)

if __name__ == "__main__":
    resultados = buscar_todos_processos()
    
    if resultados:
        print(f"\n[*] Processando {len(resultados)} registros...")
        df = processar_dados(resultados)
        df = df.drop_duplicates(subset=['CNJ'])
        
        nome_arquivo = f"tjgo_lgpd_full_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
        print(f"[v] Arquivo gerado com sucesso: {nome_arquivo}")
        print(df.head())
    else:
        print("[-] Nenhum processo encontrado.")