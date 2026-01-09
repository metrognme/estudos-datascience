import requests
import pandas as pd
import os
import time
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pathlib import Path

# --- CONFIGURAÇÃO E CARREGAMENTO DA CHAVE ---
# 1. Procura o .env na pasta do script OU na pasta acima (caso tenha movido o script)
pasta_script = Path(__file__).parent
arquivo_env = pasta_script / ".env"

if not arquivo_env.exists():
    arquivo_env = pasta_script.parent / ".env"

load_dotenv(dotenv_path=arquivo_env)
API_KEY = os.getenv("CNJ_API_KEY")

# 2. Verificação Crítica
if not API_KEY:
    print(f"[!] ERRO CRÍTICO: Não encontrei o arquivo .env.")
    print(f"    Procurei em: {pasta_script} e na pasta acima.")
    print("    Certifique-se de que o arquivo .env contém: CNJ_API_KEY=\"ApiKey ...\"")
    sys.exit(1)

# 3. Correção automática do formato da chave
if not API_KEY.startswith("ApiKey "):
    print("[*] Ajustando formato da API Key (adicionando prefixo 'ApiKey')...")
    API_KEY = API_KEY
else:
    print("[*] Chave carregada corretamente.")

URL_API = "https://api-publica.datajud.cnj.jus.br/api_publica_tjgo/_search"

# --- FUNÇÕES ---

def buscar_todos_dados_completos():
    """Busca dados brutos na API do DataJud"""
    headers = {
        "Authorization": API_KEY,
        "Content-Type": "application/json"
    }

    data_fim = datetime.now().strftime("%Y%m%d%H%M%S")
    # Janela de 1 ano. Para testes rápidos, pode diminuir o timedelta
    data_inicio = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d%H%M%S")
    
    print(f"[*] Iniciando extração COMPLETA (Janela: 1 ano)")
    print(f"[*] Buscando de {data_inicio} até {data_fim}")

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
            ]
        }

        if ultimo_sort:
            query["search_after"] = ultimo_sort

        try:
            response = requests.post(URL_API, json=query, headers=headers)
            
            if response.status_code == 200:
                dados = response.json()
                hits = dados.get('hits', {}).get('hits', [])
                
                if not hits:
                    print("[*] Fim da busca (sem mais resultados).")
                    break
                
                todos_processos.extend(hits)
                ultimo_sort = hits[-1].get('sort')
                
                print(f"[+] Baixados {len(hits)} processos. Total acumulado: {len(todos_processos)}")
                time.sleep(3)
            
            elif response.status_code == 401:
                print(f"[-] ERRO 401: A API recusou a chave.")
                print(f"[-] Detalhes: {response.text}")
                break
            else:
                print(f"[-] Erro inesperado: {response.status_code} - {response.text}")
                break

        except Exception as e:
            print(f"[!] Erro de conexão: {e}")
            break
            
    return todos_processos

def processar_dados_ricos(raw_data):
    """Processa o JSON bruto para um DataFrame limpo"""
    lista_processada = []
    
    for item in raw_data:
        source = item.get('_source', {})
        dados_basicos = source.get('dadosBasicos', {})
        
        # 1. Tratamento de Data
        data_bruta = dados_basicos.get('dataAjuizamento', '') 
        if not data_bruta:
            data_bruta = source.get('dataAjuizamento', '') 
            
        data_formatada = data_bruta
        if len(data_bruta) >= 8:
            data_formatada = f"{data_bruta[6:8]}/{data_bruta[4:6]}/{data_bruta[0:4]}"

        # 2. Tratamento do Valor da Causa
        valor_causa = dados_basicos.get('valor', 0.0)
        valor_formatado = f"R$ {valor_causa:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".") if valor_causa else "R$ 0,00"

        # 3. Tratamento de Assuntos
        assuntos_lista = source.get('assuntos', [])
        assuntos_texto = ", ".join([a.get('nome', '') for a in assuntos_lista if isinstance(a, dict)])

        # 4. Engenharia Reversa dos Polos
        autores = []
        reus = []
        lista_polos = dados_basicos.get('polo', []) 
        
        for polo in lista_polos:
            tipo = polo.get('polo') 
            parte_nome = polo.get('parte', {}).get('pessoa', {}).get('nome', 'Desconhecido')
            
            if tipo == 'AT':
                autores.append(parte_nome)
            elif tipo == 'PA':
                reus.append(parte_nome)

        processo = {
            "CNJ": dados_basicos.get('numero') or source.get('numeroProcesso'),
            "Classe Processual": source.get('classe', {}).get('nome'),
            "Data Ajuizamento": data_formatada,
            "Valor da Causa": valor_causa,
            "Valor Formatado": valor_formatado,
            "Vara/Órgão": source.get('orgaoJulgador', {}).get('nome'),
            "Município": source.get('orgaoJulgador', {}).get('codigoMunicipioIBGE'),
            "Polo Ativo (Autores)": ", ".join(autores),
            "Polo Passivo (Réus)": ", ".join(reus),
            "Nível de Sigilo": dados_basicos.get('nivelSigilo', 0),
            "Processo Eletrônico?": "Sim" if dados_basicos.get('procEl') == 1 else "Não",
            "Assuntos": assuntos_texto
        }
        lista_processada.append(processo)
        
    return pd.DataFrame(lista_processada)

# --- EXECUÇÃO PRINCIPAL ---
if __name__ == "__main__":
    print(f"\n--- SCRAPPER DATAJUD INICIADO ---")
    resultados = buscar_todos_dados_completos()
    
    if resultados:
        print(f"\n[*] Processando {len(resultados)} registros...")
        df = processar_dados_ricos(resultados)
        
        # Remove duplicatas baseadas no número do processo
        df = df.drop_duplicates(subset=['CNJ'])
        
        # Nome Dinâmico
        data_hoje = datetime.now().strftime('%Y-%m-%d')
        nome_arquivo = f"tjgo_lgpd_COMPLETO_{data_hoje}.csv"
        
        # Salva o arquivo
        df.to_csv(nome_arquivo, index=False, encoding='utf-8-sig')
        
        print(f"[v] SUCESSO! Arquivo gerado: {nome_arquivo}")
        print(f"[i] Prévia dos dados:")
        print(df[['CNJ', 'Valor Formatado', 'Polo Passivo (Réus)']].head())
    else:
        print("[-] Nenhum processo encontrado com os critérios atuais.")