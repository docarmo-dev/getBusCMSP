import json
import requests
import csv
import boto3
import io
from datetime import datetime
from zoneinfo import ZoneInfo
from botocore.exceptions import ClientError

# ================= CONFIGURAÇÕES =================

TOKEN = "0f5569588bd77253eea7cf903c0bbd4f3f07d2bd2c7fae33d54bd8ec2a63e047"
BUCKET_NAME = "sptrans-bus-dados-marcel"
ARQUIVO_ENTRADA = "Dados_Entrada.txt"

# =================================================

s3 = boto3.client("s3")
session = requests.Session()

# -------- AUTENTICAÇÃO --------
def autenticar():
    url = f"http://api.olhovivo.sptrans.com.br/v2.1/Login/Autenticar?token={TOKEN}"
    r = session.post(url)
    return r.status_code == 200


# -------- PREVISÃO --------
def previsao_por_parada(parada):
    url = f"http://api.olhovivo.sptrans.com.br/v2.1/Previsao/Parada?codigoParada={parada}"
    r = session.get(url, timeout=10)

    if r.status_code != 200:
        return None

    return r.json()


# -------- LEITURA ENTRADA --------
def ler_dados_entrada():
    dados = []

    try:
        with open(ARQUIVO_ENTRADA, "r", encoding="utf-8") as f:
            for linha in f:
                partes = linha.strip().split(",")

                if len(partes) >= 2:
                    dados.append({
                        "parada": partes[0].strip(),
                        "sentido": partes[1].strip()
                    })
    except Exception as e:
        print("Erro ao ler arquivo:", e)

    return dados


# -------- CARREGAR ÚLTIMAS PREVISÕES --------
def carregar_ultimas_previsoes(conteudo_csv):
    ultimas = {}

    if not conteudo_csv.strip():
        return ultimas

    linhas = conteudo_csv.strip().split("\n")

    # ignora cabeçalho
    for linha in linhas[1:]:
        partes = linha.split(",")

        if len(partes) < 7:
            continue

        linha_bus = partes[0]
        parada = partes[2]
        sentido = partes[3]
        chegada = partes[5]

        chave = (linha_bus, parada, sentido)

        ultimas[chave] = chegada  # mantém sempre o último registro

    return ultimas


# -------- LAMBDA --------
def lambda_handler(event, context):

    print("Iniciando execução...")

    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))
    data_atual = agora.strftime("%Y-%m-%d")
    timestamp_atual = agora.strftime("%Y-%m-%d %H:%M:%S")

    nome_arquivo_s3 = f"resultado_previsoes_{data_atual}.csv"

    if not autenticar():
        print("Erro de autenticação")
        return {"status": "erro_autenticacao"}

    print("Autenticado com sucesso")

    dados_lidos = ler_dados_entrada()

    # ===== Verifica se arquivo já existe =====
    try:
        objeto = s3.get_object(Bucket=BUCKET_NAME, Key=nome_arquivo_s3)
        conteudo_existente = objeto["Body"].read().decode("utf-8")
        arquivo_existe = True
        print("Arquivo do dia já existe - será atualizado")
    except ClientError:
        conteudo_existente = ""
        arquivo_existe = False
        print("Arquivo do dia não existe - será criado")

    ultimas_previsoes = carregar_ultimas_previsoes(conteudo_existente)

    output = io.StringIO()
    writer = csv.writer(output)

    # Cabeçalho se for novo arquivo
    if not arquivo_existe:
        writer.writerow([
            "linha",
            "timestamp_consulta",
            "parada",
            "sentido",
            "onibus",
            "chegada_estimada",
            "acessivel"
        ])

    # ===== PROCESSAMENTO =====

    for item in dados_lidos:

        parada = item["parada"]
        sentido = item["sentido"]

        dados = previsao_por_parada(parada)

        if not dados or not dados.get("p"):
            continue

        linhas_api = dados.get("p", {}).get("l", [])

        for linha_api in linhas_api:

            letreiro = linha_api.get("c")
            veiculos = linha_api.get("vs", [])

            if not veiculos:
                continue

            # pega apenas o próximo ônibus
            proximo = veiculos[0]

            horario_estimado = proximo.get("t")

            chave = (letreiro, parada, sentido)
            ultimo_gravado = ultimas_previsoes.get(chave)

            # 🔥 só grava se mudou
            if horario_estimado != ultimo_gravado:

                writer.writerow([
                    letreiro,
                    timestamp_atual,
                    parada,
                    sentido,
                    proximo.get("p"),
                    horario_estimado,
                    proximo.get("a")
                ])

                ultimas_previsoes[chave] = horario_estimado

    # ===== Junta conteúdo antigo + novo =====
    conteudo_final = conteudo_existente + output.getvalue()

    # ===== Envia para S3 =====
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=nome_arquivo_s3,
        Body=conteudo_final,
        ContentType="text/csv"
    )

    print(f"Arquivo atualizado: {nome_arquivo_s3}")

    return {
        "status": "ok",
        "arquivo": nome_arquivo_s3
    }