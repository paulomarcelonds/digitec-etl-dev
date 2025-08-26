import os
import shutil
import pandas as pd
import psycopg2

# Configuração da conexão PostgreSQL
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "1131",
    "host": "localhost",
    "port": "5432"
}


def ingestao_csv(tabela, pasta_entrada, pasta_saida):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for arquivo in os.listdir(pasta_entrada):
        if arquivo.endswith(".csv"):
            caminho = os.path.join(pasta_entrada, arquivo)
            df = pd.read_csv(caminho)

            for _, row in df.iterrows():
                cols = list(df.columns)
                valores = [row[c] for c in cols]
                placeholders = ",".join(["%s"] * len(cols))
                query = f'INSERT INTO {tabela} ({",".join(cols)}) VALUES ({placeholders})'
                cur.execute(query, valores)

            conn.commit()
            shutil.move(caminho, os.path.join(pasta_saida, arquivo))
            print(f"{arquivo} carregado em {tabela} ✅")

    cur.close()
    conn.close()


def ingestao_excel(tabela, pasta_entrada, pasta_saida):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for arquivo in os.listdir(pasta_entrada):
        if arquivo.endswith(".xlsx") or arquivo.endswith(".xls"):
            caminho = os.path.join(pasta_entrada, arquivo)
            df = pd.read_excel(caminho)

            for _, row in df.iterrows():
                cols = list(df.columns)
                valores = [row[c] for c in cols]
                placeholders = ",".join(["%s"] * len(cols))
                query = f'INSERT INTO {tabela} ({",".join(cols)}) VALUES ({placeholders})'
                cur.execute(query, valores)

            conn.commit()
            shutil.move(caminho, os.path.join(pasta_saida, arquivo))
            print(f"{arquivo} carregado em {tabela} ✅")

    cur.close()
    conn.close()


# Chamadas para cada tabela
def ingestao_atendimento():
    ingestao_csv("atendimento",
        r"C:\workspace\digitec-etl-dev\source\atendimento",
        r"C:\workspace\digitec-etl-dev\source\atendimento-process")


def ingestao_atlas():
    ingestao_excel("atlas",
        r"C:\workspace\digitec-etl-dev\source\atendimento-process",
        r"C:\workspace\digitec-etl-dev\source\atlas-process")


def ingestao_assinante():
    ingestao_excel("assinante",
        r"C:\workspace\digitec-etl-dev\source\assinante",
        r"C:\workspace\digitec-etl-dev\source\assinante-process")


def ingestao_consolidado():
    ingestao_excel("consolidado",
        r"C:\workspace\digitec-etl-dev\source\consolidado",
        r"C:\workspace\digitec-etl-dev\source\consolidado-process")


def ingestao_servicos():
    ingestao_excel("servicos",
        r"C:\workspace\digitec-etl-dev\source\servico",
        r"C:\workspace\digitec-etl-dev\source\servico-process")


if __name__ == "__main__":
    ingestao_atendimento()
    ingestao_atlas()
    ingestao_assinante()
    ingestao_consolidado()
    ingestao_servicos()
