import os
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import shutil

# Configurações do banco
DB_USER = "postgres"
DB_PASS = "1131"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"

# Pastas de origem e destino
BASE_DIR_ATENDIMENTO = os.path.join(os.path.dirname(__file__), "source", "raw", "atendimento")
DEST_DIR_ATENDIMENTO = os.path.join(os.path.dirname(__file__), "source", "process", "atendimento-process")

BASE_DIR_ASSINANTE = os.path.join(os.path.dirname(__file__), "source", "raw", "assinante")
DEST_DIR_ASSINANTE = os.path.join(os.path.dirname(__file__), "source", "process", "assinante-process")

# Conexão com Postgres
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def ingest_atendimento():
    for file in os.listdir(BASE_DIR_ATENDIMENTO):
        if file.endswith(".csv"):
            file_path = os.path.join(BASE_DIR_ATENDIMENTO, file)
            print(f"Ingerindo arquivo: {file_path}")

            df = pd.read_csv(file_path)

            ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df["ingestion_time"] = ingestion_time

            df.to_sql("atendimento", engine, schema="dev", if_exists="append", index=False)

            new_name = f"{ingestion_time.replace('-', '').replace(':', '').replace(' ', '_')}_{file}"
            dest_path = os.path.join(DEST_DIR_ATENDIMENTO, new_name)
            shutil.move(file_path, dest_path)

            print(f"Arquivo {file} ingerido e movido para {dest_path}.")



def ingest_assinante():
    source_folder = "source/raw/assinante"
    processed_folder = "source/process/assinante-process"
    os.makedirs(processed_folder, exist_ok=True)

    for file_name in os.listdir(source_folder):
        if file_name.endswith(".xls"):
            file_path = os.path.join(source_folder, file_name)
            print(f"Ingerindo arquivo Excel: {file_path}")

            try:
                # Lê como HTML (já que o .xls é HTML disfarçado)
                dfs = pd.read_html(file_path, header=0)
                df = dfs[0]

                # Mapeamento das colunas
                column_mapping = {
                    "Série / Ender. Princ.": "serie_ender_princ",
                    "Tipo": "tipo",
                    "Modelo": "modelo",
                    "Material SAP": "material_sap",
                    "Item JDE": "item_jde",
                    "Estado": "estado",
                    "Local": "local",
                    "Tipo Local": "tipo_local",
                    "Perfil Local": "perfil_local",
                    "Contrato NETSMS": "contrato_netsms",
                    "Tipo Contrato UNO": "tipo_contrato_uno",
                    "Operação": "operacao",
                    "Empresa Material": "empresa_material",
                    "Tipo Mercadoria": "tipo_mercadoria",
                    "Reusos": "reusos",
                    "Data da Alteração": "data_alteracao",
                    "Responsável": "responsavel",
                }

                # Renomeia colunas conforme mapping
                df = df.rename(columns=column_mapping)

                # Mantém apenas as colunas esperadas
                expected_columns = list(column_mapping.values())
                df = df[expected_columns]

                # Extrai apenas a parte direita de "serie_ender_princ"
                df["serie_ender_princ"] = (
                    df["serie_ender_princ"]
                    .astype(str)
                    .str.split("/")
                    .str[-1]        # pega a parte à direita da barra
                    .str.strip()    # remove espaços extras
                    .str.split(" ") # divide pelo espaço
                    .str[0]         # pega só o primeiro "token"
                )

                # Limpa e converte coluna de data
                if "data_alteracao" in df.columns:
                    df["data_alteracao"] = (
                        df["data_alteracao"]
                        .astype(str)
                        .str.strip()
                        .str.replace(r"\s+", " ", regex=True)  # substitui múltiplos espaços por 1
                    )
                    df["data_alteracao"] = pd.to_datetime(
                        df["data_alteracao"],
                        errors="coerce",
                        dayfirst=True  # porque o formato é dd/mm/yyyy
                    )

                # Adiciona ingestion_time
                ingestion_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                df["ingestion_time"] = ingestion_time

                # Insere no Postgres
                df.to_sql("assinante", engine, schema="dev", if_exists="append", index=False)

                # Renomeia e move o arquivo
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                new_file_name = f"{timestamp}.xls"
                shutil.move(file_path, os.path.join(processed_folder, new_file_name))

                print(f"Arquivo {file_name} ingerido e movido para {processed_folder} como {new_file_name}")

            except Exception as e:
                print(f"Erro ao processar {file_name}: {e}")


if __name__ == "__main__":
    ingest_atendimento()
    ingest_assinante()
