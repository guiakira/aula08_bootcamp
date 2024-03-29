import pandas as pd
import os
import glob

# Uma funcao de extract que le e consolida no json

def extrair_dados_e_consolidar(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json'))
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json]
    df_total = pd.concat(df_list,ignore_index = True)
    return df_total


# Uma funcao que transforma

def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    return df

# Uma funcao que da load em csv ou parquet

def carregar_dados(df: pd.DataFrame, format_saida: list):
    """
    parametro que vai ser "csv" ou "parquet" ou os dois
    """
    for formato in format_saida:
        if formato == 'csv':
            df.to_csv("dados.csv")
        if formato == 'parquet':
            df.to_csv("dados.parquet")


def pipeline_calcular_kpi_de_vendas_consolidadas(pasta: str, formato_de_saida: list):
    
    data_frame = extrair_dados_e_consolidar(pasta)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
 
    carregar_dados(data_frame_calculado,formato_de_saida)

#Toda que vez que for realizar algum teste, crie o comando abaixo para não atrapalhar o código no futuro:
#Outra coisa, crie um arquivo "teste_etl.py" e coloque o comando abaixo:
