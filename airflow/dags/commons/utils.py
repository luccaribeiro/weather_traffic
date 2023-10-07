import json
import pandas as pd


def save_raw_data(raw_data, date, name):
    raw_data_json = json.dumps(raw_data)
    file_name = f"raw_{name}_data-{date}.json"

    with open(file_name, "w") as arquivo:
        arquivo.write(raw_data_json)

    # Coloquei para salvar localmente pois não queria que dependesse de uma conta AWS
    # Porém normalmente esse arquivo estaria em um bucket no s3


def read_data(file_name):
    # Aqui seria o codigo de leitura no S3 ao inves de ler localmente
    try:
        with open(file_name, "r") as raw_data:
            data = json.load(raw_data)
        return data
    except FileNotFoundError:
        print(f"O arquivo '{file_name}' não foi encontrado.")
        return None
    except json.JSONDecodeError:
        print(
            f"Erro ao decodificar o arquivo JSON em '{file_name}'. Verifique a formatação."
        )
        return None


def normalize_dict_columns(df, dict_columns, prefix=False):
    for col_name in dict_columns:
        # Extrai o dicionário
        dict_data = df.pop(col_name)

        # Normaliza o dicionário
        df_join = pd.json_normalize(dict_data)

        # Adiciona prefixo se necessário
        if prefix:
            df_join = df_join.add_prefix(f"{col_name}_")

        # Junta o DataFrame original com o DataFrame normalizado
        df = pd.concat([df, df_join], axis=1)

    return df
