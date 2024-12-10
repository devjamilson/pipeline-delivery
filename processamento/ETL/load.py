import pandas as pd
from pyspark.sql import DataFrame

class Loader:
    def __init__(self, data):
        self.data = data

    def save_as_parquet(self, file_path):
        try:
            self.data.write.parquet(file_path, mode='overwrite')
            print(f"Dados salvos com sucesso em Parquet: {file_path}")
        except Exception as e:
            print(f"Erro ao salvar os dados como Parquet: {e}")

    def save_as_csv(self, file_path, header=True):
        try:
            self.data.write.csv(file_path, header=header, mode='overwrite')
            print(f"Dados salvos com sucesso em CSV: {file_path}")
        except Exception as e:
            print(f"Erro ao salvar os dados como CSV: {e}")

    def save_as_xlsx(self, file_path):
        try:
            pandas_df = self.data.toPandas()
            pandas_df.to_excel(file_path, index=False)
            print(f"Dados salvos com sucesso em XLSX: {file_path}")
        except Exception as e:
            print(f"Erro ao salvar os dados como XLSX: {e}")


