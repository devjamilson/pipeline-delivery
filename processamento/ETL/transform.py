from pyspark.sql import DataFrame

class Transformer:
    def __init__(self, data):
        self.data = data
        
    def _handle_error(self, action_name: str, func):
        """
        Função genérica para tratar erros em operações.
        :param action_name: Nome da ação (por exemplo, 'renomear colunas')
        :param func: Função a ser executada
        """
        try:
            return func()
        except Exception as e:
            print(f"Erro ao {action_name}: {e}")

    def rename_columns(self, column_mapping):
        try:
            self.data = self.data.select([col(c).alias(column_mapping.get(c, c)) for c in self.data.columns])
        except Exception as e:
            print(f"Erro ao renomear as colunas: {e}")

    def filter_data(self, condition):
        try:
            self.data = self.data.filter(condition)
        except Exception as e:
            print(f"Erro ao filtrar os dados: {e}")

    def add_column(self, column_name, expression):

        try:
            self.data = self.data.withColumn(column_name, expression)
        except Exception as e:
            print(f"Erro ao adicionar a coluna: {e}")

    def show_data(self, num_rows=5):
        try:
            self.data.show(num_rows)
        except Exception as e:
            print(f"Erro ao exibir os dados: {e}")

    def join(self, *dataframes: DataFrame, on_column: str, how: str = "inner"):
        """
        Realiza um JOIN entre as tabelas com base em um atributo de coluna.
        :param dataframes: DataFrames a serem unidos.
        :param on_column: A coluna pela qual as tabelas devem ser unidas.
        :param how: Tipo de join ("inner", "left", "right", "outer", etc.)
        """
        def join_logic():
            for df in dataframes:
                self.data = self.data.join(df, on=on_column, how=how)
            print(f"{len(dataframes) + 1} tabelas unidas com sucesso utilizando a coluna '{on_column}'.")

        self._handle_error("unir as tabelas", join_logic)
