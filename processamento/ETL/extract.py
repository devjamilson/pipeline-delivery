
class Extractor:
    def __init__(self, file_path, spark):
        self.file_path = file_path
        self.spark = spark
        self.data = None

    def load_data(self):
        try:
            self.data = self.spark.read.csv(self.file_path, header=True, inferSchema=True)
        except Exception as e:
            print(f"Erro ao carregar o arquivo: {e}")


if __name__ == "__main__":
    main()
