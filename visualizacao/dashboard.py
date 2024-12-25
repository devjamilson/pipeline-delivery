import streamlit as st
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd

spark = SparkSession.builder \
    .appName("Streamlit Spark Parquet Filter") \
    .getOrCreate()

st.title('Tabela Interativa com Filtro - Spark e Parquet')

uploaded_file = st.file_uploader("Faça upload de um arquivo Parquet", type=["parquet"])

if uploaded_file is not None:
    parquet_path = uploaded_file.name
    with open(parquet_path, "wb") as f:
        f.write(uploaded_file.getbuffer())

    df_spark = spark.read.parquet(parquet_path)

    st.write("Dados Carregados:")
    st.write(df_spark.show(5, truncate=False))  

    df_pandas = df_spark.limit(1000).toPandas()
    st.dataframe(df_pandas)

    column_to_filter = st.selectbox("Selecione uma coluna para filtrar:", df_spark.columns)

    filter_value = st.text_input(f"Digite o valor para filtrar na coluna '{column_to_filter}':")

    if filter_value:
        filtered_df_spark = df_spark.filter(col(column_to_filter).like(f"%{filter_value}%"))
        st.write(f"Resultados para '{filter_value}' na coluna '{column_to_filter}':")
        
        filtered_df_pandas = filtered_df_spark.limit(1000).toPandas()
        st.dataframe(filtered_df_pandas)
    else:
        st.write("Insira um valor para filtrar.")
