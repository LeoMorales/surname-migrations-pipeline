import pandas

# Crear una tarea raiz que no es mas que una copia local de los datos del padrón
# limpios y generados por otro pipelin


def get_surnames_2015(product):
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet"
    )

    df.to_parquet(str(product))
