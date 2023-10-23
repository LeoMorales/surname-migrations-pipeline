import pandas

# Crear una tarea raiz que no es mas que una copia local de los datos del padrón
# limpios y generados por otro pipelin


def get_surnames_2015(product):
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet"
    )

    df.to_parquet(str(product))


def get_surnames_2021(product):
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet"
    )

    df.to_parquet(str(product))


def get_origin_labels(product):
    labels_df = pandas.read_csv(
        "/home/lmorales/work/pipelines/ml_surnames_pipeline/_products/get/labels.csv"
    )

    labels_df.drop(columns=[labels_df.columns[0]])
    labels_df.to_parquet(str(product))
