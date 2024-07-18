import pandas

# Crear una tarea raiz que no es mas que una copia local de los datos del padrón
# limpios y generados por otro pipelin


def get_surnames_2015(product):
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet"
    )
    # 06217 es Chascomus antes del 2011, así que homogeneizamos reescribiendo a 06218 (Chascomus actual, y el usado en la capa geográfica)
    df["department_id"] = df["department_id"].replace("06217", "06218")

    df.to_parquet(str(product))


def get_surnames_2021(product):
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet"
    )

    # 06217 es Chascomus antes del 2011, así que homogeneizamos reescribiendo a 06218 (Chascomus actual, y el usado en la capa geográfica)
    df["department_id"] = df["department_id"].str.replace("06217", "06218", regex=False)
    # corregir códigos de departamento de Tierra del Fuego
    df["department_id"] = df["department_id"].str.replace("94007", "94008", regex=False)
    df["department_id"] = df["department_id"].str.replace("94014", "94015", regex=False)

    df.to_parquet(str(product))


def get_origin_labels(product):
    labels_df = pandas.read_csv(
        "/home/lmorales/work/pipelines/ml_surnames_pipeline/_products/get/labels.csv"
    )

    labels_df.drop(columns=[labels_df.columns[0]])
    labels_df.to_parquet(str(product))
