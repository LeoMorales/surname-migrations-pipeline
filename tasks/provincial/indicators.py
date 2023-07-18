import pandas
from surnames_package import isonymic
from surnames_package import utils

FST_COLUMN_NAME = "fst"
DF_COLUMNS = ["division", "n", "ins", FST_COLUMN_NAME, "fishers_alpha", "A", "B"]


def get_provincial_isonymy_2015(product):
    """"""
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet"
    )
    df = utils.append_province_description(df)

    provincial_results = []
    for province_id, province_df in df.groupby("province_id"):
        province_isonymy = isonymic.get_isonymic_data(province_df["surname"])
        province_isonymy["division"] = province_id

        provincial_results.append(province_isonymy)

    provincial_df = pandas.DataFrame(provincial_results)
    provincial_df = provincial_df[DF_COLUMNS]

    provincial_df.to_parquet(str(product))


def get_provincial_isonymy_2021(product):
    """"""
    df = pandas.read_parquet(
        "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet"
    )

    provincial_results = []
    for province_id, province_df in df.groupby("province_id"):
        province_isonymy = isonymic.get_isonymic_data(province_df["surname"])
        province_isonymy["division"] = province_id

        provincial_results.append(province_isonymy)

    provincial_df = pandas.DataFrame(provincial_results)
    provincial_df = provincial_df[DF_COLUMNS]

    provincial_df.to_parquet(str(product))


def get_provincial_karlin_macgregor_2015(upstream, product):
    """
    Obtiene el valor de v a partir del dato isonímico
    de alfa de Fisher.
    """
    df = pandas.read_parquet(str(upstream["get-provincial-isonymy-2015"]))
    df = df.rename(columns={"division": "province_id"})
    output_df = df[["province_id"]].copy()
    output_df["v"] = isonymic.get_karlin_mcgregor_v_vect(
        serie_a=df["fishers_alpha"], serie_n=df["n"]
    )

    output_df.to_parquet(str(product))


def get_provincial_karlin_macgregor_2021(upstream, product):
    """
    Obtiene el valor de v a partir del dato isonímico
    de alfa de Fisher.
    """
    df = pandas.read_parquet(str(upstream["get-provincial-isonymy-2021"]))
    df = df.rename(columns={"division": "province_id"})
    output_df = df[["province_id"]].copy()
    output_df["v"] = isonymic.get_karlin_mcgregor_v_vect(
        serie_a=df["fishers_alpha"], serie_n=df["n"]
    )

    output_df.to_parquet(str(product))


def get_provincial_wright_2015(upstream, product):
    provincial_population_df = pandas.read_parquet(
        "/home/lmorales/resources/estimaciones-poblacion/poblacion-por-provincias-2015-y-2021.parquet"
    )
    provincial_isonymy_df = pandas.read_parquet(
        str(upstream["get-provincial-isonymy-2015"])
    )

    provincial_isonymy_df = provincial_isonymy_df.rename(
        columns={"division": "province_id"}
    )

    # del dataset de isonimia solo necesitamos el fst y el id de departamento:
    isonymy_df = provincial_isonymy_df[["province_id", FST_COLUMN_NAME]].copy()

    # del dataset de población, solo necesitamos la del 2015
    population_df = provincial_population_df[["provincia_id", "total_2015"]].copy()
    population_df = population_df.rename(
        columns={"total_2015": "population_2015", "provincia_id": "province_id"}
    )

    wright_df = pandas.merge(population_df, isonymy_df, on="province_id")

    wright_df["m"] = isonymic.get_wright_m_vect(
        wright_df["population_2015"], wright_df[FST_COLUMN_NAME]
    )

    wright_df = wright_df.drop(columns=[FST_COLUMN_NAME])
    wright_df.to_parquet(str(product))


def get_provincial_wright_2021(upstream, product):
    provincial_population_df = pandas.read_parquet(
        "/home/lmorales/resources/estimaciones-poblacion/poblacion-por-provincias-2015-y-2021.parquet"
    )
    provincial_isonymy_df = pandas.read_parquet(
        str(upstream["get-provincial-isonymy-2021"])
    )

    provincial_isonymy_df = provincial_isonymy_df.rename(
        columns={"division": "province_id"}
    )

    # del dataset de isonimia solo necesitamos el fst y el id de departamento:
    isonymy_df = provincial_isonymy_df[["province_id", FST_COLUMN_NAME]].copy()

    # del dataset de población, solo necesitamos la del 2021
    population_df = provincial_population_df[["provincia_id", "total_2021"]].copy()
    population_df = population_df.rename(
        columns={"total_2021": "population_2021", "provincia_id": "province_id"}
    )

    wright_df = pandas.merge(population_df, isonymy_df, on="province_id")

    wright_df["m"] = isonymic.get_wright_m_vect(
        wright_df["population_2021"], wright_df[FST_COLUMN_NAME]
    )

    wright_df = wright_df.drop(columns=[FST_COLUMN_NAME])
    wright_df.to_parquet(str(product))


def get_isonymy_and_migration_indicators_2015(upstream, product):
    """Combianción de las tres tareas anuales: isonimia, karlin macgregor y wright"""
    DIVISION_KEY = "province_id"

    data_v = pandas.read_parquet(str(upstream["get-provincial-karlin-macgregor-2015"]))
    data_m = pandas.read_parquet(str(upstream["get-provincial-wright-2015"])).rename(
        columns={"division": DIVISION_KEY}
    )
    isonymic_data = pandas.read_parquet(
        str(upstream["get-provincial-isonymy-2015"])
    ).rename(columns={"division": DIVISION_KEY})

    provincial_df = pandas.merge(
        pandas.merge(data_v, data_m, on=DIVISION_KEY),
        isonymic_data,
        on=DIVISION_KEY,
    )

    provincial_df.columns = [column.lower() for column in provincial_df.columns]
    col_order = [
        DIVISION_KEY,
        "n",
        "ins",
        FST_COLUMN_NAME,
        "fishers_alpha",
        "a",
        "b",
        "v",
        "m",
        "population_2015",
    ]

    provincial_df = provincial_df[col_order]
    provincial_df.to_parquet(str(product))


def get_isonymy_and_migration_indicators_2021(upstream, product):
    """Combianción de las tres tareas anuales: isonimia, karlin macgregor y wright"""
    DIVISION_KEY = "province_id"

    data_v = pandas.read_parquet(str(upstream["get-provincial-karlin-macgregor-2021"]))
    data_m = pandas.read_parquet(str(upstream["get-provincial-wright-2021"])).rename(
        columns={"division": DIVISION_KEY}
    )
    isonymic_data = pandas.read_parquet(
        str(upstream["get-provincial-isonymy-2021"])
    ).rename(columns={"division": DIVISION_KEY})

    provincial_df = pandas.merge(
        pandas.merge(data_v, data_m, on=DIVISION_KEY),
        isonymic_data,
        on=DIVISION_KEY,
    )

    provincial_df.columns = [column.lower() for column in provincial_df.columns]
    col_order = [
        DIVISION_KEY,
        "n",
        "ins",
        FST_COLUMN_NAME,
        "fishers_alpha",
        "a",
        "b",
        "v",
        "m",
        "population_2021",
    ]

    provincial_df = provincial_df[col_order]
    provincial_df.to_parquet(str(product))
