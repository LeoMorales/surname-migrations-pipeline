import pandas
from surnames_package import utils, models_utils

import matplotlib.pyplot as plt


def get_linear_regression_params_2015(upstream, product):
    """Retorna los parámetros resultantes de aplicar OLS a las variables X='occurrences_log' e y='frequency_log'.

    Args:
        upstream (_type_): _description_
        product (_type_): Dataframe con las columnas:
            intercept,
            slope,
            r_square,
            r_square_adj,
            f_score,
            division_level,
            division_name,
            division_id

    """
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="province_id")

    output_df = models_utils.compute_linear_regression_params_for_groups(
        df, division_id_column="province_id", division_name_column="provincia_nombre"
    )

    output_df["division_level"] = "province"
    output_df["year"] = "2015"
    output_df.to_parquet(str(product))


def get_linear_regression_params_2021(upstream, product):
    """Retorna los parámetros resultantes de aplicar OLS a las variables X='occurrences_log' e y='frequency_log'.

    Args:
        upstream (_type_): _description_
        product (_type_): Dataframe con las columnas:
            intercept,
            slope,
            r_square,
            r_square_adj,
            f_score,
            division_level,
            division_name,
            division_id

    """
    df = pandas.read_parquet(str(upstream["get-surnames-2021"]))
    df = utils.append_cell_description(df, departmentCodeColumn="province_id")

    output_df = models_utils.compute_linear_regression_params_for_groups(
        df, division_id_column="province_id", division_name_column="provincia_nombre"
    )

    output_df["division_level"] = "province"
    output_df["year"] = "2021"
    output_df.to_parquet(str(product))
