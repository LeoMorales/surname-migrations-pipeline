import pandas
from surnames_package import utils, isonymic, isonymic_vis, models
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
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    regions = (
        df[["department_id", "departamento_nombre"]]
        .drop_duplicates()
        .to_records(index=False)
    )
    param_list = []

    for department_id, department_name_i in regions:
        department_df = (
            df[df["departamento_nombre"] == department_name_i]
            .reset_index(drop=True)
            .copy()
        )

        occur_vs_freq_df = isonymic.getOccurrencesVsFrequencies(
            department_df["surname"]
        )

        params_for_region_i = models.get_linear_regression_params(
            X=occur_vs_freq_df["occurrences_log"], y=occur_vs_freq_df["frecuency_log"]
        )

        params_for_region_i["division_level"] = "department"
        params_for_region_i["division_name"] = department_name_i
        params_for_region_i["division_id"] = department_id

        param_list.append(params_for_region_i)

    output_df = pandas.DataFrame(param_list)
    output_df.to_parquet(str(product))
