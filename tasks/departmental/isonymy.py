import pandas
from surnames_package import isonymic
from surnames_package import utils

DF_COLUMNS = ["division", "n", "ins", "fst", "fishers_alpha", "A", "B"]


def get_departmental_isonymy_2015(upstream, product):
    """"""
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    # df = utils.append_department_description(df)

    departamental_results = []

    for departament_id, department_df in df.groupby("department_id"):
        department_isonymy = isonymic.get_isonymic_data(department_df["surname"])
        department_isonymy["division"] = departament_id

        departamental_results.append(department_isonymy)

    departamental_df = pandas.DataFrame(departamental_results)
    departamental_df = departamental_df[DF_COLUMNS]

    departamental_df.to_parquet(str(product))


def get_departmental_isonymy_2021(upstream, product):
    """"""
    df = pandas.read_parquet(str(upstream["get-surnames-2021"]))

    departamental_results = []

    for departament_id, department_df in df.groupby("department_id"):
        department_isonymy = isonymic.get_isonymic_data(department_df["surname"])
        department_isonymy["division"] = departament_id

        departamental_results.append(department_isonymy)

    departamental_df = pandas.DataFrame(departamental_results)
    departamental_df = departamental_df[DF_COLUMNS]

    departamental_df.to_parquet(str(product))


def get_departmental_bearers_rankings_2015(upstream, product):
    """Obtiene los rankings de apellidos con mas cantidad de portadores y el ranking (top1) de los apellidos con un solo portador.

    Args:
        upstream (_type_): _description_
        product (_type_): _description_
    """
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    highest_number_of_bearers_dfs_list = []
    only_one_bearer_dfs_list = []

    for department_id, department_df in df.groupby("department_id"):
        (
            highest_number_of_bearers,
            only_one_bearer,
        ) = isonymic.getSurnamesWithTheHighestNumberOfBearersAndThoseWithOnlyOneBearer(
            department_df["surname"],
            division_level="department",
            division_id=department_id,
        )

        highest_number_of_bearers["year"] = "2015"
        only_one_bearer["year"] = "2015"

        highest_number_of_bearers_dfs_list.append(highest_number_of_bearers)
        only_one_bearer_dfs_list.append(only_one_bearer)

    departmental_highest_number_of_bearers = pandas.concat(
        highest_number_of_bearers_dfs_list
    ).reset_index(drop=True)
    departmental_only_one_bearer_df = pandas.concat(
        only_one_bearer_dfs_list
    ).reset_index(drop=True)

    departmental_highest_number_of_bearers.to_parquet(
        str(product["surnames-with-highest-number-of-bearers"])
    )
    departmental_only_one_bearer_df.to_parquet(
        str(product["surnames-with-only-one-bearer"])
    )


def get_departmental_bearers_rankings_2021(upstream, product):
    """Obtiene los rankings de apellidos con mas cantidad de portadores y el ranking (top1) de los apellidos con un solo portador.

    Args:
        upstream (_type_): _description_
        product (_type_): _description_
    """
    df = pandas.read_parquet(str(upstream["get-surnames-2021"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    highest_number_of_bearers_dfs_list = []
    only_one_bearer_dfs_list = []

    for department_id, department_df in df.groupby("department_id"):
        (
            highest_number_of_bearers,
            only_one_bearer,
        ) = isonymic.getSurnamesWithTheHighestNumberOfBearersAndThoseWithOnlyOneBearer(
            department_df["surname"],
            division_level="department",
            division_id=department_id,
        )

        highest_number_of_bearers["year"] = "2021"
        only_one_bearer["year"] = "2021"

        highest_number_of_bearers_dfs_list.append(highest_number_of_bearers)
        only_one_bearer_dfs_list.append(only_one_bearer)

    departmental_highest_number_of_bearers = pandas.concat(
        highest_number_of_bearers_dfs_list
    ).reset_index(drop=True)
    departmental_only_one_bearer_df = pandas.concat(
        only_one_bearer_dfs_list
    ).reset_index(drop=True)

    departmental_highest_number_of_bearers.to_parquet(
        str(product["surnames-with-highest-number-of-bearers"])
    )
    departmental_only_one_bearer_df.to_parquet(
        str(product["surnames-with-only-one-bearer"])
    )
