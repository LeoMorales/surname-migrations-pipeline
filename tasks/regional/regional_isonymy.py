import pandas
from surnames_package import isonymic
from surnames_package import utils

DF_COLUMNS = ["division", "n", "ins", "fst", "fishers_alpha", "A", "B"]


def get_regional_isonymy_2015(upstream, product):
    """"""
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))

    df = utils.append_province_description(df)
    regional_results = []

    for region_name, region_df in df.groupby("region_nombre"):
        region_isonymy = isonymic.get_isonymic_data(region_df["surname"])
        region_isonymy["division"] = region_name
        regional_results.append(region_isonymy)

    regional_df = pandas.DataFrame(regional_results)
    regional_df = regional_df[DF_COLUMNS]

    regional_df.to_parquet(str(product))


def get_regional_bearers_rankings_2015(upstream, product):
    """Obtiene los rankings de apellidos con mas cantidad de portadores y el ranking (top1) de los apellidos con un solo portador.

    Args:
        upstream (_type_): _description_
        product (_type_): _description_
    """
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    highest_number_of_bearers_dfs_list = []
    only_one_bearer_dfs_list = []
    for region_name, region_df in df.groupby("region_nombre"):
        (
            highest_number_of_bearers,
            only_one_bearer,
        ) = isonymic.getSurnamesWithTheHighestNumberOfBearersAndThoseWithOnlyOneBearer(
            region_df["surname"], division_level="region", division_id=region_name
        )

        highest_number_of_bearers["year"] = "2015"
        only_one_bearer["year"] = "2015"

        highest_number_of_bearers_dfs_list.append(highest_number_of_bearers)
        only_one_bearer_dfs_list.append(only_one_bearer)

    regional_highest_number_of_bearers = pandas.concat(
        highest_number_of_bearers_dfs_list
    ).reset_index(drop=True)
    regional_only_one_bearer_df = pandas.concat(only_one_bearer_dfs_list).reset_index(
        drop=True
    )

    regional_highest_number_of_bearers.to_parquet(
        str(product["surnames-with-highest-number-of-bearers"])
    )
    regional_only_one_bearer_df.to_parquet(
        str(product["surnames-with-only-one-bearer"])
    )


def get_regional_bearers_rankings_2021(upstream, product):
    """Obtiene los rankings de apellidos con mas cantidad de portadores y el ranking (top1) de los apellidos con un solo portador.

    Args:
        upstream (_type_): _description_
        product (_type_): _description_
    """
    df = pandas.read_parquet(str(upstream["get-surnames-2021"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    highest_number_of_bearers_dfs_list = []
    only_one_bearer_dfs_list = []
    for region_name, region_df in df.groupby("region_nombre"):
        (
            highest_number_of_bearers,
            only_one_bearer,
        ) = isonymic.getSurnamesWithTheHighestNumberOfBearersAndThoseWithOnlyOneBearer(
            region_df["surname"], division_level="region", division_id=region_name
        )

        highest_number_of_bearers["year"] = "2021"
        only_one_bearer["year"] = "2021"

        highest_number_of_bearers_dfs_list.append(highest_number_of_bearers)
        only_one_bearer_dfs_list.append(only_one_bearer)

    regional_highest_number_of_bearers = pandas.concat(
        highest_number_of_bearers_dfs_list
    ).reset_index(drop=True)

    regional_only_one_bearer_df = pandas.concat(only_one_bearer_dfs_list).reset_index(
        drop=True
    )

    regional_highest_number_of_bearers.to_parquet(
        str(product["surnames-with-highest-number-of-bearers"])
    )
    regional_only_one_bearer_df.to_parquet(
        str(product["surnames-with-only-one-bearer"])
    )
