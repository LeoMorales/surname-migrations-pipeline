import pandas
from surnames_package import utils, isonymic, isonymic_vis
import matplotlib.pyplot as plt

PLOT_SIZE = 6
PLOT_MAX_COLUMNS = 2


def get_ocurrences_log_plot_2015(upstream, product, region: str = "Patagonia"):
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    if region not in df["region_nombre"].unique():
        raise ValueError("La region indicada no se encuentra en el dataset")

    region_df = df[df["region_nombre"] == region]
    provinces_of_region = region_df["provincia_nombre"].unique()

    units = list(provinces_of_region) + [region]

    number_of_plots_is_odd = len(units) % 2 != 0
    ncols = PLOT_MAX_COLUMNS
    nrows = len(units) // ncols
    nrows = nrows + 1 if number_of_plots_is_odd else nrows

    plot_size = PLOT_SIZE
    f, axs = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(plot_size * ncols, plot_size * nrows),
        sharex=True,
        sharey=True,
        constrained_layout=True,
    )

    axes = axs.flatten()
    x_max_arg = 14
    y_max_arg = 12
    font_size = 12

    for i, (unit_name_i, ax) in enumerate(zip(units, axes)):
        if unit_name_i == region:
            province_surnames_df = region_df
        else:
            province_surnames_df = region_df[
                region_df["provincia_nombre"] == unit_name_i
            ]
            province_surnames_df = province_surnames_df.reset_index(drop=True)

        its_last_item = i == len(units) - 1

        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            province_surnames_df["surname"],
            main_annotation=unit_name_i,
            ax=ax,
            main_annotation_fontsize=(font_size + 8) if its_last_item else font_size,
            xmax=x_max_arg,
            ymax=y_max_arg,
        )

    if number_of_plots_is_odd:
        for ax_to_delete in range(len(units), nrows * ncols):
            axes[ax_to_delete].remove()

    plt.savefig(str(product))


def get_ocurrences_log_plot_2021(upstream, product, region: str = "Patagonia"):
    df = pandas.read_parquet(str(upstream["get-surnames-2021"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    if region not in df["region_nombre"].unique():
        raise ValueError("La region indicada no se encuentra en el dataset")

    region_df = df[df["region_nombre"] == region]
    provinces_of_region = region_df["provincia_nombre"].unique()

    units = list(provinces_of_region) + [region]

    number_of_plots_is_odd = len(units) % 2 != 0
    ncols = PLOT_MAX_COLUMNS
    nrows = len(units) // ncols
    nrows = nrows + 1 if number_of_plots_is_odd else nrows

    plot_size = PLOT_SIZE
    f, axs = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(plot_size * ncols, plot_size * nrows),
        sharex=True,
        sharey=True,
        constrained_layout=True,
    )

    axes = axs.flatten()
    x_max_arg = 14
    y_max_arg = 12
    font_size = 12

    for i, (unit_name_i, ax) in enumerate(zip(units, axes)):
        if unit_name_i == region:
            province_surnames_df = region_df
        else:
            province_surnames_df = region_df[
                region_df["provincia_nombre"] == unit_name_i
            ]
            province_surnames_df = province_surnames_df.reset_index(drop=True)

        its_last_item = i == len(units) - 1

        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            province_surnames_df["surname"],
            main_annotation=unit_name_i,
            ax=ax,
            main_annotation_fontsize=(font_size + 8) if its_last_item else font_size,
            xmax=x_max_arg,
            ymax=y_max_arg,
        )

    if number_of_plots_is_odd:
        for ax_to_delete in range(len(units), nrows * ncols):
            axes[ax_to_delete].remove()

    plt.savefig(str(product))
