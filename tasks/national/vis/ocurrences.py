import pandas
from surnames_package import utils, isonymic, isonymic_vis
import matplotlib.pyplot as plt

PLOT_SIZE = 5
PLOT_MAX_COLUMNS = 2


def get_ocurrences_log_plot_2015(upstream, product):
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    # regiones y ncols pueden ser parametros.
    regiones = ["Patagonia", "NOA", "NEA", "Centro", "Cuyo", "Argentina"]
    ncols = PLOT_MAX_COLUMNS
    number_of_plots_is_odd = len(regiones) % ncols != 0
    nrows = len(regiones) // ncols
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

    for i, (regionName, ax) in enumerate(zip(regiones, axes)):
        if regionName == "Argentina":
            region_surnames_df = df
        else:
            region_surnames_df = df[df["region_nombre"] == regionName]
            region_surnames_df = region_surnames_df.reset_index(drop=True)

        its_last_item = i == len(regiones) - 1

        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            region_surnames_df["surname"],
            main_annotation=regionName,
            ax=ax,
            main_annotation_fontsize=(font_size + 8) if its_last_item else font_size,
            xmax=x_max_arg,
            ymax=y_max_arg,
        )

    if number_of_plots_is_odd:
        for ax_to_delete in range(len(regiones), nrows * ncols):
            axes[ax_to_delete].remove()

    plt.savefig(str(product))


def get_ocurrences_log_plot_2021(upstream, product):
    df = pandas.read_parquet(str(upstream["get-surnames-2021"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    # regiones y ncols pueden ser parametros.
    regiones = ["Patagonia", "NOA", "NEA", "Centro", "Cuyo", "Argentina"]
    ncols = PLOT_MAX_COLUMNS
    number_of_plots_is_odd = len(regiones) % ncols != 0
    nrows = len(regiones) // ncols
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

    for i, (regionName, ax) in enumerate(zip(regiones, axes)):
        if regionName == "Argentina":
            region_surnames_df = df
        else:
            region_surnames_df = df[df["region_nombre"] == regionName]
            region_surnames_df = region_surnames_df.reset_index(drop=True)

        # occur_vs_freq_df = isonymic.getOccurrencesVsFrequencies(
        #     region_surnames_df["surname"]
        # )
        its_last_item = i == len(regiones) - 1

        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            region_surnames_df["surname"],
            main_annotation=regionName,
            ax=ax,
            main_annotation_fontsize=(font_size + 8) if its_last_item else font_size,
            xmax=x_max_arg,
            ymax=y_max_arg,
        )

    if number_of_plots_is_odd:
        for ax_to_delete in range(len(regiones), nrows * ncols):
            axes[ax_to_delete].remove()

    plt.savefig(str(product))
