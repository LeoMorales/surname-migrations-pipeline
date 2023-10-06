import pandas
from surnames_package import utils, isonymic, isonymic_vis
import matplotlib.pyplot as plt


def get_ocurrences_log_plot_2015(upstream, product, province: str = "Chubut"):
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    if province not in df["provincia_nombre"].unique():
        raise ValueError("La province indicada no se encuentra en el dataset")

    province_df = df[df["provincia_nombre"] == province]
    departments_of_province = sorted(province_df["departamento_nombre"].unique())

    units = list(departments_of_province) + [province]

    ncols = 3
    number_of_plots_is_odd = len(units) % ncols != 0
    nrows = len(units) // ncols
    nrows = nrows + 1 if number_of_plots_is_odd else nrows

    plot_size = 5
    f, axs = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(plot_size * ncols, plot_size * nrows),
        sharex=True,
        sharey=True,
        constrained_layout=True,
    )
    axes = axs.flatten()
    x_max_arg = 13
    y_max_arg = 12

    font_size = 12
    for i, (unit_name_i, ax) in enumerate(zip(units, axes)):
        if unit_name_i == province:
            department_surnames_df = province_df
        else:
            department_surnames_df = province_df[
                province_df["departamento_nombre"] == unit_name_i
            ]
            department_surnames_df = department_surnames_df.reset_index(drop=True)

        its_last_item = i == len(units) - 1
        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            department_surnames_df["surname"],
            main_annotation=unit_name_i[:16],
            ax=ax,
            main_annotation_fontsize=(font_size + 8) if its_last_item else font_size,
            xmax=x_max_arg,
            ymax=y_max_arg,
        )

    if number_of_plots_is_odd:
        for ax_to_delete in range(len(units), nrows * ncols):
            axes[ax_to_delete].remove()

    plt.suptitle("2015")
    plt.savefig(str(product))
    plt.close()


def get_ocurrences_log_plot_2021(upstream, product, province: str = "Chubut"):
    df = pandas.read_parquet(str(upstream["get-surnames-2021"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    if province not in df["provincia_nombre"].unique():
        raise ValueError("La province indicada no se encuentra en el dataset")

    province_df = df[df["provincia_nombre"] == province]
    departments_of_province = sorted(province_df["departamento_nombre"].unique())

    units = list(departments_of_province) + [province]

    ncols = 3
    number_of_plots_is_odd = len(units) % ncols != 0
    nrows = len(units) // ncols
    nrows = nrows + 1 if number_of_plots_is_odd else nrows

    plot_size = 5
    f, axs = plt.subplots(
        nrows=nrows,
        ncols=ncols,
        figsize=(plot_size * ncols, plot_size * nrows),
        sharex=True,
        sharey=True,
        constrained_layout=True,
    )

    axes = axs.flatten()
    x_max_arg = 13
    y_max_arg = 12

    font_size = 12
    for i, (unit_name_i, ax) in enumerate(zip(units, axes)):
        if unit_name_i == province:
            department_surnames_df = province_df
        else:
            department_surnames_df = province_df[
                province_df["departamento_nombre"] == unit_name_i
            ]
            department_surnames_df = department_surnames_df.reset_index(drop=True)

        its_last_item = i == len(units) - 1
        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            department_surnames_df["surname"],
            main_annotation=unit_name_i[:16],
            ax=ax,
            main_annotation_fontsize=(font_size + 8) if its_last_item else font_size,
            xmax=x_max_arg,
            ymax=y_max_arg,
        )

    if number_of_plots_is_odd:
        for ax_to_delete in range(len(units), nrows * ncols):
            axes[ax_to_delete].remove()

    plt.suptitle("2021")
    plt.savefig(str(product))
    plt.close()
