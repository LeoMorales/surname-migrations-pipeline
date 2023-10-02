import pandas
from surnames_package import utils, isonymic, isonymic_vis
import matplotlib.pyplot as plt


def get_ocurrences_log_plot_2015(upstream, product, province: str = "Chubut"):
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    if province not in df["provincia_nombre"].unique():
        raise ValueError("La province indicada no se encuentra en el dataset")

    province_df = df[df["provincia_nombre"] == province]
    departments_of_province = province_df["departamento_nombre"].unique()

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

    font_size = 12
    for i, (unit_name_i, ax) in enumerate(zip(units, axes)):
        if unit_name_i == province:
            department_surnames_df = province_df
        else:
            department_surnames_df = province_df[
                province_df["departamento_nombre"] == unit_name_i
            ]
            department_surnames_df = department_surnames_df.reset_index(drop=True)

        occur_vs_freq_df = isonymic.getOccurrencesVsFrequencies(
            department_surnames_df["surname"]
        )

        s_value = len(department_surnames_df["surname"].unique())
        n_value = len(department_surnames_df)

        its_last_item = i == len(units) - 1
        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            occur_vs_freq_df["occurrences_log"],
            occur_vs_freq_df["frecuency_log"],
            unit_name_i[:16],
            ax,
            main_annotation_fontsize=(font_size + 8) if its_last_item else font_size,
            subtitle_annotation=f"n = {n_value:,}\ns = {s_value:,}",
        )

        # Add text to the top-right position
        (
            _,
            surnames_with_minimal_frequency,
            max_frequencies,
            surnames_with_max_frequencies,
        ) = isonymic.getSurnameFrequencies2(department_surnames_df["surname"])

        surnames_with_minimal_frequency_n = len(surnames_with_minimal_frequency)

        selected_columns = ["frecuency_log", "occurrences_log"]
        y_min_freq, x_min_occur = (
            occur_vs_freq_df.sort_values(by="occurrences_log", ascending=True)
            .reset_index(drop=True)
            .loc[0, selected_columns]
            .to_dict()
            .values()
        )

        y_max_freq, x_max_occur = (
            occur_vs_freq_df.sort_values(by="occurrences_log", ascending=False)
            .reset_index(drop=True)
            .loc[0, selected_columns]
            .to_dict()
            .values()
        )

        ax.annotate(
            f"{surnames_with_minimal_frequency_n:,} surnames with a single bearer",
            xy=(x_min_occur, y_min_freq),
            xytext=(0.1, 0.9),
            textcoords="axes fraction",
            fontsize=10,
            color="grey",
            arrowprops=dict(arrowstyle="->", color="lightgrey"),
        )

        for i, (occurrence_i, surname_i) in enumerate(
            zip(max_frequencies[:3], surnames_with_max_frequencies[:3])
        ):
            f = occur_vs_freq_df.loc[
                occur_vs_freq_df["occurrences"] == occurrence_i, "frecuency_log"
            ]
            y_max_freq = f[f.first_valid_index()]

            f = occur_vs_freq_df.loc[
                occur_vs_freq_df["occurrences"] == occurrence_i, "occurrences_log"
            ]
            x_max_occur = f[f.first_valid_index()]

            ax.annotate(
                f"{surname_i} ({occurrence_i})",
                xy=(x_max_occur, y_max_freq),
                xytext=(0.85 - (0.1 * i), 0.25 + (0.045 * i)),
                textcoords="axes fraction",
                fontsize=6,
                color="grey",
                horizontalalignment="left",
                verticalalignment="center_baseline",
                arrowprops=dict(
                    arrowstyle="->",
                    color="lightgrey",
                    connectionstyle="arc3,rad=-0.2",
                ),
            )

    if number_of_plots_is_odd:
        for ax_to_delete in range(len(units), nrows * ncols):
            axes[ax_to_delete].remove()

    plt.savefig(str(product))
