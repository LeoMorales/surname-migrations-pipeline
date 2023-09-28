import pandas
from surnames_package import utils, isonymic, isonymic_vis
import matplotlib.pyplot as plt


def get_ocurrences_log_plot_2015(upstream, product, region: str = "Patagonia"):
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    if region not in df["region_nombre"].unique():
        raise ValueError("La region indicada no se encuentra en el dataset")

    region_df = df[df["region_nombre"] == region]
    provinces_of_region = region_df["provincia_nombre"].unique()

    units = list(provinces_of_region) + [region]

    number_of_plots_is_odd = len(units) % 2 != 0
    ncols = 2
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
        if unit_name_i == region:
            province_surnames_df = region_df
        else:
            province_surnames_df = region_df[
                region_df["provincia_nombre"] == unit_name_i
            ]
            province_surnames_df = province_surnames_df.reset_index(drop=True)

        occur_vs_freq_df = isonymic.getOccurrencesVsFrequencies(
            province_surnames_df["surname"]
        )

        its_last_item = i == len(units) - 1
        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            occur_vs_freq_df["occurrences_log"],
            occur_vs_freq_df["frecuency_log"],
            unit_name_i[:16],
            ax,
            annotation_fontsize=(font_size + 5) if its_last_item else font_size,
        )

        # Add text to the top-right position
        (
            _,
            surnames_with_minimal_frequency,
            max_frequencies,
            surnames_with_max_frequencies,
        ) = isonymic.getSurnameFrequencies2(province_surnames_df["surname"])

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

        for i, (frequency_i, surname_i) in enumerate(
            zip(max_frequencies[:3], surnames_with_max_frequencies[:3])
        ):
            f = occur_vs_freq_df.loc[
                occur_vs_freq_df["occurrences"] == frequency_i, "frecuency_log"
            ]
            y_max_freq = f[f.first_valid_index()]

            f = occur_vs_freq_df.loc[
                occur_vs_freq_df["occurrences"] == frequency_i, "occurrences_log"
            ]
            x_max_occur = f[f.first_valid_index()]

            ax.annotate(
                surname_i,
                xy=(x_max_occur, y_max_freq),
                xytext=(0.8 - (0.1 * i), 0.4 - (0.1 * i)),
                textcoords="axes fraction",
                fontsize=8,
                color="grey",
                arrowprops=dict(arrowstyle="->", color="lightgrey"),
            )

    if number_of_plots_is_odd:
        axes[-1].remove()

    plt.savefig(str(product))
