import pandas
from surnames_package import utils, isonymic, isonymic_vis
import matplotlib.pyplot as plt


def get_ocurrences_log_plot_2015(upstream, product):
    df = pandas.read_parquet(str(upstream["get-surnames-2015"]))
    df = utils.append_cell_description(df, departmentCodeColumn="department_id")

    regiones = ["Patagonia", "NOA", "NEA", "Centro", "Cuyo", "Argentina"]

    f, axs = plt.subplots(
        nrows=3,
        ncols=2,
        figsize=(14, 14),
        sharex=True,
        sharey=True,
        constrained_layout=True,
    )

    axes = axs.flatten()

    for regionName, ax in zip(regiones, axes):
        if regionName == "Argentina":
            region_surnames_df = df
        else:
            region_surnames_df = df[df["region_nombre"] == regionName]
            region_surnames_df = region_surnames_df.reset_index(drop=True)

        ocurr_vs_freq_df = isonymic.getOccurrencesVsFrequencies(
            region_surnames_df["surname"]
        )

        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            ocurr_vs_freq_df["occurrences_log"],
            ocurr_vs_freq_df["frecuency_log"],
            regionName,
            ax,
        )

        # Add text to the top-right position
        (
            _,
            surnames_with_minimal_frequency,
            _,
            surnames_with_maximal_frequency,
        ) = isonymic.getSurnameFrequencies(region_surnames_df["surname"])

        surnames_with_minimal_frequency_n = len(surnames_with_minimal_frequency)
        surnames_with_maximal_frequency_n = len(surnames_with_maximal_frequency)

        selected_columns = ["frecuency_log", "occurrences_log"]
        y_min_freq, x_min_occur = (
            ocurr_vs_freq_df.sort_values(by="occurrences_log", ascending=True)
            .reset_index(drop=True)
            .loc[0, selected_columns]
            .to_dict()
            .values()
        )

        y_max_freq, x_max_occur = (
            ocurr_vs_freq_df.sort_values(by="occurrences_log", ascending=False)
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

        max_annotation = (
            f"{surnames_with_maximal_frequency_n:,} surnames with max frecuency"
        )
        if surnames_with_maximal_frequency_n < 2:
            # si es un solo apellido con el max de portadores, mostrarlo
            max_annotation = f", ".join(surnames_with_maximal_frequency)

        ax.annotate(
            max_annotation,
            xy=(x_max_occur, y_max_freq),
            xytext=(0.8, 0.2),
            textcoords="axes fraction",
            fontsize=10,
            color="grey",
            arrowprops=dict(arrowstyle="->", color="lightgrey"),
        )

    plt.savefig(str(product))
