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

        occur_vs_freq_df = isonymic.getOccurrencesVsFrequencies(
            region_surnames_df["surname"]
        )

        isonymic_vis.plotLogOcurrencesVsLogFrequencies(
            occur_vs_freq_df["occurrences_log"],
            occur_vs_freq_df["frecuency_log"],
            regionName,
            ax,
        )

        # Add text to the top-right position
        (
            _,
            surnames_with_minimal_frequency,
            max_frequencies,
            surnames_with_max_frequencies,
        ) = isonymic.getSurnameFrequencies2(region_surnames_df["surname"])

        surnames_with_minimal_frequency_n = len(surnames_with_minimal_frequency)

        selected_columns = ["frecuency_log", "occurrences_log"]
        y_min_freq, x_min_occur = (
            occur_vs_freq_df.sort_values(by="occurrences_log", ascending=True)
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

        # 5 apellidos, 5 ocurrencias (las maximas):
        for i, (occurence_i, surname_i) in enumerate(
            zip(max_frequencies, surnames_with_max_frequencies)
        ):
            # con la ocurrencia de un apellido, buscar la frecuencia de dicha ocurrencia en el dataset ya procesado.
            # serían las coordenadas en el plot
            f = occur_vs_freq_df.loc[
                occur_vs_freq_df["occurrences"] == occurence_i, "frecuency_log"
            ]
            y_max_freq_i = f[f.first_valid_index()]

            f = occur_vs_freq_df.loc[
                occur_vs_freq_df["occurrences"] == occurence_i, "occurrences_log"
            ]
            x_max_occur_i = f[f.first_valid_index()]

            ax.annotate(
                surname_i,
                xy=(x_max_occur_i, y_max_freq_i),
                xytext=(0.85 - (0.05 * i), 0.6 - (0.07 * i)),
                textcoords="axes fraction",
                fontsize=6,
                color="grey",
                arrowprops=dict(arrowstyle="->", color="lightgrey"),
            )

    plt.savefig(str(product))
