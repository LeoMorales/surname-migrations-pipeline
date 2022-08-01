# Utilizar las distancias de apellidos y las distancias en metros.

# +
import pandas
import itertools
import matplotlib.pyplot as plt
import numpy
import math


# Unique Combination Pairs for list of elements
def uniqueCombinations(list_elements):
    l = list(itertools.combinations(list_elements, 2))
    s = set(l)
    # print('actual', len(l), l)
    return sorted(list(s))


# -

def get_scatterplot_gradient(matrix_xs, matrix_ys, matrix_cat,
         xlabel="Distancia Nei (Log)",
         ylabel="Distancia en metros (Log)"
    ):
    """ Dibuja el scatter plot con el gradiente de
    colores segun la distancia que separa cada punto.
    """
    
    common_ids = list(
        set(matrix_xs.columns.unique().values) &
        set(matrix_ys.columns.unique().values) &
        set(matrix_cat.columns.unique().values)
    )

    df_xs = matrix_xs.loc[common_ids, common_ids]
    df_ys = matrix_ys.loc[common_ids, common_ids]
    df_cats = matrix_cat.loc[common_ids, common_ids]

    x = []
    y = []
    categories = []
    for item_a, item_b in uniqueCombinations(df_xs.columns.values):
        x.append(math.log(float(df_xs.loc[item_a, item_b])))
        y.append(math.log(float(df_ys.loc[item_a, item_b])))
        #categories.append(0 if item_a[:2] == item_b[:2] else 1)
        categories.append(float(df_cats.loc[item_a, item_b]))

    categories = numpy.array(categories)
    #pandas.DataFrame(categories).value_counts()

    colormap = numpy.array(["b", "orange"])

    figure, axes = plt.subplots(figsize=(12, 12))
    #plt.scatter(x, y, alpha=0.2, c=colormap[categories])
    plt.scatter(x, y, alpha=0.2, c=categories, cmap='viridis')
    axes.set_xlabel(xlabel)
    axes.set_ylabel(ylabel)
    plt.colorbar()
    plt.show();


df_meters = pandas.read_parquet("../_products/distance_matrices/dapartamental_in_meters.parquet")
df_nei = pandas.read_parquet("../_products/distance_matrices/dapartamental_nei.parquet")
df_lasker = pandas.read_parquet("../_products/distance_matrices/dapartamental_lasker.parquet")
df_euclidean = pandas.read_parquet("../_products/distance_matrices/dapartamental_euclidean.parquet")
df_isonymic = pandas.read_parquet("../_products/distance_matrices/dapartamental_isonymic.parquet")

get_scatterplot_gradient(
    df_euclidean, df_isonymic, df_meters,
    xlabel="Distancia Euclidea (Log)",
    ylabel="Distancia Isonimica (Log)"
)

get_scatterplot_gradient(
    df_euclidean, df_nei, df_meters,
    xlabel="Distancia Euclidea (Log)",
    ylabel="Distancia Nei (Log)"
)

get_scatterplot_gradient(
    df_euclidean, df_lasker, df_meters,
    xlabel="Distancia Euclidea (Log)",
    ylabel="Distancia Lasker (Log)"
)

get_scatterplot_gradient(
    df_isonymic, df_lasker, df_meters,
    xlabel="Distancia Isonimica (Log)",
    ylabel="Distancia Lasker (Log)"
)

get_scatterplot_gradient(
    df_isonymic, df_nei, df_meters,
    xlabel="Distancia Isonimica (Log)",
    ylabel="Distancia Nei (Log)"
)

# +
import seaborn

def get_scatterplot_categorical(
        matrix_xs, matrix_ys,
        xlabel="Distancia Nei (Log)",
        ylabel="Distancia en metros (Log)"
    ):
    common_ids = list(set(matrix_xs.columns.unique().values) & set(matrix_ys.columns.unique().values))

    df_xs = matrix_xs.loc[common_ids, common_ids]
    df_ys = matrix_ys.loc[common_ids, common_ids]

    x = []
    y = []
    categories = []
    for item_a, item_b in uniqueCombinations(df_xs.columns.values):
        x.append(float(df_xs.loc[item_a, item_b]))
        y.append(float(df_ys.loc[item_a, item_b]))
        categories.append("Misma provincia" if item_a[:2] == item_b[:2] else "Distinta provincia")

    categories = numpy.array(categories)
    #pandas.DataFrame(categories).value_counts()
    plt_df = pandas.DataFrame({
        xlabel: x,
        ylabel: y,
        'Category': categories
    })

    color_dict = dict({
        'Misma provincia':'blue',
        'Distinta provincia':'orange',
    })
    figure, axes = plt.subplots(figsize=(12, 12))

    g = seaborn.scatterplot(
        x=xlabel, y=ylabel, hue="Category",
        data=plt_df,
        alpha=.2,
        palette=color_dict,
        legend='full'
    )
    g.set(xscale="log")
    g.set(yscale="log")
    plt.show()
    ;


# -

get_scatterplot_categorical(
    df_euclidean, df_lasker,
    xlabel="Distancia Euclidea (Log)", ylabel="Distancia Lasker (Log)"
)

get_scatterplot_categorical(
    df_euclidean, df_nei,
    xlabel="Distancia Euclidea (Log)", ylabel="Distancia Nei (Log)"
)

get_scatterplot_categorical(
    df_euclidean, df_isonymic,
    xlabel="Distancia Euclidea (Log)", ylabel="Distancia Isonimica (Log)"
)

get_scatterplot_categorical(
    df_isonymic, df_nei,
    xlabel="Distancia Isonimica (Log)", ylabel="Distancia Nei (Log)"
)

get_scatterplot_categorical(
    df_isonymic, df_lasker,
    xlabel="Distancia Isonimica (Log)", ylabel="Distancia Lasker (Log)"
)


