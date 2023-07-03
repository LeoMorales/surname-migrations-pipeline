# -*- coding: utf-8 -*-
import pandas
import math
import numpy
from termcolor import colored
import itertools
import geopandas
import logging


# Unique Combination Pairs for list of elements
def uniqueCombinations(list_elements):
    l = list(itertools.combinations(list_elements, 2))
    s = set(l)
    # print('actual', len(l), l)
    return sorted(list(s))

def get_internal_isonymy(surnames_i):
    """ Retorna las isonímia interna de una población segun sus apellidos.
    
    Args:
        surnames_i (pandas.Series): Apellidos de c/u de los miembros de una población
    
    Returns:
        float: Valor de la isonimia.
    """
    df_surnames_i = \
        surnames_i\
            .value_counts()\
            .reset_index()\
            .rename(columns=dict(index="surname", surname="counts"))

    df_surnames_i["relative_frequency"] = df_surnames_i.counts / len(surnames_i)
    df_surnames_i["relative_frequency_squared"] = df_surnames_i.relative_frequency ** 2

    return df_surnames_i.relative_frequency_squared.sum()

def __get_distances(surnames_i, surnames_j):
    ''' Devuelve un diccionario con todas las distancias entre las dos listas de apellidos recibidas.
    
    Returns:
        - dict: Distancias isonímicas.
            'I_ij': Isonímia entre i y j.
            'L_ij': Lasker entre i y j.
            'E_ij': Euclidea entre i y j.
            'N_ij': Nei entre i y j.
            'I_ii': Isonímia en i.
            'I_jj': Isonímia en j.   
    '''
    product = {
        'I_ii': None,
        'I_jj': None,
        'I_ij': None,
        'L_ij': None,
        'E_ij': None,
        'N_ij': None,
    }

    len_group_a = len(surnames_i)
    len_group_b = len(surnames_j)

    common_surnames = list(set(surnames_i) & set(surnames_j))

    group_a = \
        surnames_i[surnames_i.isin(common_surnames)]\
            .value_counts()\
            .reset_index()\
            .rename(columns=dict(index="surname", surname="counts_a"))

    group_a["relative_frequency_a"] = group_a.counts_a / len_group_a

    group_b = \
        surnames_j[surnames_j.isin(common_surnames)]\
            .value_counts()\
            .reset_index()\
            .rename(columns=dict(index="surname", surname="counts_b"))

    group_b["relative_frequency_b"] = group_b.counts_b / len_group_b

    assert len(group_a) == len(group_b)

    # merge by surname:
    distances_df = pandas.merge(group_a, group_b, on="surname")
    # formula
    distances_df["product_of_frequencies"] = \
        distances_df.relative_frequency_a * distances_df.relative_frequency_b

    # save isonymy    
    product["I_ij"] = distances_df["product_of_frequencies"].sum()
    # save lasker    
    product["L_ij"] = math.log(product["I_ij"]) * -1

    distances_df["square_root_of_the_product_of_frequencies"] = \
    distances_df.product_of_frequencies.apply(math.sqrt)

    # save euclidean
    product["E_ij"] = math.sqrt(1 - distances_df.square_root_of_the_product_of_frequencies.sum())

    product["I_ii"] = get_internal_isonymy(surnames_i)
    product["I_jj"] = get_internal_isonymy(surnames_j)

    product["N_ij"] = -1 * math.log(product["I_ij"]/ math.sqrt(product["I_ii"] * product["I_jj"]))

    return product


def get_dapartamental_distances(product):

    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    data_path = "/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet"

    print(colored("\nReading data", 'blue', attrs=['bold','underline']))
    print("...")
    df = pandas.read_parquet(data_path)
    print(colored(f"\nReaded: {len(df)}", 'green', attrs=['bold','underline']))

    # create empty matrices
    codes = df.department_id.unique()#[:5]
    isonymic_distance_matrix = pandas.DataFrame(
        data=numpy.zeros([len(codes), len(codes)]),
        columns=[str(code) for code in codes],
        index=[str(code) for code in codes])
    lasker_distance_matrix = isonymic_distance_matrix.copy()
    nei_distance_matrix = isonymic_distance_matrix.copy()
    euclidean_distance_matrix = isonymic_distance_matrix.copy()

    for i, (department_a, department_b) in enumerate(uniqueCombinations(codes)):

        if i % 1_000 == 0:
            logger.info(f"processed: {i}")

        # filter departamental surnames
        surnames_a = df[df.department_id == department_a]["surname"]
        surnames_b = df[df.department_id == department_b]["surname"]

        # get distances
        distances = __get_distances(surnames_a, surnames_b)

        # fill the matrix
        isonymic_distance_matrix.loc[department_a, department_b] = distances['I_ij']
        isonymic_distance_matrix.loc[department_b, department_a] = distances['I_ij']

        isonymic_distance_matrix.loc[department_a, department_a] = distances['I_ii']
        isonymic_distance_matrix.loc[department_b, department_b] = distances['I_jj']

        lasker_distance_matrix.loc[department_a, department_b] = distances['L_ij']
        lasker_distance_matrix.loc[department_b, department_a] = distances['L_ij']

        nei_distance_matrix.loc[department_a, department_b] = distances['N_ij']
        nei_distance_matrix.loc[department_b, department_a] = distances['N_ij']

        euclidean_distance_matrix.loc[department_a, department_b] = distances['E_ij']
        euclidean_distance_matrix.loc[department_b, department_a] = distances['E_ij']

    # save all:
    isonymic_distance_matrix.to_parquet(str(product['isonymic']))
    lasker_distance_matrix.to_parquet(str(product['lasker']))
    nei_distance_matrix.to_parquet(str(product['nei']))
    euclidean_distance_matrix.to_parquet(str(product['euclidean']))


def get_dapartamental_distances_in_meters(product, departments_shape_file):
    """ Construye la matriz de distancias en metros entre cada centroide de departamento.
    Utiliza la proyección Mercator.
    
    Args:
        departments_shape_file (str): Nombre del archivo de la capa.
        
    Return:
        pandas.DataFrame: Matriz con las distancias entre departamentos a partir de los centroides.
    """
    shape = geopandas.read_file(departments_shape_file)
    distances_gdf = shape[["departamento_id", "geometry"]].drop_duplicates(subset="departamento_id").copy()
    # Reproject to Mercator
    distances_gdf = distances_gdf.to_crs("EPSG:3395")
    

    for department_id in distances_gdf.departamento_id.unique():
        department_shape = distances_gdf[distances_gdf.departamento_id == department_id]["geometry"].iat[0]
        distances_gdf[str(department_id)] = distances_gdf.centroid.distance(department_shape.centroid)

    distances_gdf.index = distances_gdf.departamento_id.values
    distances_gdf = distances_gdf.drop(columns=["departamento_id", "geometry"])
    
    pandas.DataFrame(distances_gdf).to_parquet(str(product))

