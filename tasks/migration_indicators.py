# -*- coding: utf-8 -*-
# Obtiene Alpha de Fisher y v de Karlin McGregor

# +
import pandas
from termcolor import colored

from surnames_package import isonymic
from surnames_package import cleaning

# -
def get_karlin_mcgregor_departamental_2001(product):
    """"""
    # ya está procesado en un excel, limpiar para coincidir con los códigos del pipeline
    print("Computing get_karlin_mcgregor_departamental_2001...")
    path = "/home/lmorales/work/pipelines/surname_migrations_pipeline/_input_data/Base para Leo.xlsx"
    df = pandas.read_excel(path)
    df.columns = df.columns.str.upper()
    
    work_columns = ['CODPROV', 'CODLOC', 'V', "POBL2001", "A"]
    output_df = df[work_columns]\
        .rename(columns={
            'V': 'v',
            'POBL2001': 'n',
            'A': 'a'
        })
    
    output_df['provincia_id'] = \
        cleaning.rewrite_province_codes(output_df.loc[:, 'CODPROV'])
    output_df['department_id'] = \
        cleaning.rewrite_department_codes(
            output_df['CODLOC'],
            output_df['provincia_id'])
    
    output_df = output_df[
        ["department_id", "provincia_id", "n", "v", "a"]
    ]
    output_df.to_parquet(str(product))

def get_wright_m_departaments_2001(product):
    """"""
    print("Computing get_wright_m_departaments_2001...")
    # ya está procesado en un excel, limpiar para coincidir con los códigos del pipeline
    path = "/home/lmorales/work/pipelines/surname_migrations_pipeline/_input_data/Base para Leo.xlsx"
    df = pandas.read_excel(path)
    df.columns = df.columns.str.upper()
    
    df['m'] = isonymic.get_wright_m(df['POBL2001'], df['FST'])
    
    work_columns = ['CODPROV', 'CODLOC', 'POBL2001', 'IS', 'FST', 'm']
    # department_id 	population_2015 	ins 	fst 	m
    output_df = df[work_columns]\
        .rename(columns={
            'POBL2001': 'population_2001',
            'FST': 'fst',
            'IS': 'ins',
        })
    
    output_df['provincia_id'] = \
        cleaning.rewrite_province_codes(output_df.loc[:, 'CODPROV'])
    output_df['department_id'] = \
        cleaning.rewrite_department_codes(
            output_df['CODLOC'],
            output_df['provincia_id'])
    
    output_df = output_df[
        ["department_id", "population_2001", "ins", "fst", "m"]
    ]
    output_df.to_parquet(str(product))


def _get_karlin_mcgregor_by_column_deprecated(df, slice_by_column):    
    """
    TODO: PASAR A PAQUETE
    ```Util function```
    
    Obtiene el valor de v de Karlin-McGregor para cada unidad.
    
    Args:
        df (pandas.Dataframe):
            La ruta del dataframe (archivo parquet).
            Debe contener las columnas:
                - slice_by_column
                - surname
    
    Returns:
        pandas.Dataframe:
            Con las columnas:
                - <slice_by_column>: ids de unidades del dataframe de entrada
                - n: cantidad de personas en el unidad
                - a: alfa de Fisher del unidad (necesario para calcular el v)
                - v: v de Karlin-McGregor
            
    """
    assert slice_by_column in df.columns.values, f"La columna {slice_by_column} debe estar en el dataframe recibido"
    
    items = []
    ids_unidades = df[slice_by_column].unique()
    
    for i, unit_id in enumerate(ids_unidades):
        if i % 1_000 == 0:
            print(colored(f"_get_karlin_mcgregor_by_column -> processed: {i}", 'green'))

        slice_data = df[df[slice_by_column] == unit_id]
        items.append(
            {
                f'{slice_by_column}':unit_id,
                'n':len(slice_data),
                'a':isonymic.get_fishers_alpha(slice_data.surname)
            }
        )

    output_df = pandas.DataFrame(items)
    output_df['v'] = output_df.apply(lambda row: row['a'] / (row['n'] + row['a']), axis=1)
    return output_df

def _get_karlin_mcgregor_by_column(df, slice_by_column, surname_column: str = 'surname'):    
    """
    TODO: PASAR A PAQUETE
    ```Util function```
    
    Obtiene el valor de v de Karlin-McGregor para cada unidad.
    
    Args:
        df (pandas.Dataframe):
            La ruta del dataframe (archivo parquet).
            Debe contener las columnas:
                - slice_by_column
                - surname
    
    Returns:
        pandas.Dataframe:
            Con las columnas:
                - <slice_by_column>: ids de unidades del dataframe de entrada
                - n: cantidad de personas en el unidad
                - a: alfa de Fisher del unidad (necesario para calcular el v)
                - v: v de Karlin-McGregor
            
    """
    assert slice_by_column in df.columns.values, f"La columna {slice_by_column} debe estar en el dataframe recibido"
    
    items = []
    cell_ids = df[slice_by_column].unique()
    
    for i, cell_id in enumerate(cell_ids):
        if i % 1_000 == 0:
            print(colored(f"_get_karlin_mcgregor_by_column -> processed: {i}", 'green'))

        slice_data = df[df[slice_by_column] == cell_id]
        slice_surnames = slice_data[surname_column]
        items.append(
            {
                f'{slice_by_column}':cell_id,
                'n':len(slice_surnames),
                'a':isonymic.get_fishers_alpha(slice_surnames)
            }
        )

    output_df = pandas.DataFrame(items)
    output_df['v'] = isonymic.get_karlin_mcgregor_v_vect(output_df['a'], output_df['n'])
    return output_df

def _get_karlin_mcgregor_departamental(df):
    return _get_karlin_mcgregor_by_column(df, slice_by_column='department_id')

def get_karlin_mcgregor_departamental_2015(product):
    """"""
    print("Computing karlin_mcgregor_departamental_2015...")
    df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet")
    #df = pandas.read_parquet(surname_data_path)
    output_df = _get_karlin_mcgregor_departamental(df)
    output_df.to_parquet(str(product))


def get_karlin_mcgregor_departamental_2021(product):
    """"""
    df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet")
    output_df = _get_karlin_mcgregor_departamental(df)
    output_df.to_parquet(str(product))


def get_karlin_mcgregor_circuito_2021(product):
   
    #path = "/home/lmorales/datasets/padron2021/Padron2021.txt"
    #df = pandas.read_csv(path, delimiter="|", encoding="latin1")
    df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet")
    print(colored(f"\nPadron 2021 -> Leido: {len(df)}", 'green', attrs=['bold','underline']))
    
    print(colored(f"\nPadron 2021 -> Procesando circuitos: {len(df)}", 'green'))
    
    # process:
    output_df = _get_karlin_mcgregor_by_column(df, slice_by_column="circuit_id")
    
    # save:
    print(colored("\nKarlin-McGregor Circuitos 2021: OK", 'green', attrs=['bold','underline']))
    output_df.to_parquet(str(product))

def get_wright_m_departaments_2015(upstream, product):
    isonymy_df = pandas.read_parquet(
        upstream['get-isonymy-departments-2015'])
    population_df = pandas.read_parquet(
        "/home/lmorales/resources/censo_proyeccion_poblacion.parquet")

    wright_df = pandas.merge(
        population_df,
        isonymy_df,
        left_on='departamento_id',
        right_on='department_id'
    )

    wright_df['m'] = \
            isonymic.get_wright_m(
                wright_df['total_2015'],
                wright_df['fst']
            )

    wright_df = \
        wright_df[['department_id', 'total_2015', 'ins', 'fst', 'm']]\
            .rename(columns={'total_2015': 'population_2015'})
    
    wright_df.to_parquet(str(product))


def get_wright_m_departaments_2021(upstream, product):
    isonymy_df = pandas.read_parquet(
        upstream['get-isonymy-departments-2021'])
    population_df = pandas.read_parquet(
        "/home/lmorales/resources/censo_proyeccion_poblacion.parquet")

    wright_df = pandas.merge(
        population_df,
        isonymy_df,
        left_on='departamento_id',
        right_on='department_id'
    )

    wright_df['m'] = \
            isonymic.get_wright_m(
                wright_df['total_2021'],
                wright_df['fst']
            )

    wright_df = \
        wright_df[['department_id', 'total_2021', 'ins', 'fst', 'm']]\
            .rename(columns={'total_2021': 'population_2021'})
    
    wright_df.to_parquet(str(product))