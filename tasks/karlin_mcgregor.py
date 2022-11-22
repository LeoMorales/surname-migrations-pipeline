# -*- coding: utf-8 -*-
# Obtiene Alpha de Fisher y v de Karlin McGregor

# +
import pandas
from termcolor import colored

from surnames_package import isonymic


# -

def _contains_letter(code):
    return len([letter for letter in str(code) if not letter.isdigit()]) > 0

def _get_karlin_mcgregor_by_column(df, slice_by_column):
    
    """
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
    
    # df['codigo_circuito'] = df["Cod_Circuito"].astype('str').str.rjust(4, fillchar='0')
    # df['code_with_letter'] = df.Cod_Circuito.apply(_contains_letter)
    # df.loc[df.code_with_letter == True, "codigo_circuito"] = \
    #     df.loc[df.code_with_letter == True].codigo_circuito.str.rjust(5, fillchar='0')
    
    # df = df.rename(columns={'Apellido': 'surname'})
    # df = df.drop(columns='code_with_letter')

    print(colored(f"\nPadron 2021 -> Procesando circuitos: {len(df)}", 'green'))
    
    # process:
    output_df = _get_karlin_mcgregor_by_column(df, slice_by_column="circuit_id")
    
    # save:
    print(colored("\nKarlin-McGregor Circuitos 2021: OK", 'green', attrs=['bold','underline']))
    output_df.to_parquet(str(product))
