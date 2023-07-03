# -*- coding: utf-8 -*-
# Obtiene Alpha de Fisher y v de Karlin McGregor

# +
import pandas
from surnames_package import isonymic
from surnames_package import cleaning


def get_wright_m_departmental_2001(product):
    """"""
    # ya está procesado en un excel, limpiar para coincidir con los códigos del pipeline
    path = "/home/lmorales/work/pipelines/migration_pipeline/_input_data/Base para Leo.xlsx"
    df = pandas.read_excel(path)
    df.columns = df.columns.str.upper()
    
    df['m'] = isonymic.get_wright_m_vect(df['POBL2001'], df['FST'])
    
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

def get_departmental_wright_m_2015(upstream, product):
    '''
    Returns:
        pandas.DataFrame: department_id, population_2015, m
    '''
    isonymy_df = pandas.read_parquet(str(upstream['get-departmental-isonymy-2015']))
    isonymy_df = isonymy_df.rename(columns={'division': 'department_id'})
    population_df = pandas.read_parquet(
        "/home/lmorales/resources/censo_proyeccion_poblacion.parquet")

    # del dataset de isonimia solo necesitamos el fst y el id de departamento:
    isonymy_df = isonymy_df[['department_id', 'fst']].copy()
    # del dataset de población, solo necesitamos la del 2015
    population_df = population_df[['departamento_id', 'total_2015']].copy()
    population_df = population_df.rename(
        columns={
            'total_2015': 'population_2015',
            'departamento_id': 'department_id'
            }
        )
    
    wright_df = pandas.merge(
        population_df,
        isonymy_df,
        on='department_id'
    )

    wright_df['m'] = \
            isonymic.get_wright_m_vect(
                wright_df['population_2015'],
                wright_df['fst']
            )
    
    wright_df = wright_df.drop(columns=['fst'])
    wright_df.to_parquet(str(product))


def get_departmental_wright_m_2021(upstream, product):
    '''
    Returns:
        pandas.DataFrame: department_id, population_2021, m
    '''
    isonymy_df = pandas.read_parquet(
        upstream['get-departmental-isonymy-2021'])
    population_df = pandas.read_parquet(
        "/home/lmorales/resources/censo_proyeccion_poblacion.parquet")

    # del dataset de isonimia solo necesitamos el fst y el id de departamento:
    isonymy_df = isonymy_df.rename(columns={'division': 'department_id'})
    isonymy_df = isonymy_df[['department_id', 'fst']].copy()
    # del dataset de población, solo necesitamos la del 2021
    population_df = population_df[['departamento_id', 'total_2021']].copy()
    population_df = population_df.rename(
        columns={
            'total_2021': 'population_2021',
            'departamento_id': 'department_id'
            }
        )
    
    wright_df = pandas.merge(
        population_df,
        isonymy_df,
        on='department_id'
    )

    wright_df['m'] = \
            isonymic.get_wright_m_vect(
                wright_df['population_2021'],
                wright_df['fst']
            )
    
    wright_df = wright_df.drop(columns=['fst'])
    wright_df.to_parquet(str(product))

