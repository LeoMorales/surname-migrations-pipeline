import pandas
from surnames_package import isonymic
from surnames_package import cleaning


def get_karlin_mcgregor_departmental_2001(product):
    """"""
    # ya está procesado en un excel, limpiar para coincidir con los códigos del pipeline
    print("Computing get_karlin_mcgregor_departmental_2001...")
    path = "/home/lmorales/work/pipelines/migration_pipeline/_input_data/Base para Leo.xlsx"
    df = pandas.read_excel(path)
    df.columns = df.columns.str.upper()
    
    columns_mapper = {
        'V': 'v',
        "N INDIV": 'n',
        'ALFA': 'fishers_alpha',
        'A': 'a',
        'B': 'b'
    }

    work_columns = ['CODPROV', 'CODLOC', *list(columns_mapper.keys())]
    output_df = df[work_columns].rename(columns=columns_mapper)
    
    output_df['provincia_id'] = \
        cleaning.rewrite_province_codes(output_df.loc[:, 'CODPROV'])
    output_df['department_id'] = \
        cleaning.rewrite_department_codes(
            output_df['CODLOC'],
            output_df['provincia_id'])
    
    output_df = output_df[
        ["department_id", *list(columns_mapper.values())]
    ]
    output_df.to_parquet(str(product))
    
def get_departmental_karlin_mcgregor_2015(upstream, product):
    """
    Returns:
        pandas.DataFrame: department_id, v
    """
    # df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet")
    # output_df = __get_karlin_mcgregor_by_column(df, slice_by_column='department_id')
    # output_df = output_df.rename(columns={'cell_id': 'department_id'})
    # output_df.to_parquet(str(product))
    df = pandas.read_parquet(str(upstream['get-departmental-isonymy-2015']))
    df = df.rename(columns={'division': 'department_id'})
    output_df = df[['department_id']].copy()
    output_df['v'] = isonymic.get_karlin_mcgregor_v_vect(
        serie_a=df['fishers_alpha'],
        serie_n=df['n']
    )
    
    output_df.to_parquet(str(product))

def get_departmental_karlin_mcgregor_2021(upstream, product):
    """
    Returns:
        pandas.DataFrame: department_id, n, a, v
    """
    # df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet")
    # output_df = __get_karlin_mcgregor_by_column(df, slice_by_column='department_id')
    # output_df = output_df.rename(columns={'cell_id': 'department_id'})
    # output_df.to_parquet(str(product))
    df = pandas.read_parquet(upstream['get-departmental-isonymy-2021'])
    df = df.rename(columns={'division': 'department_id'})

    output_df = df[['department_id']].copy()
    output_df['v'] = isonymic.get_karlin_mcgregor_v_vect(
        serie_a=df['fishers_alpha'],
        serie_n=df['n']
    )
    
    output_df.to_parquet(str(product))



# TODO: Descomentar y adaptar cuando tenga la isonimia a nivel circuito
# TODO [UPDATE]: Carpetita circuito...
# def get_karlin_mcgregor_circuito_2021(product):
   
#     #path = "/home/lmorales/datasets/padron2021/Padron2021.txt"
#     #df = pandas.read_csv(path, delimiter="|", encoding="latin1")
#     df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet")
#     print(colored(f"\nPadron 2021 -> Leido: {len(df)}", 'green', attrs=['bold','underline']))
#     print(colored(f"\nPadron 2021 -> Procesando circuitos: {len(df)}", 'green'))
    
#     # process:
#     output_df = __get_karlin_mcgregor_by_column(df, slice_by_column="circuit_id")
#     # save:
#     print(colored("\nKarlin-McGregor Circuitos 2021: OK", 'green', attrs=['bold','underline']))
#     output_df.to_parquet(str(product))
