import pandas
from surnames_package import isonymic
from termcolor import colored


def __collect_data_by_column(df, slice_by_column, surname_column: str = 'surname'):    
    """
    """
    assert slice_by_column in df.columns.values, f"La columna {slice_by_column} debe estar en el dataframe recibido"
    
    data = []
    for i, (cell_id, cell_data) in enumerate(df.groupby(slice_by_column)):
        if i % 1_000 == 0:
            print(colored(f"__collect_data_by_column -> processed: {i}", 'green'))
        data.append(
            {
                'cell_id': cell_id,
                'ins': isonymic.get_isonymy(cell_data[surname_column]),
            }
        )
    
    # pandas.DataFrame(data).assign(fst=isonymic.get_fst_vect(pandas.DataFrame(data)['ins']))
    output_df = pandas.DataFrame(data)
    output_df['fst'] = isonymic.get_fst_vect(output_df['ins'])
    
    return output_df

def get_isonymy_departments_2015(product):
    """"""
    print("Computing get_isonymy_departments_2015...")
    df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2015.parquet")
    #df = pandas.read_parquet(surname_data_path)
    
    output_df = __collect_data_by_column(
        df,
        slice_by_column='department_id',
        surname_column='surname'
    )
    output_df = output_df.rename(columns={'cell_id': 'department_id'})
    output_df.to_parquet(str(product))

def get_isonymy_departments_2021(product):
    """"""
    print("Computing get_isonymy_departments_2021...")
    df = pandas.read_parquet("/home/lmorales/work/pipelines/surname_data_pipeline/_products/clean/surnames_2021.parquet")
    #df = pandas.read_parquet(surname_data_path)
    
    output_df = __collect_data_by_column(
        df,
        slice_by_column='department_id',
        surname_column='surname'
    )
    output_df = output_df.rename(columns={'cell_id': 'department_id'})
    output_df.to_parquet(str(product))

