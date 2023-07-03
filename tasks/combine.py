import pandas

def combine_departamental_datasets_to_refactor(upstream, product):
    ''''''
    karlin_df_2001 = pandas.read_parquet(upstream["get-karlin-mcgregor-departmental-2001"])
    wright_df_2001 = pandas.read_parquet(upstream["get-wright-m-departmental-2001"])
    output_2001_df = pandas.merge(karlin_df_2001, wright_df_2001, on="department_id")
    
    karlin_df_2015 = pandas.read_parquet(upstream["get-karlin-mcgregor-departmental-2015"])
    wright_df_2015 = pandas.read_parquet(upstream["get-departmental-wright-m-2015"])
    isonymy_df_2015 = pandas.read_parquet(upstream["get-isonymy-departments-2015"])

    # merge operations:
    output_2015_df = pandas.merge(
        pandas.merge(karlin_df_2015, wright_df_2015, on="department_id"),
        isonymy_df_2015,
        on="department_id"
    )

    output_2015_df = output_2015_df.sort_values("department_id").reset_index(drop=True)
    ordered_columns = [
        'department_id',
        'population_2015', 'n', 'ins', 'fst', 'fishers_alpha', 'A', 'B', 'v', 'm'
    ]

    output_2015_df = output_2015_df[ordered_columns]

    karlin_df_2021 = pandas.read_parquet(upstream["get-departmental-karlin-mcgregor-2021"])
    wright_df_2021 = pandas.read_parquet(upstream["get-departmental-wright-m-2021"])
    isonymy_df_2021 = pandas.read_parquet(upstream["get-departmental-isonymy-2021"])

    isonymy_df_2021 = isonymy_df_2021.rename(columns={'division': 'department_id'})

    output_2021_df = pandas.merge(
        pandas.merge(karlin_df_2021, wright_df_2021, on="department_id"),
        isonymy_df_2021,
        on="department_id"
    )

    output_2021_df = output_2021_df.sort_values("department_id").reset_index(drop=True)
    print(output_2021_df.columns)
    ordered_columns = [
        'department_id',
        'population_2021', 'n', 'ins', 'fst', 'fishers_alpha', 'A', 'B', 'v', 'm'
    ]
    output_2021_df = output_2021_df[ordered_columns]

    
    output_2001_df.columns = output_2001_df.columns.str.lower()
    output_2015_df.columns = output_2015_df.columns.str.lower()
    output_2021_df.columns = output_2021_df.columns.str.lower()
    
    output_2001_df.to_parquet(str(product["data_2001"]))
    output_2015_df.to_parquet(str(product["data_2015"]))
    output_2021_df.to_parquet(str(product["data_2021"]))