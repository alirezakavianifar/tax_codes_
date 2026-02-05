import pandas as pd


def rename_duplicates(df, col_names, replaced_char='_'):
    for col in col_names:
        df[col].mask(df.duplicated(subset=[col]) == True,
                     df[col]+replaced_char, inplace=True)

    return df


# accepts a saving path and a dictionary of dataframes as arguments and saves each dataframe into an excel sheet
def df_to_excelsheet(saved_path, dict_df, index, *args, **kwargs):
    with pd.ExcelWriter(saved_path) as writer:
        for key, edare in dict_df:
            if isinstance(key, tuple):
                sheet_name = ''
                for item in key:
                    if 'names' in kwargs:
                        for name in kwargs['names']:
                            if name in item:
                                item = item.replace(name, '')
                    sheet_name += item + '-'
            else:
                sheet_name = str(key)
            edare.to_excel(writer, sheet_name=sheet_name)
