import pandas as pd
from automation.helpers import get_edare_shahr, find_edares


df = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\test\ثبت نام هاي اداره کل 16.xlsx', sheet_name=None)

df_edare_shahr = get_edare_shahr()

# Concatenate all sheets into a single DataFrame
combined_df = pd.concat(df.values(), ignore_index=True)

dff_final = find_edares(
    df=combined_df, colname_tomerge='کد حوزه', report_typ='کد رهگیری')

df_hoghoghi = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\test\خوزستان حقوقی.xlsx')

df_haghighi = pd.read_excel(
    r'E:\automating_reports_V2\saved_dir\test\خوزستان حقیقی.xlsx')

df_merged_hoghoghi = df_hoghoghi.merge(
    # Select only necessary columns from combined_df
    dff_final[['کد رهگیری', 'کد اداره']],
    on='کد رهگیری',                         # Join on 'کد رهگیری'
    how='left'                              # Keep all rows from df_hoghoghi
)

df_merged_haghighi = df_haghighi.merge(
    # Select only necessary columns from combined_df
    dff_final[['کد رهگیری', 'کد اداره']],
    on='کد رهگیری',                         # Join on 'کد رهگیری'
    how='left'                              # Keep all rows from df_hoghoghi
)

df_merged_hoghoghi.drop('ردیف', axis=1, inplace=True)
df_merged_haghighi.drop('ردیف', axis=1, inplace=True)

with pd.ExcelWriter(r'E:\automating_reports_V2\saved_dir\test\haghighi.xlsx') as writer:
    for code, group in df_merged_haghighi.groupby('کد اداره'):
        group.to_excel(writer, sheet_name=str(code), index=False)

with pd.ExcelWriter(r'E:\automating_reports_V2\saved_dir\test\hoghoghi.xlsx') as writer:
    for code, group in df_merged_hoghoghi.groupby('کد اداره'):
        group.to_excel(writer, sheet_name=str(code), index=False)

...
