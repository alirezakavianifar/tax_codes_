import pandas as pd

file_path = r"E:\automating_reports_V2\saved_dir\pos_faal.xlsx"
# Load all sheets into a dictionary of DataFrames
dff_final = pd.read_excel(file_path)

# Reset index to ensure consistency
dff_final = dff_final.reset_index()

# Group by 'کد پستی' and 'شماره رهگیری ثبت نام' and count occurrences
count = dff_final.groupby(
    ['کد پستی', 'شماره رهگیری ثبت نام']).size().reset_index(name='Count')

# Set index as required
dff_final = dff_final.set_index(['کد پستی', 'شماره رهگیری ثبت نام'])

# Save to Excel with multiple sheets
with pd.ExcelWriter(r'pos_sheet4.xlsx') as writer:
    dff_final.to_excel(writer, sheet_name='Main Data')
    count.to_excel(writer, sheet_name='Count Data', index=False)
