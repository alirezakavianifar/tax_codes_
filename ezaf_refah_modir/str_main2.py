import streamlit as st
import pandas as pd
from functools import reduce
import math

st.title('Data Upload and Merge App')

# Upload files
uploaded_ezafe = st.file_uploader(
    "Upload 'اضافه کار تير.xls' file", type=['xls', 'xlsx'])
uploaded_refahi = st.file_uploader(
    "Upload 'رفاهي تير.xls' file", type=['xls', 'xlsx'])
uploaded_zarayeb = st.file_uploader(
    "Upload 'ضرایب مدیر.xlsx' file", type=['xls', 'xlsx'])
uploaded_nafar_edare = st.file_uploader(
    "Upload 'نفر-اداره.xls' file", type=['xls', 'xlsx'])

if uploaded_ezafe and uploaded_refahi and uploaded_zarayeb and uploaded_nafar_edare:
    # Read the uploaded files
    df_ezafe = pd.read_excel(uploaded_ezafe)
    df_refahi = pd.read_excel(uploaded_refahi)
    df_zarayeb = pd.read_excel(uploaded_zarayeb)
    df_nafar_edare = pd.read_excel(uploaded_nafar_edare)

    # Default columns for merging
    default_ezafe_refahi_columns = 'شماره کارمند'
    default_nafar_edare_column = 'شماره کارمند'
    default_zarayeb_column = 'اداره'

    # Allow user to select columns for merging
    ezafe_refahi_column = st.selectbox("Select column for merging 'اضافه کار تير.xls' and 'رفاهي تير.xls':",
                                       df_ezafe.columns, index=df_ezafe.columns.get_loc(default_ezafe_refahi_columns))
    nafar_edare_column = st.selectbox("Select column for merging with 'نفر-اداره.xls':",
                                      df_nafar_edare.columns, index=df_nafar_edare.columns.get_loc(default_nafar_edare_column))
    zarayeb_column = st.selectbox("Select column for merging with 'ضرایب مدیر.xlsx':",
                                  df_zarayeb.columns, index=df_zarayeb.columns.get_loc(default_zarayeb_column))

    # Filter required columns
    df_ezafe = df_ezafe[[ezafe_refahi_column, 'نام کارمند', 'نرخ اضافه کار']]
    df_refahi = df_refahi[[ezafe_refahi_column, 'نرخ']]

    # Perform the merging operations
    lst_ezafe_refahi = [df_ezafe, df_refahi]
    df_merged_ezafe_refahi = reduce(lambda left, right: pd.merge(
        left, right, on=ezafe_refahi_column, how='outer'), lst_ezafe_refahi)

    lst_merged_ezafe_refahi_edare = [df_merged_ezafe_refahi, df_nafar_edare]
    df_merged_ezafe_refahi_edare = reduce(lambda left, right: pd.merge(
        left, right, on=nafar_edare_column, how='left'), lst_merged_ezafe_refahi_edare)

    lst_merged_ezafe_refahi_edare_zarayeb = [
        df_merged_ezafe_refahi_edare, df_zarayeb]
    df_final = reduce(lambda left, right: pd.merge(left, right, on=zarayeb_column, how='left'),
                      lst_merged_ezafe_refahi_edare_zarayeb).drop_duplicates(subset=[ezafe_refahi_column])

    # Convert 'شماره کارمند' column to string to avoid comma separation
    df_final[ezafe_refahi_column] = df_final[ezafe_refahi_column].astype(str)

    # Function to calculate 'مبلغ اضافه کار اولیه'
    def calculate_ezafe(x):
        try:
            score = math.ceil(x['سرانه اضافه کار'] * x['نرخ اضافه کار'])
            return score
        except ValueError:
            return 0

    # Function to calculate 'مبلغ رفاهی اولیه'
    def calculate_refahi(x):
        try:
            score = math.ceil((x['سرانه رفاهی'] * x['نرخ']) / 100)
            return score
        except ValueError:
            return 0

    # Apply calculations
    df_final['مبلغ اضافه کار اولیه'] = df_final.apply(
        lambda row: calculate_ezafe(row), axis=1)
    df_final['مبلغ رفاهی اولیه'] = df_final.apply(
        lambda row: calculate_refahi(row), axis=1)

    # Select columns for detailed DataFrame
    cols_df_final = ['اداره', 'شماره کارمند',
                     'نام کارمند', 'نرخ اضافه کار', 'نرخ']

    # Group by 'اداره' and aggregate
    grouped_df = df_final.groupby('اداره', dropna=False).agg({
        'مبلغ اضافه کار اولیه': 'sum',
        'مبلغ رفاهی اولیه': 'sum',
        'شماره کارمند': 'count',  # Counting rows in each group
        'سرانه اضافه کار': 'first',  # Adding first value of 'سرانه اضافه کار'
        'سرانه رفاهی': 'first'  # Adding first value of 'سرانه رفاهی'
    }).reset_index()

    # Rename 'شماره کارمند' to 'تعداد'
    grouped_df.rename(columns={'شماره کارمند': 'تعداد'}, inplace=True)

    # Fill NaN values in 'اداره' with 'نامشخص'
    grouped_df['اداره'] = grouped_df['اداره'].fillna('نامشخص')

    # Display the detailed DataFrame
    st.write("Detailed DataFrame:")
    st.dataframe(df_final[cols_df_final])

    # Display the grouped DataFrame
    st.write("Grouped DataFrame:")
    st.dataframe(grouped_df)

    # Download link for the final result
    def to_excel(df, file_name):
        with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
            worksheet = writer.sheets['Sheet1']
            worksheet.right_to_left()

        return file_name

    # Creating a detailed Excel file with a sheet for each اداره
    def to_excel_multiple_sheets(df, grouped_df, file_name):
        header_text = """
        توجه: متذکر می گردد صرفا ستون های مربوط به ساعت اضافه کار و درصد رفاهی تکمیل گردد
        سقف اضافه کار همکاران مشاغل کارگری حداکثر 120 ساعت می باشد
        ساعت اضافه کار و ضریب رفاهی بین 0 تا 30 ، صفر محاسبه می گردد
        """

        with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'align': 'center',
                'font_color': 'red'
            })

            # Number format for numeric columns
            number_format = workbook.add_format({'num_format': '#,##0'})

            # Iterate over each 'اداره' to create a sheet
            for edareh in df['اداره'].unique():
                sheet_name = str(edareh)[:31]
                worksheet = workbook.add_worksheet(sheet_name)

                # Set sheet direction to right-to-left
                worksheet.right_to_left()

                # Write the header text
                worksheet.merge_range('A1:L4', header_text, header_format)

                # Get the filtered data for this 'اداره'
                df_filtered = df[df['اداره'] == edareh]
                columns = df_filtered.columns.tolist(
                ) + ['ساعت اضافه کار', 'درصد رفاهی', 'مبلغ اضافه', 'مبلغ رفاهی']

                # Write the data columns
                for i, col in enumerate(columns):
                    worksheet.write(4, i, col)

                # Write the data rows
                for row_num, row_data in enumerate(df_filtered.values):
                    for col_num, cell_data in enumerate(row_data):
                        worksheet.write(row_num + 5, col_num, cell_data)

                # Add the formula-based columns
                start_row = 5
                for row_num in range(start_row, start_row + len(df_filtered)):
                    # ساعت اضافه کار and درصد رفاهی (user input cells)
                    # ساعت اضافه کار
                    worksheet.write(row_num, len(df_filtered.columns), '')
                    worksheet.write(row_num, len(
                        df_filtered.columns) + 1, '')  # درصد رفاهی

                    # مبلغ اضافه calculation: (ساعت اضافه کار * نرخ اضافه کار)
                    formula_ezafe = f"=IF(F{row_num+1}=\"\",\"\",F{row_num+1}*D{row_num+1})"
                    worksheet.write_formula(row_num, len(
                        df_filtered.columns) + 2, formula_ezafe)

                    # مبلغ رفاهی calculation: (درصد رفاهی * نرخ) / 100
                    formula_refahi = f"=IF(G{row_num+1}=\"\",\"\",G{row_num+1}*E{row_num+1}/100)"
                    worksheet.write_formula(row_num, len(
                        df_filtered.columns) + 3, formula_refahi)

                # Get the aggregated data for this 'اداره' from the grouped_df
                grouped_data = grouped_df[grouped_df['اداره'] == edareh]

                if not grouped_data.empty:
                    مبلغ_اضافه_کار_اولیه = grouped_data['مبلغ اضافه کار اولیه'].values[0]
                    مبلغ_رفاهی_اولیه = grouped_data['مبلغ رفاهی اولیه'].values[0]

                    # Write the headers in columns M and N
                    worksheet.write(4, 12, 'کل مبلغ قابل توزیع اضافه کار')
                    worksheet.write(4, 13, 'کل مبلغ قابل توزیع رفاهی')

                    # Write the aggregated values in the first data row
                    worksheet.write(5, 12, مبلغ_اضافه_کار_اولیه, number_format)
                    worksheet.write(5, 13, مبلغ_رفاهی_اولیه, number_format)

                # Format the other numerical columns with commas
                for col_num in range(3, 11):  # Columns D to K (excluding 'شماره کارمند')
                    worksheet.set_column(col_num, col_num, None, number_format)

        return file_name

    detailed_output_file = to_excel(
        df_final[cols_df_final], 'detailed_data.xlsx')
    grouped_output_file = to_excel(grouped_df, 'grouped_data.xlsx')
    detailed_sheets_file = to_excel_multiple_sheets(
        df_final[cols_df_final], grouped_df, 'detailed_data_sheets.xlsx')

    with open(detailed_output_file, 'rb') as file:
        btn1 = st.download_button(
            label="Download Detailed Data",
            data=file,
            file_name="detailed_data.xlsx",
            mime="application/vnd.ms-excel"
        )

    with open(grouped_output_file, 'rb') as file:
        btn2 = st.download_button(
            label="Download Grouped Data",
            data=file,
            file_name="grouped_data.xlsx",
            mime="application/vnd.ms-excel"
        )

    with open(detailed_sheets_file, 'rb') as file:
        btn3 = st.download_button(
            label="Download Detailed Data with Sheets",
            data=file,
            file_name="detailed_data_sheets.xlsx",
            mime="application/vnd.ms-excel"
        )
