import streamlit as st
import pandas as pd
import numpy as np
from functools import reduce
import math

st.set_page_config(layout="wide")

# Inject custom CSS to change the font and direction
st.markdown(
    """
    <style>
    body {
        font-family: 'Tahoma', sans-serif;
        direction: rtl;
    }
    .stApp {
        direction: rtl;
    }
    h1, h2, h3, h4, h5, h6, label {
        font-family: 'Tahoma', sans-serif;
    }
    </style>
    """,
    unsafe_allow_html=True
)

st.title('برنامه آپلود و ادغام داده‌ها')

# Dropdown to select the process
process_option = st.selectbox("کدام فرآیند را می‌خواهید اجرا کنید؟",
                              ["ادغام و محاسبه اضافه کار و رفاهی", "پردازش نیم درصد و تجمیع پاداش", "اضافه کار و رفاهی مبلغی"])

if process_option == "ادغام و محاسبه اضافه کار و رفاهی":
    # Original code
    uploaded_ezafe = st.file_uploader(
        "فایل 'اضافه کار تیر.xls' را آپلود کنید", type=['xls', 'xlsx'])
    uploaded_refahi = st.file_uploader(
        "فایل 'رفاهی تیر.xls' را آپلود کنید", type=['xls', 'xlsx'])
    uploaded_zarayeb = st.file_uploader(
        "فایل 'ضرایب مدیر.xlsx' را آپلود کنید", type=['xls', 'xlsx'])
    uploaded_nafar_edare = st.file_uploader(
        "فایل 'نفر-اداره.xls' را آپلود کنید", type=['xls', 'xlsx'])

    if uploaded_ezafe and uploaded_refahi and uploaded_zarayeb and uploaded_nafar_edare:
        df_ezafe = pd.read_excel(uploaded_ezafe)
        df_refahi = pd.read_excel(uploaded_refahi)
        df_zarayeb = pd.read_excel(uploaded_zarayeb)
        df_nafar_edare = pd.read_excel(uploaded_nafar_edare)

        default_ezafe_refahi_columns = 'شماره کارمند'
        default_nafar_edare_column = 'شماره کارمند'
        default_zarayeb_column = 'اداره'

        ezafe_refahi_column = st.selectbox("ستون مورد نظر برای ادغام 'اضافه کار تیر.xls' و 'رفاهی تیر.xls' را انتخاب کنید:",
                                           df_ezafe.columns, index=df_ezafe.columns.get_loc(default_ezafe_refahi_columns))
        nafar_edare_column = st.selectbox("ستون مورد نظر برای ادغام با 'نفر-اداره.xls' را انتخاب کنید:",
                                          df_nafar_edare.columns, index=df_nafar_edare.columns.get_loc(default_nafar_edare_column))
        zarayeb_column = st.selectbox("ستون مورد نظر برای ادغام با 'ضرایب مدیر.xlsx' را انتخاب کنید:",
                                      df_zarayeb.columns, index=df_zarayeb.columns.get_loc(default_zarayeb_column))

        df_ezafe = df_ezafe[[ezafe_refahi_column,
                             'نام کارمند', 'نرخ اضافه کار']]
        df_refahi = df_refahi[[ezafe_refahi_column, 'نرخ رفاهی']]

        lst_ezafe_refahi = [df_ezafe, df_refahi]
        df_merged_ezafe_refahi = reduce(lambda left, right: pd.merge(
            left, right, on=ezafe_refahi_column, how='outer'), lst_ezafe_refahi)

        lst_merged_ezafe_refahi_edare = [
            df_merged_ezafe_refahi, df_nafar_edare]
        df_merged_ezafe_refahi_edare = reduce(lambda left, right: pd.merge(
            left, right, on=nafar_edare_column, how='left'), lst_merged_ezafe_refahi_edare)

        lst_merged_ezafe_refahi_edare_zarayeb = [
            df_merged_ezafe_refahi_edare, df_zarayeb]
        df_final = reduce(lambda left, right: pd.merge(left, right, on=zarayeb_column, how='left'),
                          lst_merged_ezafe_refahi_edare_zarayeb).drop_duplicates(subset=[ezafe_refahi_column])

        df_final[ezafe_refahi_column] = df_final[ezafe_refahi_column].astype(
            str)

        def calculate_ezafe(x):
            try:
                score = math.ceil(x['سرانه اضافه کار'] * x['نرخ اضافه کار'])
                return score
            except ValueError:
                return 0

        def calculate_refahi(x):
            try:
                score = math.ceil((x['سرانه رفاهی'] * x['نرخ رفاهی']) / 100)
                return score
            except ValueError:
                return 0

        df_final['مبلغ اضافه کار اولیه'] = df_final.apply(
            lambda row: calculate_ezafe(row), axis=1)
        df_final['مبلغ رفاهی اولیه'] = df_final.apply(
            lambda row: calculate_refahi(row), axis=1)

        cols_df_final = ['اداره', 'شماره کارمند',
                         'نام کارمند', 'نرخ اضافه کار', 'نرخ رفاهی']

        df_final['اداره'] = df_final['اداره'].fillna('نامشخص')

        grouped_df = df_final.groupby('اداره', dropna=False).agg(
            مبلغ_اضافه_کار_اولیه=('مبلغ اضافه کار اولیه', 'sum'),
            مبلغ_رفاهی_اولیه=('مبلغ رفاهی اولیه', 'sum'),
            تعداد_کارمند=('شماره کارمند', 'count'),
            سرانه_اضافه_کار=('سرانه اضافه کار', 'first'),
            سرانه_اضافه_کار_جمع=('سرانه اضافه کار', 'sum'),
            سرانه_رفاهی=('سرانه رفاهی', 'first'),
            سرانه_رفاهی_جمع=('سرانه رفاهی', 'sum')
        ).reset_index()

        grouped_df.rename(columns={'شماره کارمند': 'تعداد'}, inplace=True)
        # grouped_df['اداره'] = grouped_df['اداره'].fillna('نامشخص')

        st.write("جدول جزئیات:")
        st.dataframe(df_final[cols_df_final])

        st.write("جدول گروه‌بندی‌شده:")
        st.dataframe(grouped_df)

        def to_excel(df, file_name):
            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
                worksheet = writer.sheets['Sheet1']
                worksheet.right_to_left()
                workbook = writer.book
                number_format = workbook.add_format({'num_format': '#,##0'})
                numeric_columns = [i for i, col in enumerate(
                    df.columns) if col not in ['شماره کارمند', 'اداره']]
                for col_num in numeric_columns:
                    worksheet.set_column(col_num, col_num, None, number_format)
            return file_name

        # Generate Excel file with a sheet for each department
        def to_excel_multiple_sheets_ezafe(df, grouped_df, file_name):
            header_text = """
            توجه: متذکر می‌گردد صرفاً ستون‌های مربوط به ساعت اضافه کار و درصد رفاهی تکمیل گردد.
            سقف اضافه کار همکاران مشاغل کارگری حداکثر 120 ساعت می‌باشد.
            ساعت اضافه کار و ضریب رفاهی بین 0 تا 29، صفر محاسبه می‌گردد.
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

                number_format = workbook.add_format({'num_format': '#,##0'})

                for edareh in df['اداره'].unique():
                    sheet_name = str(edareh)[:31]
                    worksheet = workbook.add_worksheet(sheet_name)
                    worksheet.right_to_left()
                    worksheet.merge_range('A1:L4', header_text, header_format)

                    df_filtered = df[df['اداره'] == edareh]
                    columns = df_filtered.columns.tolist(
                    ) + ['ساعت اضافه کار', 'درصد رفاهی', 'مبلغ اضافه', 'مبلغ رفاهی']

                    for i, col in enumerate(columns):
                        worksheet.write(4, i, col)

                    for row_num, row_data in enumerate(df_filtered.values):
                        for col_num, cell_data in enumerate(row_data):
                            worksheet.write(row_num + 5, col_num, cell_data)

                    start_row = 5
                    for row_num in range(start_row, start_row + len(df_filtered)):
                        worksheet.write(row_num, len(df_filtered.columns), '')
                        worksheet.write(row_num, len(
                            df_filtered.columns) + 1, '')

                        formula_ezafe = f"=IF(F{row_num+1}=\"\",\"\",F{row_num+1}*D{row_num+1})"
                        worksheet.write_formula(row_num, len(
                            df_filtered.columns) + 2, formula_ezafe)

                        formula_refahi = f"=IF(G{row_num+1}=\"\",\"\",G{row_num+1}*E{row_num+1}/100)"
                        worksheet.write_formula(row_num, len(
                            df_filtered.columns) + 3, formula_refahi)

                        total_row = start_row + len(df_filtered)

                        worksheet.write(total_row, len(df_filtered.columns),
                                        f'=SUM({chr(65 + len(df_filtered.columns))}{start_row+1}:{chr(65 + len(df_filtered.columns))}{total_row})', number_format)
                        worksheet.write(total_row, len(df_filtered.columns) + 1,
                                        f'=SUM({chr(65 + len(df_filtered.columns) + 1)}{start_row+1}:{chr(65 + len(df_filtered.columns) + 1)}{total_row})', number_format)

                        worksheet.write(total_row, len(df_filtered.columns) + 2,
                                        f'=SUM({chr(65 + len(df_filtered.columns) + 2)}{start_row+1}:{chr(65 + len(df_filtered.columns) + 2)}{total_row})', number_format)
                        worksheet.write(total_row, len(df_filtered.columns) + 3,
                                        f'=SUM({chr(65 + len(df_filtered.columns) + 3)}{start_row+1}:{chr(65 + len(df_filtered.columns) + 3)}{total_row})', number_format)

                    grouped_data = grouped_df[grouped_df['اداره'] == edareh]
                    if not grouped_data.empty:
                        مبلغ_اضافه_کار_اولیه = grouped_data['مبلغ_اضافه_کار_اولیه'].values[0]
                        مبلغ_رفاهی_اولیه = grouped_data['مبلغ_رفاهی_اولیه'].values[0]

                        سرانه_اضافه_کار = grouped_data['سرانه_اضافه_کار'].values[0]
                        سرانه_رفاهی = grouped_data['سرانه_رفاهی'].values[0]

                        جمع_سرانه_اضافه_کار = grouped_data['سرانه_اضافه_کار_جمع'].values[0]
                        جمع_سرانه_رفاهی = grouped_data['سرانه_رفاهی_جمع'].values[0]

                        # worksheet.write(4, 12, 'کل مبلغ قابل توزیع اضافه کار')
                        # worksheet.write(4, 13, 'کل مبلغ قابل توزیع رفاهی')

                        worksheet.write(4, 12, 'جمع ساعت اضافه کار قابل تخصیص')
                        worksheet.write(4, 13, 'جمع درصد رفاهی تخصیصی')

                        worksheet.write(7, 12, 'سرانه اضافه کار هر نفر')
                        worksheet.write(7, 13, 'سرانه رفاهی هر نفر')

                        # worksheet.write(
                        #     5, 12, مبلغ_اضافه_کار_اولیه, number_format)
                        # worksheet.write(5, 13, مبلغ_رفاهی_اولیه, number_format)

                        worksheet.write(
                            5, 12, جمع_سرانه_اضافه_کار, number_format)
                        worksheet.write(5, 13, جمع_سرانه_رفاهی, number_format)

                        worksheet.write(
                            8, 12, سرانه_اضافه_کار, number_format)
                        worksheet.write(8, 13, سرانه_رفاهی, number_format)

                    for col_num in range(3, 11):
                        worksheet.set_column(
                            col_num, col_num, None, number_format)

            return file_name

        detailed_output_file = to_excel(
            df_final[cols_df_final], 'detailed_data.xlsx')
        grouped_output_file = to_excel(grouped_df, 'grouped_data.xlsx')
        detailed_sheets_file = to_excel_multiple_sheets_ezafe(
            df_final[cols_df_final], grouped_df, 'detailed_data_sheets.xlsx')

        with open(detailed_output_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های جزئیات",
                data=file,
                file_name="detailed_data.xlsx",
                mime="application/vnd.ms-excel"
            )

        with open(grouped_output_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های گروه‌بندی‌شده",
                data=file,
                file_name="grouped_data.xlsx",
                mime="application/vnd.ms-excel"
            )

        with open(detailed_sheets_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های جزئیات به همراه شیت‌ها",
                data=file,
                file_name="detailed_data_sheets.xlsx",
                mime="application/vnd.ms-excel"
            )

elif process_option == "پردازش نیم درصد و تجمیع پاداش":
    # New code for "پردازش نیم درصد و تجمیع پاداش"
    uploaded_nim = st.file_uploader(
        "فایل 'نیم درصد تیر.xls' را آپلود کنید", type=['xls', 'xlsx'])
    uploaded_zarayeb = st.file_uploader(
        "فایل 'ضرایب مدیر.xlsx' را آپلود کنید", type=['xls', 'xlsx'])
    uploaded_nafar_edare = st.file_uploader(
        "فایل 'نفر-اداره.xls' را آپلود کنید", type=['xls', 'xlsx'])

    if uploaded_nim and uploaded_zarayeb and uploaded_nafar_edare:
        nim_cols = ['شماره کارمند', 'نام کارمند']

        df_nim = pd.read_excel(uploaded_nim)[nim_cols]
        df_zarayeb = pd.read_excel(uploaded_zarayeb)
        df_nafar_edare = pd.read_excel(uploaded_nafar_edare)

        lst_nim_nafar_edare = [df_nim, df_nafar_edare]

        df_nim_nafar_edare = reduce(lambda left, right: pd.merge(
            left, right, on='شماره کارمند', how='left'), lst_nim_nafar_edare)

        lst_nim_nafar_edare_zarayeb = [df_nim_nafar_edare, df_zarayeb]

        df_nim_nafar_edare_zarayeb = reduce(lambda left, right: pd.merge(
            left, right, on='اداره', how='left'), lst_nim_nafar_edare_zarayeb).drop_duplicates(subset=['شماره کارمند'])

        df_nim_nafar_edare_zarayeb['اداره'] = df_nim_nafar_edare_zarayeb['اداره'].fillna(
            'نامشخص')

        grouped_df = df_nim_nafar_edare_zarayeb.groupby('اداره', dropna=False).agg(
            sum_سرانه_پاداش=('سرانه پاداش', 'sum'),
            first_سرانه_پاداش=('سرانه پاداش', 'first'),
            count_شماره_کارمند=('شماره کارمند', 'count')
        ).reset_index()

        grouped_df.rename(columns={
            'count_شماره_کارمند': 'تعداد',
            'first_سرانه_پاداش': 'سرانه پاداش اختصاصی',
            'sum_سرانه_پاداش': 'جمع سرانه پاداش'
        }, inplace=True)

        # grouped_df['اداره'] = grouped_df['اداره'].fillna('نامشخص')

        st.write("جدول جزئیات نیم درصد:")
        st.dataframe(df_nim_nafar_edare_zarayeb)

        st.write("جدول گروه‌بندی‌شده پاداش:")
        st.dataframe(grouped_df)

        def to_excel(df, file_name):
            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
                worksheet = writer.sheets['Sheet1']
                worksheet.right_to_left()
                workbook = writer.book
                number_format = workbook.add_format({'num_format': '#,##0'})
                numeric_columns = [i for i, col in enumerate(
                    df.columns) if col not in ['شماره کارمند', 'اداره']]
                for col_num in numeric_columns:
                    worksheet.set_column(col_num, col_num, None, number_format)
            return file_name

        # Generate Excel file with a sheet for each department
        def to_excel_multiple_sheets_nim(df, grouped_df, file_name):
            try:
                with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                    workbook = writer.book
                    number_format = workbook.add_format(
                        {'num_format': '#,##0'})

                    for edareh in df['اداره'].unique():
                        if edareh == 'نامشخص':
                            print('d')
                        sheet_name = str(edareh)[:31]
                        worksheet = workbook.add_worksheet(sheet_name)
                        worksheet.right_to_left()

                        df_filtered = df[df['اداره'] == edareh].drop(
                            columns=['سرانه پاداش'])

                        # Add the 'مبلغ پاداش' column with default values (e.g., 0 or NaN)
                        df_filtered['مبلغ پاداش'] = np.nan

                        columns = df_filtered.columns.tolist()

                        for i, col in enumerate(columns):
                            worksheet.write(0, i, col)

                        for row_num, row_data in enumerate(df_filtered.values):
                            for col_num, cell_data in enumerate(row_data):
                                if pd.isna(cell_data) or (isinstance(cell_data, float) and np.isnan(cell_data)):
                                    worksheet.write(row_num + 1, col_num, '')
                                else:
                                    worksheet.write(
                                        row_num + 1, col_num, cell_data)

                        # Apply number format to all columns except 'اداره'
                        for col_num in range(len(columns)):
                            if columns[col_num] not in ['اداره', 'شماره کارمند']:
                                worksheet.set_column(
                                    col_num, col_num, None, number_format)

                        # Add auto sum formula for the 'مبلغ پاداش' column
                        mablagh_padash_col_index = columns.index('مبلغ پاداش')
                        sum_formula = f'=SUM({chr(65 + mablagh_padash_col_index)}2:{chr(65 + mablagh_padash_col_index)}{row_num + 2})'
                        worksheet.write(
                            row_num + 2, mablagh_padash_col_index, sum_formula, number_format)

                        grouped_data = grouped_df[grouped_df['اداره'] == edareh]
                        if not grouped_data.empty:
                            # Write 'جمع سرانه پاداش' at the beginning of column F
                            # F column is index 5
                            worksheet.write(0, 5, 'جمع سرانه پاداش')
                            worksheet.write(
                                1, 5, grouped_data['جمع سرانه پاداش'].values[0], number_format)

            except Exception as e:
                print(e)

            return file_name

        detailed_output_file = to_excel(
            df_nim_nafar_edare_zarayeb, 'nim_detailed_data.xlsx')
        grouped_output_file = to_excel(grouped_df, 'nim_grouped_data.xlsx')
        detailed_sheets_file = to_excel_multiple_sheets_nim(
            df_nim_nafar_edare_zarayeb, grouped_df, 'nim_detailed_data_sheets.xlsx')

        with open(detailed_output_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های جزئیات نیم درصد",
                data=file,
                file_name="nim_detailed_data.xlsx",
                mime="application/vnd.ms-excel"
            )

        with open(grouped_output_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های گروه‌بندی‌شده پاداش",
                data=file,
                file_name="nim_grouped_data.xlsx",
                mime="application/vnd.ms-excel"
            )

        with open(detailed_sheets_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های جزئیات به همراه شیت‌ها",
                data=file,
                file_name="nim_detailed_data_sheets.xlsx",
                mime="application/vnd.ms-excel"
            )
elif process_option == "اضافه کار و رفاهی مبلغی":
    # Original code

    uploaded_ezafe = st.file_uploader(
        "فایل 'اضافه کار تیر.xls' را آپلود کنید", type=['xls', 'xlsx'])
    uploaded_refahi = st.file_uploader(
        "فایل 'رفاهی تیر.xls' را آپلود کنید", type=['xls', 'xlsx'])
    uploaded_zarayeb = st.file_uploader(
        "فایل 'ضرایب مدیر.xlsx' را آپلود کنید", type=['xls', 'xlsx'])
    uploaded_nafar_edare = st.file_uploader(
        "فایل 'نفر-اداره.xls' را آپلود کنید", type=['xls', 'xlsx'])

    if uploaded_ezafe and uploaded_refahi and uploaded_zarayeb and uploaded_nafar_edare:
        df_ezafe = pd.read_excel(uploaded_ezafe)
        df_refahi = pd.read_excel(uploaded_refahi)
        df_zarayeb = pd.read_excel(uploaded_zarayeb)
        df_nafar_edare = pd.read_excel(uploaded_nafar_edare)

        default_ezafe_refahi_columns = 'شماره کارمند'
        default_nafar_edare_column = 'شماره کارمند'
        default_zarayeb_column = 'اداره'

        ezafe_refahi_column = st.selectbox("ستون مورد نظر برای ادغام 'اضافه کار تیر.xls' و 'رفاهی تیر.xls' را انتخاب کنید:",
                                           df_ezafe.columns, index=df_ezafe.columns.get_loc(default_ezafe_refahi_columns))
        nafar_edare_column = st.selectbox("ستون مورد نظر برای ادغام با 'نفر-اداره.xls' را انتخاب کنید:",
                                          df_nafar_edare.columns, index=df_nafar_edare.columns.get_loc(default_nafar_edare_column))
        zarayeb_column = st.selectbox("ستون مورد نظر برای ادغام با 'ضرایب مدیر.xlsx' را انتخاب کنید:",
                                      df_zarayeb.columns, index=df_zarayeb.columns.get_loc(default_zarayeb_column))

        df_ezafe = df_ezafe[[ezafe_refahi_column,
                             'نام کارمند', 'نرخ اضافه کار']]
        df_refahi = df_refahi[[ezafe_refahi_column, 'نرخ رفاهی']]

        lst_ezafe_refahi = [df_ezafe, df_refahi]
        df_merged_ezafe_refahi = reduce(lambda left, right: pd.merge(
            left, right, on=ezafe_refahi_column, how='outer'), lst_ezafe_refahi)

        lst_merged_ezafe_refahi_edare = [
            df_merged_ezafe_refahi, df_nafar_edare]
        df_merged_ezafe_refahi_edare = reduce(lambda left, right: pd.merge(
            left, right, on=nafar_edare_column, how='left'), lst_merged_ezafe_refahi_edare)

        lst_merged_ezafe_refahi_edare_zarayeb = [
            df_merged_ezafe_refahi_edare, df_zarayeb]
        df_final = reduce(lambda left, right: pd.merge(left, right, on=zarayeb_column, how='left'),
                          lst_merged_ezafe_refahi_edare_zarayeb).drop_duplicates(subset=[ezafe_refahi_column])

        df_final[ezafe_refahi_column] = df_final[ezafe_refahi_column].astype(
            str)

        # def calculate_ezafe(x):
        #     try:
        #         score = x['سرانه اضافه کار']
        #         return score
        #     except ValueError:
        #         return 0

        # def calculate_refahi(x):
        #     try:
        #         score = x['سرانه رفاهی']
        #         return score
        #     except ValueError:
        #         return 0

        # df_final['مبلغ_اضافه_کار_اولیه'] = df_final.apply(
        #     lambda row: calculate_ezafe(row), axis=1)
        # df_final['مبلغ_رفاهی_اولیه'] = df_final.apply(
        #     lambda row: calculate_refahi(row), axis=1)

        cols_df_final = ['اداره', 'شماره کارمند',
                         'نام کارمند', 'نرخ اضافه کار', 'نرخ رفاهی']

        df_final['اداره'] = df_final['اداره'].fillna('نامشخص')

        grouped_df = df_final.groupby('اداره', dropna=False).agg({
            'شماره کارمند': 'count',
            'سرانه اضافه کار': 'first',
            'سرانه رفاهی': 'first'
        }).reset_index()
        grouped_df = grouped_df.fillna('--')

        grouped_df.rename(columns={'شماره کارمند': 'تعداد'}, inplace=True)
        # grouped_df['اداره'] = grouped_df['اداره'].fillna('نامشخص')

        st.write("جدول جزئیات:")
        st.dataframe(df_final[cols_df_final])

        st.write("جدول گروه‌بندی‌شده:")
        st.dataframe(grouped_df)

        def to_excel(df, file_name):
            with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
                df.to_excel(writer, index=False)
                worksheet = writer.sheets['Sheet1']
                worksheet.right_to_left()
                workbook = writer.book
                number_format = workbook.add_format({'num_format': '#,##0'})
                numeric_columns = [i for i, col in enumerate(
                    df.columns) if col not in ['شماره کارمند', 'اداره']]
                for col_num in numeric_columns:
                    worksheet.set_column(col_num, col_num, None, number_format)
            return file_name

        # Generate Excel file with a sheet for each department
        def to_excel_multiple_sheets_ezafe(df, grouped_df, file_name):
            header_text = """
            توجه: متذکر می‌گردد صرفاً ستون‌های مربوط به ساعت اضافه کار و درصد رفاهی تکمیل گردد.
            سقف اضافه کار همکاران مشاغل کارگری حداکثر 120 ساعت می‌باشد.
            ساعت اضافه کار و ضریب رفاهی بین 0 تا 29، صفر محاسبه می‌گردد.
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

                number_format = workbook.add_format({'num_format': '#,##0'})

                for edareh in df['اداره'].unique():
                    sheet_name = str(edareh)[:31]
                    worksheet = workbook.add_worksheet(sheet_name)
                    worksheet.right_to_left()
                    worksheet.merge_range('A1:L4', header_text, header_format)

                    df_filtered = df[df['اداره'] == edareh]
                    columns = df_filtered.columns.tolist(
                    ) + ['ساعت اضافه کار', 'درصد رفاهی', 'مبلغ اضافه', 'مبلغ رفاهی']

                    for i, col in enumerate(columns):
                        worksheet.write(4, i, col)

                    for row_num, row_data in enumerate(df_filtered.values):
                        for col_num, cell_data in enumerate(row_data):
                            worksheet.write(row_num + 5, col_num, cell_data)

                    start_row = 5
                    for row_num in range(start_row, start_row + len(df_filtered)):
                        worksheet.write(row_num, len(df_filtered.columns), '')
                        worksheet.write(row_num, len(
                            df_filtered.columns) + 1, '')

                        formula_ezafe = f"=IF(F{row_num+1}=\"\",\"\",F{row_num+1}*D{row_num+1})"
                        worksheet.write_formula(row_num, len(
                            df_filtered.columns) + 2, formula_ezafe)

                        formula_refahi = f"=IF(G{row_num+1}=\"\",\"\",G{row_num+1}*E{row_num+1}/100)"
                        worksheet.write_formula(row_num, len(
                            df_filtered.columns) + 3, formula_refahi)

                        total_row = start_row + len(df_filtered)
                        worksheet.write(total_row, len(df_filtered.columns) + 2,
                                        f'=SUM({chr(65 + len(df_filtered.columns) + 2)}{start_row+1}:{chr(65 + len(df_filtered.columns) + 2)}{total_row})', number_format)
                        worksheet.write(total_row, len(df_filtered.columns) + 3,
                                        f'=SUM({chr(65 + len(df_filtered.columns) + 3)}{start_row+1}:{chr(65 + len(df_filtered.columns) + 3)}{total_row})', number_format)

                    grouped_data = grouped_df[grouped_df['اداره'] == edareh]
                    if not grouped_data.empty:
                        مبلغ_اضافه_کار_اولیه = grouped_data['سرانه اضافه کار'].values[0]
                        مبلغ_رفاهی_اولیه = grouped_data['سرانه رفاهی'].values[0]

                        worksheet.write(4, 12, 'کل مبلغ قابل توزیع اضافه کار')
                        worksheet.write(4, 13, 'کل مبلغ قابل توزیع رفاهی')

                        worksheet.write(
                            5, 12, مبلغ_اضافه_کار_اولیه, number_format)
                        worksheet.write(5, 13, مبلغ_رفاهی_اولیه, number_format)

                    for col_num in range(3, 11):
                        worksheet.set_column(
                            col_num, col_num, None, number_format)

            return file_name

        detailed_output_file = to_excel(
            df_final[cols_df_final], 'detailed_data.xlsx')
        grouped_output_file = to_excel(grouped_df, 'grouped_data.xlsx')
        detailed_sheets_file = to_excel_multiple_sheets_ezafe(
            df_final[cols_df_final], grouped_df, 'detailed_data_sheets.xlsx')

        with open(detailed_output_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های جزئیات",
                data=file,
                file_name="detailed_data.xlsx",
                mime="application/vnd.ms-excel"
            )

        with open(grouped_output_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های گروه‌بندی‌شده",
                data=file,
                file_name="grouped_data.xlsx",
                mime="application/vnd.ms-excel"
            )

        with open(detailed_sheets_file, 'rb') as file:
            st.download_button(
                label="دانلود داده‌های جزئیات به همراه شیت‌ها",
                data=file,
                file_name="detailed_data_sheets.xlsx",
                mime="application/vnd.ms-excel"
            )
