import streamlit as st
import pandas as pd
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
    </style>
    """,
    unsafe_allow_html=True
)


st.title('برنامه آپلود و ادغام داده‌ها')

# آپلود فایل‌ها
uploaded_ezafe = st.file_uploader(
    "فایل 'اضافه کار تیر.xls' را آپلود کنید", type=['xls', 'xlsx'])
uploaded_refahi = st.file_uploader(
    "فایل 'رفاهی تیر.xls' را آپلود کنید", type=['xls', 'xlsx'])
uploaded_zarayeb = st.file_uploader(
    "فایل 'ضرایب مدیر.xlsx' را آپلود کنید", type=['xls', 'xlsx'])
uploaded_nafar_edare = st.file_uploader(
    "فایل 'نفر-اداره.xls' را آپلود کنید", type=['xls', 'xlsx'])

if uploaded_ezafe and uploaded_refahi and uploaded_zarayeb and uploaded_nafar_edare:
    # خواندن فایل‌های آپلود شده
    df_ezafe = pd.read_excel(uploaded_ezafe)
    df_refahi = pd.read_excel(uploaded_refahi)
    df_zarayeb = pd.read_excel(uploaded_zarayeb)
    df_nafar_edare = pd.read_excel(uploaded_nafar_edare)

    # ستون‌های پیش‌فرض برای ادغام
    default_ezafe_refahi_columns = 'شماره کارمند'
    default_nafar_edare_column = 'شماره کارمند'
    default_zarayeb_column = 'اداره'

    # انتخاب ستون‌ها برای ادغام توسط کاربر
    ezafe_refahi_column = st.selectbox("ستون مورد نظر برای ادغام 'اضافه کار تیر.xls' و 'رفاهی تیر.xls' را انتخاب کنید:",
                                       df_ezafe.columns, index=df_ezafe.columns.get_loc(default_ezafe_refahi_columns))
    nafar_edare_column = st.selectbox("ستون مورد نظر برای ادغام با 'نفر-اداره.xls' را انتخاب کنید:",
                                      df_nafar_edare.columns, index=df_nafar_edare.columns.get_loc(default_nafar_edare_column))
    zarayeb_column = st.selectbox("ستون مورد نظر برای ادغام با 'ضرایب مدیر.xlsx' را انتخاب کنید:",
                                  df_zarayeb.columns, index=df_zarayeb.columns.get_loc(default_zarayeb_column))

    # فیلتر کردن ستون‌های مورد نیاز
    df_ezafe = df_ezafe[[ezafe_refahi_column, 'نام کارمند', 'نرخ اضافه کار']]
    df_refahi = df_refahi[[ezafe_refahi_column, 'نرخ رفاهی']]

    # انجام عملیات ادغام
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

    # تبدیل ستون 'شماره کارمند' به رشته برای جلوگیری از جدا شدن با کاما
    df_final[ezafe_refahi_column] = df_final[ezafe_refahi_column].astype(str)

    # تابع محاسبه 'مبلغ اضافه کار اولیه'
    def calculate_ezafe(x):
        try:
            score = math.ceil(x['سرانه اضافه کار'] * x['نرخ اضافه کار'])
            return score
        except ValueError:
            return 0

    # تابع محاسبه 'مبلغ رفاهی اولیه'
    def calculate_refahi(x):
        try:
            score = math.ceil((x['سرانه رفاهی'] * x['نرخ رفاهی']) / 100)
            return score
        except ValueError:
            return 0

    # اعمال محاسبات
    df_final['مبلغ اضافه کار اولیه'] = df_final.apply(
        lambda row: calculate_ezafe(row), axis=1)
    df_final['مبلغ رفاهی اولیه'] = df_final.apply(
        lambda row: calculate_refahi(row), axis=1)

    # انتخاب ستون‌ها برای نمایش جزئیات
    cols_df_final = ['اداره', 'شماره کارمند',
                     'نام کارمند', 'نرخ اضافه کار', 'نرخ رفاهی']

    # گروه‌بندی بر اساس 'اداره' و تجمیع داده‌ها
    grouped_df = df_final.groupby('اداره', dropna=False).agg({
        'مبلغ اضافه کار اولیه': 'sum',
        'مبلغ رفاهی اولیه': 'sum',
        'شماره کارمند': 'count',  # شمارش ردیف‌ها در هر گروه
        'سرانه اضافه کار': 'first',  # اضافه کردن اولین مقدار 'سرانه اضافه کار'
        'سرانه رفاهی': 'first'  # اضافه کردن اولین مقدار 'سرانه رفاهی'
    }).reset_index()

    # تغییر نام ستون 'شماره کارمند' به 'تعداد'
    grouped_df.rename(columns={'شماره کارمند': 'تعداد'}, inplace=True)

    # پر کردن مقادیر NaN در ستون 'اداره' با 'نامشخص'
    grouped_df['اداره'] = grouped_df['اداره'].fillna('نامشخص')

    # نمایش جدول جزئیات
    st.write("جدول جزئیات:")
    st.dataframe(df_final[cols_df_final])

    # نمایش جدول گروه‌بندی‌شده
    st.write("جدول گروه‌بندی‌شده:")
    st.dataframe(grouped_df)

    # لینک دانلود برای نتیجه نهایی
    def to_excel(df, file_name):
        with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
            worksheet = writer.sheets['Sheet1']
            worksheet.right_to_left()

            # فرمت اعداد برای ستون‌های عددی (غیر از 'شماره کارمند' و 'اداره')
            workbook = writer.book
            number_format = workbook.add_format({'num_format': '#,##0'})

            # اعمال فرمت برای تمامی ستون‌های عددی به جز 'شماره کارمند' و 'اداره'
            numeric_columns = [i for i, col in enumerate(
                df.columns) if col not in ['شماره کارمند', 'اداره']]
            for col_num in numeric_columns:
                worksheet.set_column(col_num, col_num, None, number_format)

        return file_name

    # ایجاد فایل Excel با یک شیت برای هر اداره
    def to_excel_multiple_sheets(df, grouped_df, file_name):
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

            # فرمت اعداد برای ستون‌های عددی
            number_format = workbook.add_format({'num_format': '#,##0'})

            # ایجاد یک شیت برای هر اداره
            for edareh in df['اداره'].unique():
                sheet_name = str(edareh)[:31]
                worksheet = workbook.add_worksheet(sheet_name)

                # تنظیم جهت شیت به راست‌چین
                worksheet.right_to_left()

                # نوشتن متن بالای شیت
                worksheet.merge_range('A1:L4', header_text, header_format)

                # گرفتن داده‌های فیلتر شده برای این اداره
                df_filtered = df[df['اداره'] == edareh]
                columns = df_filtered.columns.tolist(
                ) + ['ساعت اضافه کار', 'درصد رفاهی', 'مبلغ اضافه', 'مبلغ رفاهی']

                # نوشتن ستون‌های داده
                for i, col in enumerate(columns):
                    worksheet.write(4, i, col)

                # نوشتن ردیف‌های داده
                for row_num, row_data in enumerate(df_filtered.values):
                    for col_num, cell_data in enumerate(row_data):
                        worksheet.write(row_num + 5, col_num, cell_data)

                # اضافه کردن ستون‌های محاسباتی
                start_row = 5
                for row_num in range(start_row, start_row + len(df_filtered)):
                    # ساعت اضافه کار و درصد رفاهی (ورودی کاربر)
                    worksheet.write(row_num, len(df_filtered.columns), '')
                    worksheet.write(row_num, len(
                        df_filtered.columns) + 1, '')  # درصد رفاهی

                    # محاسبه مبلغ اضافه: (ساعت اضافه کار * نرخ اضافه کار)
                    formula_ezafe = f"=IF(F{row_num+1}=\"\",\"\",F{row_num+1}*D{row_num+1})"
                    worksheet.write_formula(row_num, len(
                        df_filtered.columns) + 2, formula_ezafe)

                    # محاسبه مبلغ رفاهی: (درصد رفاهی * نرخ) / 100
                    formula_refahi = f"=IF(G{row_num+1}=\"\",\"\",G{row_num+1}*E{row_num+1}/100)"
                    worksheet.write_formula(row_num, len(
                        df_filtered.columns) + 3, formula_refahi)

                # گرفتن داده‌های تجمیع‌شده برای این اداره از grouped_df
                grouped_data = grouped_df[grouped_df['اداره'] == edareh]

                if not grouped_data.empty:
                    مبلغ_اضافه_کار_اولیه = grouped_data['مبلغ اضافه کار اولیه'].values[0]
                    مبلغ_رفاهی_اولیه = grouped_data['مبلغ رفاهی اولیه'].values[0]

                    # نوشتن سرخط‌ها در ستون‌های M و N
                    worksheet.write(4, 12, 'کل مبلغ قابل توزیع اضافه کار')
                    worksheet.write(4, 13, 'کل مبلغ قابل توزیع رفاهی')

                    # نوشتن مقادیر تجمیع‌شده در اولین ردیف داده
                    worksheet.write(5, 12, مبلغ_اضافه_کار_اولیه, number_format)
                    worksheet.write(5, 13, مبلغ_رفاهی_اولیه, number_format)

                # فرمت‌دهی سایر ستون‌های عددی با کاما
                for col_num in range(3, 11):  # ستون‌های D تا K (به جز 'شماره کارمند')
                    worksheet.set_column(col_num, col_num, None, number_format)

        return file_name

    detailed_output_file = to_excel(
        df_final[cols_df_final], 'detailed_data.xlsx')
    grouped_output_file = to_excel(grouped_df, 'grouped_data.xlsx')
    detailed_sheets_file = to_excel_multiple_sheets(
        df_final[cols_df_final], grouped_df, 'detailed_data_sheets.xlsx')

    with open(detailed_output_file, 'rb') as file:
        btn1 = st.download_button(
            label="دانلود داده‌های جزئیات",
            data=file,
            file_name="detailed_data.xlsx",
            mime="application/vnd.ms-excel"
        )

    with open(grouped_output_file, 'rb') as file:
        btn2 = st.download_button(
            label="دانلود داده‌های گروه‌بندی‌شده",
            data=file,
            file_name="grouped_data.xlsx",
            mime="application/vnd.ms-excel"
        )

    with open(detailed_sheets_file, 'rb') as file:
        btn3 = st.download_button(
            label="دانلود داده‌های جزئیات به همراه شیت‌ها",
            data=file,
            file_name="detailed_data_sheets.xlsx",
            mime="application/vnd.ms-excel"
        )
