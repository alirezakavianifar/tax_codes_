import pyodbc
import pandas as pd
import streamlit as st

# اطلاعات اتصال به SQL Server
SERVER = '10.52.0.114'
DATABASE = 'test2'
USERNAME = 'sa'
PASSWORD = '14579Ali...'

# تابع برای ایجاد اتصال به SQL Server


def create_connection():
    conn_str = (
        f'DRIVER={{SQL Server}};'
        f'SERVER={SERVER};'
        f'DATABASE={DATABASE};'
        f'UID={USERNAME};'
        f'PWD={PASSWORD};'
    )
    return pyodbc.connect(conn_str)

# تابع برای بارگذاری داده‌ها از پایگاه داده


def load_data():
    conn = create_connection()
    query = "SELECT * FROM tblParvanehayeMohem_Ghabz"
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# تابع برای ذخیره داده‌ها به پایگاه داده با استفاده از تراکنش


def save_data(df):
    conn = create_connection()
    cursor = conn.cursor()

    try:
        # شروع تراکنش
        conn.autocommit = False

        # حذف داده‌های قبلی
        cursor.execute("DELETE FROM tblParvanehayeMohem_Ghabz")

        # افزودن داده‌های جدید
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO tblParvanehayeMohem_Ghabz (fish,disc) VALUES (?, ?)",
                row['fish'], row['disc']
            )

        # تایید تراکنش
        conn.commit()
    except Exception as e:
        conn.rollback()
        st.error(f"خطا در ذخیره‌سازی: {str(e)}")
    finally:
        conn.close()

# تابع اصلی برای اجرای برنامه


def main():
    st.title('ویرایش اطلاعات پایگاه داده SQL Server (چند کاربره)')

    # بارگذاری داده‌ها از پایگاه داده
    df = load_data()

    # نمایش داده‌ها به صورت جدول و امکان ویرایش آنها
    st.write("اطلاعات پایگاه داده:")
    edited_df = st.data_editor(df, use_container_width=True)

    # دکمه ذخیره تغییرات
    if st.button("ذخیره تغییرات"):
        save_data(edited_df)
        st.success("تغییرات با موفقیت ذخیره شد")


if __name__ == "__main__":
    main()
