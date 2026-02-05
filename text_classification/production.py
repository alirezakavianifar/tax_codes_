import pickle
import numpy as np
import pandas as pd
import streamlit as st
import time
from io import BytesIO
import os
import zipfile
import shutil
import seaborn as sns
from automation.helpers import zipdir, df_to_excelworkbooks
from automation.codeeghtesadi import code_eghtesadi as codeghtesadi
# from automation.oracle import run_it as oracle


def test():
    time.sleep(5)


def change_state(state):
    state = True
    return state


def refresh_page(sessions):
    for key in sessions.keys():
        del sessions[key]


def convert_file(df, type='xlsx'):
    if type == 'xlsx':
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        return output

    if type == 'csv':
        return df.to_csv(index=False)


if __name__ == '__main__':
    radio_btns = ['Predict edare', 'Divide']
    st.title("This is a demo program")
    st.sidebar.write("Utilities")

    st.session_state['change'] = False
    st.session_state['selected_opt'] = st.sidebar.radio('Select option', radio_btns, horizontal=True,
                                                        on_change=lambda: change_state(st.session_state['change']))
    # Create a button
    if st.sidebar.button("Dadrasi"):
        # Code to execute when the button is clicked
        test()
        # codeghtesadi(get_dadrasi=True)
        st.success("Button was clicked!")

    st.write("Upload your file")

    st.session_state['file'] = st.file_uploader(
        'please upload an excel file', type=['xlsx'])

    if ((st.session_state['file'] is not None) and ('df' not in st.session_state)):

        st.session_state['df'] = pd.read_excel(st.session_state['file'])
        st.session_state['df'] = st.session_state['df'].astype('str')
        st.session_state['df'].fillna(
            'نامشخص', inplace=True)
        st.dataframe(st.session_state['df'])

    if ('df' in st.session_state):
        st.session_state['col_names'] = st.multiselect(
            'Columns', st.session_state['df'].columns)

        if len(st.session_state['col_names']) > 0:

            if st.session_state['selected_opt'] == 'Predict edare':
                with open(r'E:\automating_reports_V2\saved_model_ngram1_4_retrain_', 'rb') as f:
                    model = pickle.load(f)
                st.session_state['df']['edare'] = model.predict(
                    st.session_state['df'][st.session_state['col_names'][0]].to_numpy())
                st.session_state['df'][st.session_state['col_names'][0]] = st.session_state['df'][st.session_state['col_names'][0]].apply(
                    lambda x: 'نامشخص' if x == 'nan' else x)
                st.session_state['df'].loc[st.session_state['df']
                                           [st.session_state['col_names'][0]] == 'نامشخص', 'edare'] = 'نامشخص'
                st.session_state['df'] = convert_file(
                    st.session_state['df'], type='xlsx')
                st.download_button('Download xlsx', data=st.session_state['df'],
                                   on_click=lambda: refresh_page(
                    st.session_state),
                    file_name='final.xlsx')

            if st.session_state['selected_opt'] == 'Divide':
                df_to_excelworkbooks(
                    st.session_state['df'], st.session_state['col_names'])
                with open(r'E:\automating_reports_V2\saved_dir\arzeshafzoodeh_sonati\test\final\final.zip', 'rb') as f:
                    st.download_button('Download Zip', f,
                                       file_name='final.zip',
                                       on_click=lambda: refresh_page(
                                           st.session_state),
                                       mime='application/zip')

            if st.session_state['selected_opt'] == 'report':
                grouped_df = st.session_state['df'].groupby(
                    st.session_state['col_names']).size().reset_index()

                col_names = grouped_df.columns.tolist()

                st.bar_chart(grouped_df, x=col_names[0], y=col_names[1])
