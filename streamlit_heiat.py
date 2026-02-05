import streamlit as st
import subprocess
import threading
import time


def run_sbprocess():
    process = subprocess.Popen(['sleep', '10'])

    process.wait()

    notification_placeholder.text("Subprocess has finished!")


st.title("Subprocesss notification in streamlit")

notification_placeholder = st.empty()

if st.button("Start Subprocess"):
    notification_thread = threading.Thread(target=run_sbprocess)
    notification_thread.start()

    notification_placeholder.text("Subprocess is running...")
