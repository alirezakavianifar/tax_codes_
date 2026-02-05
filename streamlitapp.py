# streamlit_app.py

import streamlit as st
import subprocess
import os

# Path to the directory containing the scripts
SCRIPTS_DIR = r'E:\automating_reports_V2\automation'
LOGS_DIR = r'E:\automating_reports_V2\automation\testfloder'


def get_scripts_list(directory):
    """Get a list of Python scripts in the specified directory."""
    return [f for f in os.listdir(directory) if f.endswith('.py')]


def run_script(script_path, *args):
    """Run the selected script asynchronously with arguments."""
    log_file = os.path.join(
        LOGS_DIR, f"{os.path.splitext(os.path.basename(script_path))[0]}.log")
    try:
        command = ['python', script_path] + list(args)
        with open(log_file, 'w') as log:
            subprocess.Popen(command, stdout=log, stderr=log)
        return True, log_file
    except Exception as e:
        return False, str(e)


def check_script_status(log_file):
    """Check if the script has completed by reading the log file."""
    if os.path.exists(log_file):
        with open(log_file, 'r') as file:
            logs = file.read()
            if "Script completed successfully." in logs:
                return "completed"
            elif "Script failed:" in logs:
                return "failed"
    return "running"


def main():
    st.title("Selenium Script Runner")

    # Get the list of scripts
    scripts = get_scripts_list(SCRIPTS_DIR)

    # Dropdown menu to select the script
    selected_script = st.selectbox("Select a script to run", scripts)

    # Input fields for script arguments
    get_dadrasi = st.text_input("Enter value for --get_dadrasi")
    saving_path = st.text_input("Enter value for --saving_path")

    # Button to run the script
    if st.button("Run Script"):
        script_path = os.path.join(SCRIPTS_DIR, selected_script)
        args = ['--get_dadrasi', get_dadrasi, '--saving_path', saving_path]
        success, log_file = run_script(script_path, *args)
        if success:
            st.success(f"Started script: {selected_script}")
            st.session_state['log_file'] = log_file
        else:
            st.error(
                f"Failed to start script: {selected_script}. Error: {log_file}")

    # Check script status
    if 'log_file' in st.session_state:
        log_file = st.session_state['log_file']
        status = check_script_status(log_file)
        if status == "completed":
            st.success("The script has completed successfully.")
        elif status == "failed":
            st.error("The script has failed. Check the log for details.")
        else:
            st.info("The script is still running. Please wait...")


if __name__ == "__main__":
    main()
