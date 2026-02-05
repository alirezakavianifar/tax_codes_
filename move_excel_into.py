import os
import shutil

# Get the directory where the script is being run
source_dir = os.getcwd()

# Get a list of all files in the directory
files = os.listdir(source_dir)

# Loop through each file in the directory
for file in files:
    # Check if the file is an Excel file
    if file.endswith('.xlsx') or file.endswith('.xls'):
        # Create a new folder with the same name as the file (without the extension)
        folder_name = os.path.splitext(file)[0]
        folder_path = os.path.join(source_dir, folder_name)

        # Create the new folder
        os.mkdir(folder_path)

        # Move the file into the new folder
        shutil.move(os.path.join(source_dir, file),
                    os.path.join(folder_path, file))

print("All Excel files have been moved to their respective folders.")
