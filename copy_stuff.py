import os
import shutil


def copy_python_files(src_dir, dest_dir):
    for root, dirs, files in os.walk(src_dir):
        # Skip the 'venv' directory
        if 'venv' in dirs:
            dirs.remove('venv')

        for file in files:
            if file.endswith('.py'):
                full_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(full_file_path, src_dir)
                target_file_path = os.path.join(dest_dir, relative_path)

                os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
                shutil.copy2(full_file_path, target_file_path)
                print(f'Copied {full_file_path} to {target_file_path}')


# Define the source and target directories
source_directory = r'E:\automating_reports_V2'
target_directory = r'E:\final'

# Call the function to copy the files
copy_python_files(source_directory, target_directory)
