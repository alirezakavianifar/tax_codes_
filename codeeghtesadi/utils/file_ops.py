import os
import zipfile
import shutil
import glob
from codeeghtesadi.utils.decorators import log_the_func

def list_files(directory, extension):
    return [it.path for it in os.scandir(directory) if it.is_file() and it.name.endswith('.' + extension)]

def unzip_files(dir, remove=True):
    # Step 1: Find all subdirectories in the specified directory
    dirs = [it.path for it in os.scandir(dir) if it.is_dir()]

    # Step 2: Find subdirectories within each subdirectory (nested directories)
    final_dirs = dirs
    for item in dirs:
        final_dirs.extend([it.path for it in os.scandir(item) if it.is_dir()])

    # Step 3: Iterate through all found directories
    for item in final_dirs:
        # Step 4: Find all zip files in the current directory
        files = list_files(item, 'zip')

        # Step 5: If zip files are found, unzip each file
        if len(files) > 0:
            for file in files:
                with zipfile.ZipFile(file, 'r') as zipObj:
                    # Extract all contents of the zip file into the current directory
                    zipObj.extractall(item)

            # Step 6: If 'remove' is True, delete the original zip files
            if remove:
                for file in files:
                    os.remove(file)

def zipdir(path, ziph):
    # Iterate over all the directories and files in the given path
    for (root, dirs, files) in os.walk(path):
        print(root)  # Print the current directory being processed
        for file in files:
            # Write each file to the zip file with a relative path
            ziph.write(
                os.path.join(root, file),
                os.path.relpath(os.path.join(root, file),
                                os.path.join(path, '..'))
            )

def get_file_size(filename):
    return os.stat(filename)

@log_the_func('none')
def move_files(srcs, dsts, *args, **kwargs):
    # Iterate over pairs of source and destination paths
    for src, dst in zip(srcs, dsts):
        # Move the file from source to destination
        shutil.move(src, dst)

def count_num_files(dir):
    """A python function which return the number of files in a directory"""
    count = 0
    for _, _, files in os.walk(dir):
        count += len(files)
    return count

def generate_readme(file_path):
    content = """# Project Title

Brief description of your project.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)
- [License](#license)

## Installation

Describe how to install your project.

## Usage

Provide instructions on how to use your project.

## Contributing

Explain how others can contribute to your project.

## License

This project is licensed under the [License Name] - see the [LICENSE.md](LICENSE.md) file for details.

```python
def example_function():
    # Your code here
    pass
"""

    with open(file_path, 'w') as readme_file:
        readme_file.write(content)

