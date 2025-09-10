# 🌍 Scripts iPAVE

Python Project for versionning scripts, working on a virtual environement

`Python Version 3.13`

## Packages Dependencies

- **psycopg2-binary** for connection Databases
- **geopandas** for geospatial working datas
- **numpy** for adding algorithms & mathematics functions
- **requests** for HTTP requests manipulations

> 🚨 To prevent issues for automatic upgrade's issues, it's recommended to lock packages versions and check sometimes to upgrade them manually

## Setup (for Windows)

### A - Check if pip and pipenv are installed

To run the app on Windows, we need a python environnement (we're using pipenv) :

```shell
python --version    # Verify version
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py

pip --version       # Verify version
pip install pipenv
```

### B - Clone the project

```shell
git clone https://github.com/datagora-erasme/ipave-scripts.git
cd ipave-scripts
```

### C - Create .env configuration file

Copy the .env.EXAMPLE file to .env and set all variables needed for the project (ask to your Lead Developer for the configuration data)

### D - Install and Launch the app

```shell
pipenv --three        # Create Python virtual env
pipenv install -d     # Install all dependencies
pipenv shell          # Activate the env and get into
```

If you want to execute a script :
```shell
python ./scripts/sample.py
```

If you want to quit the virtual environement
```shell
exit                  # Kill the virtual environement (pipenv)
```

> 🚨 If you changes your `.env` file variables : you need to restart the Virtual Environement (pipenv)

## Configuration

### Architecture

- If you want to use `shapefile` or `geojson`, ... files to import datas, you have to put them on the directory : `0_geodatas/input/`
- All generated files needs to be saved on the directory : `0_geodatas/output/`
- These PATHs are available on the file `utils/contants.py` : **INPUT_DATAS_DIR** and **OUTPUT_DATAS_DIR**

### Files

- All your Python scripts need to be added on the `script/` folder.
- `.env.SAMPLE` file is a template to create the `.env` file
- `Pipfile` is the dependencies file configuration of the project
- `Pipfile.lock` (not versionnend) is the current dependencies packages installed on your application on the pipenv
- If you need to add new package, you can use the pipenv command :
  ```shell
  pipenv install new-package-name
  ```
  > - It'll install the last version and automaticaly add it on the dependencies configuration on `Pipfile`
  > - Then set the actual version of the package on `Pipfile` to lock it (use the command `pip freeze` or check the version on the `Pifile.lock` file)
- `utils/sample.py` is an example of a script with a connection to a DataBase and a SQL Query Select
- `utils/utils.py` is an internal functions librairies (wrote and added by Exo-Dev) : you can auto import all of them by adding the import line `from utils.utils import *`

⚠️ If Python doesn't find **utils files** : you have to add the path like this :
```python
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# ...
from utils.functions import format_elapsed_time
```

## Develop by Exo-Dev

```
            /$$       /$$$$$$$$                                         /$$$$$$$                             /$$   
           /$$/      | $$_____/                                        | $$__  $$                           |  $$  
          /$$/       | $$       /$$   /$$  /$$$$$$                     | $$  \ $$  /$$$$$$  /$$    /$$       \  $$ 
         /$$/        | $$$$$   |  $$ /$$/ /$$__  $$       /$$$$$$      | $$  | $$ /$$__  $$|  $$  /$$/        \  $$
        |  $$        | $$__/    \  $$$$/ | $$  \ $$      |______/      | $$  | $$| $$$$$$$$ \  $$/$$/          /$$/
         \  $$       | $$        >$$  $$ | $$  | $$                    | $$  | $$| $$_____/  \  $$$/          /$$/ 
          \  $$      | $$$$$$$$ /$$/\  $$|  $$$$$$/                    | $$$$$$$/|  $$$$$$$   \  $/          /$$/  
           \__/      |________/|__/  \__/ \______/                     |_______/  \_______/    \_/          |__/   
```