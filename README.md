![](https://raw.githubusercontent.com/S0mbre/russtat/master/icons/main.ico)

# russtat: Python / PostgreSQL access to the Russian Federal statistics
*russtat* utilizes the power of Python to download and process the massive public data release by the Russian Statistics Office from the [EMISS website](https://fedstat.ru/). The original XML-formatted datasets (close to 7,000 in total) are parsed and saved as JSON files which can then be fed into a ready-made PostgreSQL database to give additional power. Alternativaly, you can utilize the JSON files with your own software, in any way you like!

## Download
*russtat* source code and documentation are hosted on [Github](https://github.com/S0mbre/russtat)

## Features:
* parallel non-blocking data retrieval and processing using the multiprocessing library
* fool-proof XML parsing with default values for missing data, string trimming, type conversions and exception handling
* ready SQL script to create the PostgreSQL database from scratch
* database routine handling
* extensive API documentation with Doxygen

## Installation

### Requirements
You must have the following applications / packages installed in your system:

* Python 3.7+ (the app was written and tested with Python 3.8)
* Python packages: 
	- pip
	- psycopg2
* Git (should be pre-installed on most modern Linux and Mac systems, alternatively install from the [git website](https://git-scm.com/downloads))

#### 1. Clone repo

  To get the latest version from Github, run:
  ```bash
  git clone https://github.com/S0mbre/russtat .
  ```
  
#### 2. Install the required packages

  I recommend (as many do) installing packages into python's virtual environment using *virtualenv* or the inbuilt *venv*:
  
  Create a new virtual environment (assuming your projects root folder is 'myprojects'):
  
  **Linux / Mac**
  ```bash
  cd myprojects
  venv russtat
  cd russtat
  . ./bin/activate
  ```
  
  **Windows**
  ```bash
  cd myprojects
  venv russtat
  cd russtat
  scripts\activate.bat
  ```
  
  This step is, of course, optional. You can skip it if you don't want to use virtual environments for some reason or other. 
  
  Then just run:
  ```bash
  cd russtat
  python -m pip install -r requirements.txt
  ```
  
  If you're using a virtual environment, you can deactivate it after closing the app with `deactivate`.

## Usage
Run `python russtat.py` to start the application. Please modify the `main()` function is that file first to suit your purpose!

See the documentation in russtat/doc/ref to find out more!