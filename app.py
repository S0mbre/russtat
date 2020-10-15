# -*- coding: utf-8 -*-
# Copyright: (c) 2020, Iskander Shafikov <s00mbre@gmail.com>
# GNU General Public License v3.0+ (see LICENSE.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# --------------------------------------------------------------- #

## @package russtat.app
# @brief Flask web app implementation.
from flask import Flask, redirect, url_for, request, render_template
from rsengine import Russtat
from psdb import Russtatdb

# --------------------------------------------------------------- #
app = Flask(__name__)
db = Russtatdb(password='Fknzoo2052')
# --------------------------------------------------------------- #

@app.route('/') 
def index():
    classificator = db.collect_classificator()
    return render_template('index.html', classifier=classificator)

# --------------------------------------------------------------- #

## Program entry point.
if __name__ == '__main__':
    app.run(debug=True)