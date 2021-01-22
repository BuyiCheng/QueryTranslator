import sql_tool
import mongotl
import dataframetl
from flask import Flask, render_template, request, jsonify

app = Flask(__name__)


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/translate", methods=['GET', 'POST'])
def translate():
    tl_type = request.form['type']
    sql = request.form['sql']
    if sql_tool.validate(sql):
        sql_dict = sql_tool.parse_sql(sql)
    print("sql_dict:",sql_dict)
    if tl_type == 'MongoDB':
        result = mongotl.translate(sql_dict)
    elif tl_type == 'Pandas Dataframe':
        result = dataframetl.translate(sql_dict)
    return jsonify({'tl': result})


if __name__ == '__main__':
    app.run(host="0.0.0.0")