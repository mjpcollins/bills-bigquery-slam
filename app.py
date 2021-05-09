from flask import Flask, request
from config.conf import settings
from utils import TableData, XLSplitter
app = Flask(__name__)


@app.route('/')
def home():
    return 'OK'


@app.route('/upload', methods=['POST'])
def upload():
    req = request.json
    if req['uri'].split('.')[-1] in ['xls', 'xlsx']:
        x = XLSplitter(req)
        x.split()
        tables = x.get_table_data()
    else:
        tables = [TableData(project=req['project'],
                            dataset=req['dataset'],
                            name=req['name'],
                            uri=req['uri'],
                            bucket=req['bucket'])]
    for table in tables:
        table.upload_to_bq()
    return 'OK'


def run():
    app.run(host='0.0.0.0',
            port=settings['port'])


def debug():
    app.run(host='0.0.0.0',
            port='5000')


if __name__ == '__main__':
    run()
