# 기존 flask에서 사용한 모듈 전체 적용
from flask import Flask, jsonify, request, Response, render_template, copy_current_request_context, current_app, abort

import requests
import json
import account.BE_flask as BE

# flask 객체
app = Flask(__name__) # 정적 파일과 템플릿을 찾는데 쓰인다고 한다. 무슨소리일까..

@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE')
    return response

@app.route('/preprocessing', methods=['GET', 'POST'])
def preprocessing():
    print("/preprocessing"+"request"+"recieved")
    if request.method == 'POST':
        data = request.json
        print(data)
    r = requests.post("https://"+BE.ip+":"+BE.port+"/preprocessing",verify=False, json =data).text
    return r

@app.route('/textmining', methods=['GET', 'POST'])
def textmining():
    print("/textming"+"request"+"recieved")
    if request.method == 'POST':
        data = request.json
        print(data)
    r = requests.post("https://"+BE.ip+":"+BE.port+"/textmining",verify=False, json =data).text
    return r

@app.route('/tfidfTable', methods=['GET', 'POST'])
def tfidfTable():
    #app = Flask(__name__)
    #app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True
    if request.method == 'POST':
            data = request.json 
    r = requests.post("https://"+BE.ip+":"+BE.port+"/tfidfTable",verify=False, json =data).text
    return r


@app.route('/test', methods=['GET', 'POST'])
def test():
    if request.method == 'GET':
        r = requests.post("https://"+BE.ip+":"+BE.port+"/preprocessing",verify=False, json = {
            "userEmail": "21800520@handong.edu",
            "keyword": "북한",
            "savedDate": "2021-09-07T07:01:07.137Z",
            "synonym": False,
            "stopword": False,
            "compound": False,
            "wordclass": "010"
        })

        print(r.text)

        r = requests.post("https://"+BE.ip+":"+BE.port+"/textmining",verify=False, json ={
            "userEmail": "21800520@handong.edu",
            "keyword": "북한",
            "savedDate": "2021-09-07T07:01:07.137Z",
            "option1": "100",
            "option2": None,
            "option3": None,
            "analysisName": "count"
            }).text

        print(r)
        return r
    elif request.method == 'POST':
        data = request.json

        return "사용자"+data["userEmail"]+"이(가) 확인되었습니다."

import account.FE_flask as FERS
import account.kubic_sslFile as kubic_ssl

if __name__ == "__main__":

    context=(kubic_ssl.crt,kubic_ssl.key)
    app.run(host=FERS.hostIp, port=FERS.port, ssl_context=context, debug=True)

