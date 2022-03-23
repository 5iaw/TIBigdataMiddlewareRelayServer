import requests
import json
import account.BE_http as BE

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