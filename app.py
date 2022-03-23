import requests
import json
import account.BE_http as BE

# google = requests.get("http://google.com")
# print(google)


# print("http://"+BE.ip+":"+BE.port+"/textmining")

# r = requests.get("http://"+BE.ip+":"+"15044/test").text #request 객체를 문자로 변환.
# print(r) 

r = requests.post("http://"+BE.ip+":"+BE.port+"/preprocessing", json = {
    "userEmail": "21800520@handong.edu",
    "keyword": "북한",
    "savedDate": "2021-09-07T07:01:07.137Z",
    "synonym": False,
    "stopword": False,
    "compound": False,
    "wordclass": "010"
})

print(r.text)

r = requests.post("http://"+BE.ip+":"+BE.port+"/textmining", json ={
    "userEmail": "21800520@handong.edu",
    "keyword": "북한",
    "savedDate": "2021-09-07T07:01:07.137Z",
    "option1": "100",
    "option2": None,
    "option3": None,
    "analysisName": "count"
    }).text


print(r)