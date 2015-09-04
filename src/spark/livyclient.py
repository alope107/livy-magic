import json, pprint, requests, textwrap, time
class LivyClient(object):
    def __init__(self, host="http://localhost:8998", kind='spark'):
        self.host = host
        self.headers = {'Content-Type': 'application/json'}
        self.data = {'kind': kind}
        self.statement_count = 0
        self.session_id = None
        self.connected = False
        
    def connect(self):
        r = requests.post(self.host + '/sessions', data=json.dumps(self.data), headers=self.headers)
        self.session_id = r.json()["id"]
        self.session_url = self.host + "/sessions" + r.headers['location']
        self.statement_url = self.session_url + "/statements"
        time.sleep(3) #figure out correct way
        self.connected = True
        
    def execute(self, code, poll_interval=1):
        if not self.connected:
            self.connect()
        data = {'code': code}
        
        r = requests.post(self.statement_url, data=json.dumps(data), headers=self.headers)
        result = r.json()
        while result["state"] == "running":
            time.sleep(poll_interval)
            r = requests.get(self.statement_url + "/" + str(self.statement_count), headers=self.headers)
            result = r.json()
        self.statement_count += 1
        return result["output"]["data"]["text/plain"]
