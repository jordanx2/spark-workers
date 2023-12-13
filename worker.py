from flask import Flask
from flask import request
import requests
import os
import json
from google.cloud import secretmanager
app = Flask(__name__)

def get_api_key() -> str:
    # Try to get the API key from environment variable first
    secret = os.environ.get("compute-api-key")
    if secret:
        return secret

    # If not found in environment variables, fetch from Secret Manager
    project_id = "cloudcomputelab6"  # Replace with your project ID
    secret_id = "compute-api-key"  # The secret name in Secret Manager

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(name=name)
    return response.payload.data.decode("UTF-8")
      
@app.route("/")
def hello():
    return "Add workers to the Spark cluster with a POST request to add"

@app.route("/test")
def test():
    # return "Test" # testing 
    return(get_api_key())

@app.route("/add",methods=['GET','POST'])
def add():
  if request.method=='GET':
    return "Use post to add" # replace with form template
  else:
    token=get_api_key()
    ret = addWorker(token,request.form['num'])
    return ret    

@app.route("/multiple", methods=['GET', 'POST'])
def addMultiple():
    if request.method == 'GET':
        return "Use post to add multiple" # replace with form template
    else:
        token = get_api_key()
        nums = request.form['nums'].split(',') 
        nums = [int(num) for num in nums]  
        ret = addMultipleWorkers(token, nums)  
        return ret

def addWorker(token, num):
    with open('payload.json') as p:
      tdata=json.load(p)
    tdata['name']='slave'+str(num)
    data=json.dumps(tdata)
    url='https://www.googleapis.com/compute/v1/projects/cloudcomputelab6/zones/europe-west1-b/instances'
    headers={"Authorization": "Bearer "+token}
    resp=requests.post(url,headers=headers, data=data)
    if resp.status_code==200:     
      return "Done"
    else:
      print(resp.content)
      return "Error\n"+resp.content.decode('utf-8') + '\n\n\n'+data


def addMultipleWorkers(token, nums):
  url = 'https://www.googleapis.com/compute/v1/projects/cloudcomputelab6/zones/europe-west1-b/instances'
  headers = {"Authorization": "Bearer " + token}
  results = []

  for num in nums:
      with open('payload.json') as p:
          tdata = json.load(p)
      tdata['name'] = 'slave' + str(num)
      data = json.dumps(tdata)
      resp = requests.post(url, headers=headers, data=data)
      if resp.status_code == 200:
          results.append(f"Slave {num}: Done")
      else:
          error_message = f"Slave {num}: Error\n{resp.content.decode('utf-8')}\n\n\n{data}"
          print(error_message)
          results.append(error_message)






if __name__ == "__main__":
    app.run(host='0.0.0.0',port='8080')
