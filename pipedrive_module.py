import requests
import json

PIPEDRIVE_TOKEN="insert_your_token_here"
COMPANY_DOMAIN_NAME="insert_your_company_domain_name_here"

#GET
def get_stages():
    url="https://"+COMPANY_DOMAIN_NAME+".pipedrive.com/api/v1/stages?api_token="+PIPEDRIVE_TOKEN
    response=requests.get(url)
    print(response)
    print(response.content)
    return(response.content)
   
def get_users():
    url="https://"+COMPANY_DOMAIN_NAME+".pipedrive.com/api/v1/users?api_token="+PIPEDRIVE_TOKEN
    
    response=requests.get(url)
    print(response)
    print(response.content)
    return(response.content)
 
#ADD (POST)
def add_person(name,email,phone):
    url="https://"+COMPANY_DOMAIN_NAME+".pipedrive.com/api/v1/persons?api_token="+PIPEDRIVE_TOKEN
      
    payload={"name":name,"email":email,"phone":phone}
    
    
    response=requests.post(url,data=payload)
    
    print(response)
    print(response.content)
    return(response.content)
    
def add_deal(title,stage_id,user_id,value=0,name="",email="",phone=""):
    """Missing link"""
    
    url="https://"+COMPANY_DOMAIN_NAME+".pipedrive.com/api/v1/deals?api_token="+PIPEDRIVE_TOKEN
    
    if name=="":
        name="Untitled"
    response_content=add_person(name,email,phone) #98
    person_id=json.loads(response_content)["data"]["id"]
              
    payload={"title":title,"stage_id":stage_id,"user_id":user_id,"value":value,"person_id":person_id}    
    response=requests.post(url,data=payload)
    
    print(response)
    print(response.content)
    return(response.content)
  
def add_note(content,deal_id):
    url="https://"+COMPANY_DOMAIN_NAME+".pipedrive.com/api/v1/notes?api_token="+PIPEDRIVE_TOKEN  
    payload={"content":content,"deal_id":deal_id}
    response=requests.post(url,data=payload)
    print(response)
    print(response.content)

#DELETE 
def delete_deal(deal_id):
    url="https://"+COMPANY_DOMAIN_NAME+".pipedrive.com/api/v1/deals/"+str(deal_id)+"?api_token="+PIPEDRIVE_TOKEN
    response=requests.delete(url)
    
    print(response)
    print(response.content)
    
