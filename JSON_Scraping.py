'''
Created on Aug 17, 2016
@author: Nilam
'''
import simplejson as json
from urllib import request
import re
import psycopg2
import pandas as pd

df = pd.read_csv("F:\\Nilam\\MSBA_Degree\\CISI\\FDA Labels\\Set_ID.csv")
IDs = set(df.IDS)

#key - CsHHTgpPtAFpFtrQwL0UP4nqOxIg9NUeBIDSDKzp

#declaring Lists and dictonary 
ids_not_found =[]
namedic = ();

count = 0;
for setid in IDs: 
    count = count + 1;
    print("set id ", count)   
    #Set all variable values to NULL
    dic={}
    set_id ='NULL'
    version = 'NULL'
    Effective_time = 'NULL'
    Clinical_pharmocology = 'NULL'
    Mec_action = 'NULL'
    Indications_and_usage = 'NULL'
    description = 'NULL'
    Application_number = 'NULL'
    brand_name = 'NULL'
    generic_name ='NULL'
    manufacturer_name = 'NULL'
    pharm_class_cs = 'NULL'
    pharm_class_epc = 'NULL'
    pharm_class_pe = 'NULL'
    pharm_class_moa = 'NULL'
    
    #URL Request
    url = "https://api.fda.gov/drug/label.json?api_key=CsHHTgpPtAFpFtrQwL0UP4nqOxIg9NUeBIDSDKzp&search=set_id:" + setid 
    
    print(url)

    try:
        #SEnd URL request
        fp = request.urlopen(url)
        doc = json.loads(fp.read())
        
        #Extract required values from JSON file returned
        try:
            set_id =doc['results'][0]['set_id'];
        except:
            print('No set_id')
        try:
            version = doc['results'][0]['version']
        except:
            print('no version found')
        try:
            Effective_time = doc['results'][0]['effective_time']
        except:
            print('no Effective_time found')
        try:
            description = doc['results'][0]['description'][0]
        except:
            print('Description not found')
        try:
            Mec_action = doc['results'][0]['mechanism_of_action'][0]
        except:
            print('no Mec_action found')
        try:
            Indications_and_usage = doc['results'][0]['indications_and_usage'][0]
        except:
            print('no Indications_and_usage found')
        try: 
            App_number = doc['results'][0]['openfda']['application_number'][0]
            Application_number = re.findall('[0-9]+', App_number)[0] 
            drugType = re.findall('[a-zA-Z]+', App_number)[0]  
            if drugType not in ('NDA', 'ANDA','BLA'):
                ids_not_found.append(setid)
                print("coming here")
                continue;
        except:
            print('no application number found')                             
        try:
            brand_name = doc['results'][0]['openfda']['brand_name'][0]
        except:
            print('no brand name found')            
        try:
            generic_name = doc['results'][0]['openfda']['generic_name'][0]
        except:
            print('no generic name found')
        try: 
            manufacturer_name = doc['results'][0]['openfda']['manufacturer_name'][0]
        except:
            print('no manufacturer name found')
        try:
            pharm_class_cs = doc['results'][0]['openfda']['pharm_class_cs'][0]
        except:
            print('no pharm_class_cs found')
        try:
            pharm_class_epc = doc['results'][0]['openfda']['pharm_class_epc'][0]
        except:
            print('no pharm_class_epc found')    
        try:
            pharm_class_pe = doc['results'][0]['openfda']['pharm_class_pe'][0]
        except:
            print('no pharm_class_pe found')
        try:
            pharm_class_moa = doc['results'][0]['openfda']['pharm_class_moa'][0]  
        except:
            print('no pharm_class_moa found')
        try:
            Clinical_pharmocology = doc['results'][0]['clinical_pharmacology'][0]
        except:
            print('no Clinical_pharmocology found')
        
        print("app number", Application_number )             
        dic['Application_number'] = Application_number
        dic['set_id'] = set_id
        dic['version'] = version
        dic['Effective_time'] = Effective_time
        dic['Clinical_pharmocology'] = Clinical_pharmocology
        dic['Mec_action'] = Mec_action
        dic['Indications_and_usage'] = Indications_and_usage
        dic['description'] = description
        dic['brand_name'] = brand_name
        dic['generic_name'] = generic_name
        dic['manufacturer_name'] = manufacturer_name
        dic['pharm_class_cs'] = pharm_class_cs
        dic['pharm_class_epc'] = pharm_class_epc
        dic['pharm_class_pe'] = pharm_class_pe
        dic['pharm_class_moa'] = pharm_class_moa
        
    except Exception as e:
        print("in except", str(e))
        ids_not_found.append(setid)
    
    if dic !={}:
         if Application_number != 'NULL':
             namedic = namedic + (dic,)
         #print(namedic)


df=pd.DataFrame(ids_not_found)
df.to_csv("F:\\Nilam\\MSBA_Degree\\CISI\\FDA Labels\\ids_not_found.csv")

#Connect to Postgres Database
conn = psycopg2.connect(user="postgres", password="postgres", host="127.0.0.1", port="5432")
print ("Opened database successfully")

#Insert values into table
cur = conn.cursor()
cur.executemany("""INSERT INTO FDA_DB.Labels( set_id, Application_number,version, Effective_time, Clinical_pharmocology, Mec_action, Indications_and_usage, description, brand_name, generic_name, manufacturer_name, pharm_class_cs, pharm_class_epc, pharm_class_pe, pharm_class_moa) \
      VALUES (%(set_id)s, %(Application_number)s, %(version)s, %(Effective_time)s, %(Clinical_pharmocology)s, %(Mec_action)s, %(Indications_and_usage)s, %(description)s,%(brand_name)s, %(generic_name)s, %(manufacturer_name)s, %(pharm_class_cs)s, %(pharm_class_epc)s, %(pharm_class_pe)s, %(pharm_class_moa)s)""",
      namedic);

conn.commit()