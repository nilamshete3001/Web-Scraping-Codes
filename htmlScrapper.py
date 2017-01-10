'''
Created on Jan 19, 2016
@author: Nilam
'''
import lxml.html as PARSER
import pandas as pd
import os;

#Extract the list of files
data_root = "F:\\Nilam\\MSBA_Degree\\CISI\Feedstuff output\\Prices data\\Prices data\\"
files_in_dir = os.listdir(data_root)
countloop = 0;

for file in files_in_dir:
    countloop = countloop + 1
	#Read and parse data
    data = open(data_root + file).read()
    root = PARSER.fromstring(data)
	
	#Declare list of list objects
    prodList =list()
    catList=list()
    category=list()    
    col1 = list()
    col2 = list()
    city = list()
    price = list()
    Counter =0      
    categories =''   
    name =''    
	
	#Extract the Categories from file
    for ele in root.getiterator():
        if ele.tag == "tr":
            Counter += 1
            for col in ele.getiterator():
                if col.tag == "td":
                    for sub in col.getiterator():
                        if (sub.tag == "b" or sub.tag == "strong") and sub.text_content().isupper():                
                            cat = sub.text_content()
                            unit = [el.text for el in root.xpath("//tr[position()="+str(Counter)+"]/td[position()=1]/p/span")]
                            if (unit[0].strip().startswith("(")):
                                name = str(cat+unit[0])
                            else:
                                name = cat
                            category.append(name)
            if [el.text for el in root.xpath("//tr[position()="+str(Counter)+"]/td[position()=2]/p/span")] != ['\xa0'] and [el.text for el in root.xpath("//tr[position()="+str(Counter)+"]/td[position()=2]/p/span")] !=[] and name != '':  
                col1.append(name)
    
	
    row =0
    count=-1
    for ele in root.getiterator():
        if ele.tag == "tr" and ele.text_content() != '': 
            row +=1    
            for col in ele.getiterator():
                if col.tag == "td":
                    for sub in col.getiterator():
                        if (sub.tag == "b" or sub.tag == "strong") and sub.text_content().isupper(): 
                            count += 1
                            categories = category[count]
                            
    for i in category:   
        if i !='\xa0':
            catList.append(i)  
    
	#Extract cities and prices
    prod_Counter =0
    for el in root.getiterator():
        if el.tag == "tr":
            prod_Counter += 1
            if ([tg.text for tg in root.xpath("//tr[position()="+str(prod_Counter)+"]/td[position()=2]/p/span")] == ['\xa0']): 
                for val in root.xpath("//tr[position()="+str(prod_Counter)+"]/td[position()=1]/p/span"):
                    if val.text != '(dollars per ton)' and not((val.text).strip().startswith("*") or (val.text).strip().startswith("(*")):
                        prodList.append(val.text)
            else:
                for sub1 in root.xpath("//tr[position()="+str(prod_Counter)+"]/td[position()=1]/p/span"):
                    city.append(sub1.text)
                for sub2 in root.xpath("//tr[position()="+str(prod_Counter)+"]/td[position()=2]/p/span"):
                    price.append(sub2.text)    
    
	#Extract Product list for each category
    del city[0]                             
    item =-1       
    prodCategory=list() 
    for prod in prodList:
        item +=1
        if (item+1 <= len(prodList)-1):
            if prodList[item+1].startswith("("):
                prodName = prodList[item] + prodList[item+1]
                prodCategory.append(prodName)
            elif not prodList[item+1].startswith("(") and not(prodList[item].startswith("(")):
                prodCategory.append(prodList[item])
        elif not(prodList[item].startswith("(")):
            prodCategory.append(prodList[item])
    
    prod_row=0
    prod_Counter=-1
    for el in root.getiterator():
        if el.tag == "tr":
            prod_row += 1
            if [el.text for el in root.xpath("//tr[position()="+str(prod_row)+"]/td[position()=2]/p/span")] != ['\xa0'] and [el.text for el in root.xpath("//tr[position()="+str(prod_row)+"]/td[position()=2]/p/span")] !=[] and prod_Counter < len(prodCategory):  
                col2.append(prodCategory[prod_Counter])
            else: 
                if [el.text for el in root.xpath("//tr[position()="+str(prod_row+1)+"]/td[position()=2]/p/span")] != ['\xa0'] and [el.text for el in root.xpath("//tr[position()="+str(prod_row+1)+"]/td[position()=2]/p/span")] !=[]:
                    prod_Counter +=1
    
	#Write extracted data into csv file
    df= pd.DataFrame(index=[col1, col2,city,price])
    df.to_csv('F:/Nilam/MSBA_Degree/CISI/Feedstuff/'+file[0:file.find('.html')]+ ".csv")         


            
            
            
            