
class files_to_load():

  import pandas as pd
  from flask import Flask
  import requests
  import sqlite3 as sql
  import random
  
  app = Flask(__name__)
  
  
  #read all files dynamically from an input csv file

  def read_files_arrays(self):
    path=input('please supply a full input path of csv file contains all files arrays:')
    file_arrays=self.pd.read_csv(path, delimiter=',', header=None)
    list_of_arrays=file_arrays.values.tolist()

    file_arrays=[]

    for array in list_of_arrays:
      filtered_list = [item for item in array if not(self.pd.isnull(item)) == True]
      file_arrays.append(filtered_list)
      
    return file_arrays
  
  #rundomly replace with a currapt files (concatination of a string to the file name)
 
  def randomly_currapt_files(self):
    currapt='currapt_'

    currapt_this_array=self.read_files_arrays()
      
    for array in currapt_this_array:
      #10 percent chance for file curraption:
      random_10_perc=self.random.randint(0,10)
      if random_10_perc==10:
        #replace with a currapt file in a random place within the array
        random_index=self.random.randint(0,(len(array)-1))
        array[random_index] = currapt+'_'+array[random_index]

    return currapt_this_array
  #currapt files will be removed from all arrays and then will be appended to variable and finally printed

  def clean_currapt_files(self):

    currapt_files_list=[]
    currapted_array= self.randomly_currapt_files()
      
    for array in currapted_array:
      for i in range(0,len(array)-1):
        if 'currapt' in array[i]:
          currapt_files_list.append(array[i])
          array.pop(i)

    if len(currapt_files_list)>0:
      print('Currapt files are as following: {}, All files were cleaned from the arrays.'.format(currapt_files_list))
    else:
      print('No files are currapt')
      
    self.cleand_array=currapted_array

  def send_requests(self):

    payload1={}
    payload2={}
    payload3={}
    payload4={}

    index=0

    #array is dividing with 4

    file_arrays=self.cleand_array
    
    if len(file_arrays)%4==0:
      for i in range(int(len(file_arrays)/4)):
        payload1['file'], payload2['file'], payload3['file'], payload4['file']=file_arrays[index], file_arrays[index+1], file_arrays[index+2], file_arrays[index+3]
        index+=4
        self.requests.post('http://localhost:8081', data=payload1, timeout=40)
        self.requests.post('http://localhost:8081', data=payload2, timeout=40)
        self.requests.post('http://localhost:8081', data=payload3, timeout=40)
        self.requests.post('http://localhost:8081', data=payload4, timeout=40)
        #print(payload1, payload2, payload3, payload4)
        #send requests to sqlite with timeout in loops + 1 min heartbeat to db between each loop

    #array is not dividing with 4 and has more than 4 values:
    if len(file_arrays)>4 and (len(file_arrays)%4>0 or len(file_arrays)%4==0):
        for i in range(int(len(file_arrays)/4)):
          payload1['file'], payload2['file'], payload3['file'], payload4['file']=file_arrays[index], file_arrays[index+1], file_arrays[index+2], file_arrays[index+3]
          index+=4
          self.requests.post('http://localhost:8081', data=payload1, timeout=40)
          self.requests.post('http://localhost:8081', data=payload2, timeout=40)
          self.requests.post('http://localhost:8081', data=payload3, timeout=40)
          self.requests.post('http://localhost:8081', data=payload4, timeout=40)
          #print(payload1, payload2, payload3, payload4)
          #send requests to sqlite with timeout in loops

        #send rest of requests:
        if len(file_arrays)%4==1:
          payload1['file']=file_arrays[index]
          self.requests.post('http://localhost:8081', data=payload1, timeout=40)
          #print(payload1)
        elif len(file_arrays)%4==2:
          payload1['file'],  payload2['file']=file_arrays[index], file_arrays[index+1]
          self.requests.post('http://localhost:8081', data=payload1, timeout=40)
          self.requests.post('http://localhost:8081', data=payload2, timeout=40)
          #print(payload1,payload2)
        elif len(file_arrays)%4==3:
          payload1['file'], payload2['file'], payload3['file']=file_arrays[index], file_arrays[index+1], file_arrays[index+2]
          self.requests.post('http://localhost:8081', data=payload1, timeout=40)
          self.requests.post('http://localhost:8081', data=payload2, timeout=40)
          self.requests.post('http://localhost:8081', data=payload3, timeout=40)
          #print(payload1,payload2,payload3)

    #array lenght < 4:
    if len(file_arrays)<4:
      if len(file_arrays)==1:
        payload1['file']=file_arrays[index]
        self.requests.post('http://localhost:8081:8081', data=payload1, timeout=40)
        #print(payload1)
      elif len(file_arrays)==2:
        payload1['file'],  payload2['file']=file_arrays[index], file_arrays[index+1]
        self.requests.post('http://localhost:8081', data=payload1, timeout=40)
        self.requests.post('http://localhost:8081', data=payload2, timeout=40)
        #print(payload1,payload2)
      elif len(file_arrays)==3:
        payload1['file'], payload2['file'], payload3['file']=file_arrays[index], file_arrays[index+1], file_arrays[index+2]
        self.requests.post('http://localhost:8081', data=payload1, timeout=40)
        self.requests.post('http://localhost:8081', data=payload2, timeout=40)
        self.requests.post('http://localhost:8081', data=payload3, timeout=40)
        #print(payload1,payload2,payload3)

  def insert_data(self, file):
    db_name = "my_files.db"
    conn = self.sql.connect(db_name)
    cursor=conn.cursor()
    #create table
    table="create table if not exists loaded_files (file_list nvarchar(4000))"
    cursor.execute(table)
    conn.commit()
    #insert
    insert="INSERT INTO loaded_files (file_list) VALUES (?)"
    cursor.execute(insert, (file))
    conn.commit()

  @app.route("/", methods=['POST'])
  def result(self):
    file = self.request.form['file']       
    self.insert_data(file)
    return "Done"


files=files_to_load()
files.clean_currapt_files()
files.send_requests()
files.result()

if __name__=='__main__':
    app.run(debug=True)

