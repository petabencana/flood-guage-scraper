#!/usr/bin/env python
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import requests
import pandas as pd
import psycopg2

def lambda_handler(event, context) :
    try:
        url="http://poskobanjirdsda.jakarta.go.id/xmldata.xml"
        xml=requests.get(url) #get the data
        soup=BeautifulSoup(xml.content,'lxml') #parse the data
        newfile=soup.find_all('sp_get_last_status_pintu_air')

        records=[]
        for new in newfile:
            print("files inside file" , new)
            Floodgates=new.find('id_pintu_air').text
            Station_Code=new.find('kode_stasiun').text
            Floodgates_name=new.find('nama_pintu_air').text
            Location=new.find('lokasi').text
            Alert_Level_1=new.find('siaga1').text
            Alert_Level_2=new.find('siaga2').text
            Alert_Level_3=new.find('siaga3').text
            Alert_Level_4=new.find('siaga4').text
            Latitude=new.find('latitude').text
            Longitude=new.find('longitude').text
            Date=new.find('tanggal').text
            Water_Level=new.find('tinggi_air').text
            Previous_Water_Level=new.find('tinggi_air_sebelumnya').text
            records.append((Floodgates,Station_Code,Floodgates_name,Location,Alert_Level_1,Alert_Level_2,Alert_Level_3,Alert_Level_4,Date,Latitude,Longitude,Water_Level,Previous_Water_Level))

        df=pd.DataFrame(records,columns=['Floodgates','Station_Code','Floodgates_name','Location','Alert_Level_1','Alert_Level_2','Alert_Level_3','Alert_Level_4','Date','Latitude','Longitude','Water_Level','Previous_Water_Level'])
        df = df.reset_index()
        output=[]
        for index, row in df.iterrows():
            print("The row data is" , row)
            level_data = get_level_data(row)

            
            output.append({"gaugeId":row['Floodgates'],
                        "deviceId":row['Station_Code'],
                        "gaugeNameId":row['Floodgates_name'],
                        "reportType":row['Location'],
                        "warninglevel_1":row['Alert_Level_1'],
                        "warninglevel_2":row['Alert_Level_2'],
                        "warninglevel_3":row['Alert_Level_3'],
                        "warninglevel_4":row['Alert_Level_4'],
                        "measureDateTime":row['Date'],
                        "latitude":row['Latitude'],
                        "longitude":row['Longitude'],
                        "depth":row['Water_Level'],
                        "Previous_Water_Level":row['Previous_Water_Level'],
                        "level" : level_data['level'],
                        "warningLevel" : level_data['warninglevel'],
                        "warningNameEn" : level_data['warningnameen'],
                        "warningNameId" : level_data['warningnameid'],
                        "warningNameJp" : level_data['warningnamejp'],
                        })
        #connect to database
        try:
            con = psycopg2.connect(
                    host = "petabencana.caaxg6zgploo.ap-southeast-1.rds.amazonaws.com",
                    database = "cognicity_dev",
                    user = "postgres",
                    password = "u5tRWcPPx8KO")
            # con = psycopg2.connect(
            #         host = "127.0.0.1",
            #         database = "cognicity",
            #         user = "postgres",
            #         password = "postgres")
        except Exception as e:
            print("Connection errror" , e)
        cur = con.cursor()

        try:
            cur.executemany(
                "INSERT INTO floodgauge.reports(gaugeid,measuredatetime, depth, deviceid, reporttype,level,  notificationflag, gaugenameid,gaugenameen,gaugenamejp,warninglevel,warningnameid,warningnameen,warningnamejp,observation_comment)"
                "VALUES(%(gaugeId)s,%(measureDateTime)s,%(depth)s,%(deviceId)s,%(reportType)s,%(level)s,1,%(gaugeNameId)s,%(gaugeNameId)s,%(gaugeNameId)s,%(warningLevel)s,%(warningNameId)s,%(warningNameEn)s,%(warningNameJp)s,0);",output
            )
            sql1 = '''select * from floodgauge.reports;'''
            cur.execute(sql1)
        except Exception as e:
            print("Execution error" , e)

        for i in cur.fetchall():
            print(i)
        con.commit()
        con.close()


        #connect to database
        con = psycopg2.connect(
                    host = "petabencana.caaxg6zgploo.ap-southeast-1.rds.amazonaws.com",
                    database = "cognicity_dev",
                    user = "postgres",
                    password = "u5tRWcPPx8KO")
        cur = con.cursor()

        out= cur.execute("SELECT * FROM floodgauge.reports")
        for table in cur.fetchall():
            print(table);
        con.commit();
        con.close();
    except Exception as e:
        print("Exception in function" , e)

def get_level_data(row):
    level_data = {}
    if int(row['Water_Level'])>=int(row['Alert_Level_1']): 
        level_data['level'] = 0
        level_data['warninglevel'] = 1
        level_data['warningnameid'] = "warninglevel_1" 
        level_data['warningnameen'] = "Disaster" 
        level_data['warningnamejp'] = "災害"
    elif int(row['Water_Level'])>=int(row['Alert_Level_2']):
        level_data['level'] = 1
        level_data['warninglevel'] = 2 
        level_data['warningnameid'] = "warninglevel_2"
        level_data['warningnameen'] = "Critical"
        level_data['warningnamejp'] = "警戒"
    elif int(row['Water_Level'])>=int(row['Alert_Level_3']):
        level_data['level'] = 2
        level_data['warninglevel'] = 3
        level_data['warningnameid'] = "warninglevel_3"
        level_data['warningnameen'] = "Alert"
        level_data['warningnamejp'] = "注意"
    else:
        level_data['level'] = 9
        level_data['warninglevel'] = 4
        level_data['warningnameid'] = "warninglevel_4"
        level_data['warningnameen'] = "Safe"
        level_data['warningnamejp'] = "安全"
    return level_data;

lambda_handler('event' , 'context')