#!/usr/bin/env python
# -*- coding: utf-8 -*-
import json
import boto3
from bs4 import BeautifulSoup
import requests

# import pandas as pd

lambda_client = boto3.client('lambda')


def save_data(payload):
    print("invoking persist lambda with payload"+str(payload))
    invoke_response = lambda_client.invoke(
        FunctionName="arn:aws:lambda:ap-southeast-1:803326344021:function:Persist_Floog_Guage_info_to_DB",
        InvocationType='Event',
        Payload=json.dumps(payload))
    print(invoke_response)


def lambda_handler(event, context):
    try:
        url = "http://poskobanjirdsda.jakarta.go.id/xmldata.xml"
        xml = requests.get(url)  # get the data
        soup = BeautifulSoup(xml.content, 'lxml')  # parse the data
        newfile = soup.find_all('sp_get_last_status_pintu_air')

        records = []
        for new in newfile:
            print("files inside file", new)
            Floodgates = new.find('id_pintu_air').text
            Station_Code = new.find('kode_stasiun').text
            Floodgates_name = new.find('nama_pintu_air').text
            Location = new.find('lokasi').text
            Alert_Level_1 = new.find('siaga1').text
            Alert_Level_2 = new.find('siaga2').text
            Alert_Level_3 = new.find('siaga3').text
            Alert_Level_4 = new.find('siaga4').text
            Latitude = new.find('latitude').text
            Longitude = new.find('longitude').text
            Date = new.find('tanggal').text
            Water_Level = new.find('tinggi_air').text
            Previous_Water_Level = new.find('tinggi_air_sebelumnya').text
            records.append(
                {"Floodgates": Floodgates, "Station_Code": Station_Code, "Floodgates_name": Floodgates_name,
                 "Location": Location,
                 "Alert_Level_1": Alert_Level_1, "Alert_Level_2": Alert_Level_2,
                 "Alert_Level_3": Alert_Level_3, "Alert_Level_4": Alert_Level_4, "Date": Date, "Latitude": Latitude,
                 "Longitude": Longitude, "Water_Level": Water_Level, "Previous_Water_Level": Previous_Water_Level})

            # df = pd.DataFrame(records,
            #                   columns=['Floodgates', 'Station_Code', 'Floodgates_name', 'Location', 'Alert_Level_1',
            #                            'Alert_Level_2', 'Alert_Level_3', 'Alert_Level_4', 'Date', 'Latitude', 'Longitude',
            #                            'Water_Level', 'Previous_Water_Level'])
            # df = df.reset_index()
            # output = []
            for record in records:
                print("The row data is", record)
                level_data = get_level_data(record)

                gauge_data = {"gaugeId": record['Floodgates'],
                              "deviceId": record['Station_Code'],
                              "gaugeNameId": record['Floodgates_name'],
                              "reportType": record['Location'],
                              "warninglevel_1": record['Alert_Level_1'],
                              "warninglevel_2": record['Alert_Level_2'],
                              "warninglevel_3": record['Alert_Level_3'],
                              "warninglevel_4": record['Alert_Level_4'],
                              "measureDateTime": record['Date'],
                              "latitude": record['Latitude'],
                              "longitude": record['Longitude'],
                              "depth": record['Water_Level'],
                              "Previous_Water_Level": record['Previous_Water_Level'],
                              "level": level_data['level'],
                              "warningLevel": level_data['warninglevel'],
                              "warningNameEn": level_data['warningnameen'],
                              "warningNameId": level_data['warningnameid'],
                              "warningNameJp": level_data['warningnamejp'],
                              }
                save_data(gauge_data)
    except Exception as e:
        print("Exception in function", e)


def get_level_data(row):
    level_data = {}
    if int(row['Water_Level']) >= int(row['Alert_Level_1']):
        level_data['level'] = 0
        level_data['warninglevel'] = 1
        level_data['warningnameid'] = "warninglevel_1"
        level_data['warningnameen'] = "Disaster"
        level_data['warningnamejp'] = "災害"
    elif int(row['Water_Level']) >= int(row['Alert_Level_2']):
        level_data['level'] = 1
        level_data['warninglevel'] = 2
        level_data['warningnameid'] = "warninglevel_2"
        level_data['warningnameen'] = "Critical"
        level_data['warningnamejp'] = "警戒"
    elif int(row['Water_Level']) >= int(row['Alert_Level_3']):
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

lambda_handler('event', 'context')
