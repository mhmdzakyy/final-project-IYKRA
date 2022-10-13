import os
os.environ['CONDA_DLL_SEARCH_MODIFICATION_ENABLE'] = '1'


import csv
from time import sleep
import os
from google.cloud import pubsub_v1
import json
import base64

# from confluent_kafka import avro
credentials_path = 'google_credentials.json'
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/data-fellowship7/topics/final-project-streaming'

# bank_2022-10-01
def send_record():
     file = open('datasets/bank_2022-10-01.csv')
     csvreader = csv.reader(file)
     header = next(csvreader)
     for row in csvreader:
         attributes = {"id": (int(row[0])), 
                        "date": str(row[1]), 
                        "age": int(row[2]), 
                        "job": str(row[3]), 
                        "marital": str(row[4]),
                        "education":str(row[5]),
                        "default":str(row[6]), 
                        "housing": str(row[7]), 
                        "loan": str(row[8]),
                        "contact":str(row[9]), 
                        "month":str(row[10]),
                        "day_of_week": str(row[11]), 
                        "duration": int(row[12]), 
                        "campaign": int(row[13]), 
                        "pdays": int(row[14]),
                        "previous":int(row[15]),
                        "poutcome":str(row[16]), 
                        "emp_var_rate": float(row[17]), 
                        "cons_price_idx": str(row[18]),
                        "cons_conf_idx":float(row[19]), 
                        "euribor3m":str(row[20]),
                        "nr_employed":int(row[21]), 
                        "y":bool(row[22]),
                        }

         try:
             attributes_dumped = json.dumps(attributes)
             future = publisher.publish(topic_path, attributes_dumped.encode("utf-8"))
         except Exception as e:
             print(f"Exception while producing record value - {attributes}: {e}")
         else:
             print(f"Successfully producing record value - {attributes}")

         print(f'published message id {future.result()}')
         sleep(1)

if __name__ == "__main__":
     send_record()