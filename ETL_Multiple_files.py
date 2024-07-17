import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine

log_file = "log_file.txt"

def extract_csv(csv_file):
    dataframe = pd.read_csv(csv_file)
    return dataframe

def extract_json(json_file):
    dataframe= pd.read_json(json_file)
    return dataframe

def extract_xml(xml_file):
    dataframe = pd.DataFrame(columns=["Name", "Height", "Weight"])
    tree=ET.parse(xml_file)
    root=tree.getroot()
    for student in root.findall('Entry'):
        Name = student.find('Name').text
        Height = float(student.find('Height').text)
        Weight = float(student.find('Weight').text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"Name": Name, "Height": Height, "Weight":Weight}])], ignore_index = True)

    return dataframe

def extract():
    extracted_data = pd.DataFrame(columns= ["Name","Height","Weight"])

    for csvfile in glob.glob('Data_Files\\' + '*.csv'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_csv(csvfile))], ignore_index = True)
    for jsonfile in glob.glob('Data_files\\' + '*.json'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_json(jsonfile))], ignore_index = True)
    for xmlfile in glob.glob('Data_Files\\' + '*.xml'):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_xml(xmlfile))], ignore_index = True)
    return extracted_data


def transform(df1):
    df = df1
    #converting Height from inches to Meters and rounding float to 2 decimals
    df.Height = round (df.Height*0.0254,2)

    #converting Weight from pounds to KGs and rounding float to 2 decimals
    df.Weight = round (df.Weight*0.45359237,2)

    return df

def load_data(df):
    # Define connection parameters
    DATABASE = "HopeView_Academy_Data"
    USER = "postgres"
    PASSWORD = "**************" # Enter your secret password set for the database
    HOST = "localhost"
    PORT = "5432"

    # Establish the connection
    conn = psycopg2.connect(
        database=DATABASE,
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT
    )

    # Create SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DATABASE}')
    df.to_sql('student_profile', engine, if_exists='replace', index=False)

    conn.close()

def log_progress(message):
    #changing Date and Time Format
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now=datetime.now()
    timestamp= now.strftime(timestamp_format)

    #opening Log File in append mode
    with open(log_file, 'a') as f:
        f.write(timestamp+ ' '+ message+ ' \n')
    

# Initializing the ETL process
log_progress("ETL process starting...")

#logging start of extraction process
log_progress("Extraction phase started")
raw_data = extract()

#logging end of extraction phase
log_progress("Extraction phase completed")

#logging beginning of transformation phase
log_progress("Beginning of Transformation Phase")
transformed_data = transform(extract())
print("Below is the transformed data: ")
print(transformed_data)

log_progress("End of Transformation Phase")

#Initializing the Loading Phase
log_progress("Initializing Loading Phase")
load_data(transformed_data)

#log completion of loading phase
log_progress("Loading Completed")

#ETL process Ended
log_progress("ETL Job Completed Successfully")





