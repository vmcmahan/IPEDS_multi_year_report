import sqlalchemy
from dotenv import load_dotenv
import os
import cx_Oracle
import profile
import time


from datetime import date

import pandas as pd


from sqlalchemy import text, create_engine


pd.set_option('display.max_columns', None)

all_data_with_labels = pd.read_csv('data_abridged.csv')

load_dotenv()

host_name = os.environ.get('tx_dev_host')
service_name = os.environ.get('tx_dev_db')
port = os.environ.get('tx_dev_port')
username = os.environ.get('tx_dev_user')
password = os.environ.get('tx_dev_password')
cx_Oracle.init_oracle_client(lib_dir=r"C:\opt\oracle\instantclient-basic-windows.x64-21.6.0.0.0dbru\instantclient_21_6")

oracle_connection_string = (
    'oracle+cx_oracle://{username}:{password}@' +
    cx_Oracle.makedsn('{hostname}', '{port}', service_name='{service_name}')
)
print('r u doing anything?')

engine = create_engine(
    oracle_connection_string.format(
        username=username,
        password=password,
        hostname=host_name,
        port=port,
        service_name=service_name,
    )
)

print('hola')
all_data_with_labels.to_sql('bnch_ipeds_cip_fall_enrollment', engine, if_exists='replace', index=False,
                                   chunksize=2000,
                                   dtype={"Unique identification number of the institution": sqlalchemy.types.VARCHAR(10),
                                          "Major field of study_value": sqlalchemy.types.VARCHAR(10),
                                          "CIP Code for major field of study_value": sqlalchemy.types.VARCHAR(50),
                                          "Level of student (original line number on survey form)_value": sqlalchemy.types.VARCHAR(10),
                                          "Attendance status of student_value": sqlalchemy.types.VARCHAR(10),
                                          "Level of student_value": sqlalchemy.types.VARCHAR(10),
                                          "Grand total": sqlalchemy.types.INTEGER,
                                          "Total men": sqlalchemy.types.INTEGER,
                                          "Total women": sqlalchemy.types.INTEGER,
                                          "American Indian or Alaska Native total": sqlalchemy.types.INTEGER,
                                          "American Indian or Alaska Native men": sqlalchemy.types.INTEGER,
                                          "American Indian or Alaska Native women": sqlalchemy.types.INTEGER,
                                          "Asian total": sqlalchemy.types.INTEGER,
                                          "Asian men": sqlalchemy.types.INTEGER,
                                          "Asian women": sqlalchemy.types.INTEGER,
                                          "Black or African American total": sqlalchemy.types.INTEGER,
                                          "Black or African American men": sqlalchemy.types.INTEGER,
                                          "Black or African American women": sqlalchemy.types.INTEGER,
                                          "Hispanic total": sqlalchemy.types.INTEGER,
                                          "Hispanic men": sqlalchemy.types.INTEGER,
                                          "Hispanic women": sqlalchemy.types.INTEGER,
                                          "Native Hawaiian or Other Pacific Islander total": sqlalchemy.types.INTEGER,
                                          "Native Hawaiian or Other Pacific Islander men": sqlalchemy.types.INTEGER,
                                          "Native Hawaiian or Other Pacific Islander women": sqlalchemy.types.INTEGER,
                                          "White total": sqlalchemy.types.INTEGER,
                                          "White men": sqlalchemy.types.INTEGER,
                                          "White women": sqlalchemy.types.INTEGER,
                                          "Two or more races total": sqlalchemy.types.INTEGER,
                                          "Two or more races men": sqlalchemy.types.INTEGER,
                                          "Two or more races women": sqlalchemy.types.INTEGER,
                                          "Race/ethnicity unknown total": sqlalchemy.types.INTEGER,
                                          "Race/ethnicity unknown men": sqlalchemy.types.INTEGER,
                                          "Race/ethnicity unknown women": sqlalchemy.types.INTEGER,
                                          "Nonresident alien total": sqlalchemy.types.INTEGER,
                                          "Nonresident alien men": sqlalchemy.types.INTEGER,
                                          "Nonresident alien women": sqlalchemy.types.INTEGER,
                                          # "acad_year": sqlalchemy.DateTime(),
                                          "CIP Code for major field of study": sqlalchemy.types.VARCHAR(50),
                                          "Level of student": sqlalchemy.types.VARCHAR(100),
                                          "Level of student (original line number on survey form)": sqlalchemy.types.VARCHAR(100),
                                          "Major field of study": sqlalchemy.types.VARCHAR(100),
                                          "Attendance status of student": sqlalchemy.types.VARCHAR(20)})

