import sqlalchemy
from dotenv import load_dotenv
import os
import cx_Oracle
import profile
import time
import urllib.request
import zipfile
from datetime import date

import pandas as pd
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By

from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR, INTEGER, DATE


pd.set_option('display.max_columns', None)

# using the Edge driver because Chrome would lose its place when selecting more than one report
# and firefox would not hold the configuration to all zip file saves automatically
driver = webdriver.Edge()
# driver.maximize_window()

# Complete data files entry point
driver.get('http://nces.ed.gov/ipeds/datacenter/login.aspx?gotoReportId=7')

# Click into the year drop down box and select all years
select = Select(driver.find_element(By.ID, 'contentPlaceHolder_ddlYears'))

select.select_by_visible_text('All years')

# Click into the survey drop down box and select the needed Survey, in this case it was Fall Enrollment
select2 = Select(driver.find_element(By.ID, 'contentPlaceHolder_ddlSurveys'))

select2.select_by_visible_text('Fall Enrollment')

# Select the continue buttom, I was only able to get it to work with the XPAth
driver.find_element(By.XPATH, '//*[@id="aspnetForm"]/div[4]/div[3]/div[1]/table/tbody/tr/td[3]/img').click()

# gave the start year to pull in as far back as useful, I will likely shift this to a more recent year
# to avoid survey changes that have occurred over time
start_year = 2008
# selecting current year to get the most recent, I have error handlers below to skip if the year is not found
end_year = date.today().year
# switched to the table because it was having trouble finding elements otherwise
elem = driver.switch_to.active_element
# find all of the individual year links
lnks=driver.find_elements(By.TAG_NAME, "a")
# create a list of all of the hrefs
hrefs = []
for lnk in lnks:
    href = lnk.get_attribute('href')
    hrefs.append(href)

# in cases where there was no href, i removed it from the list
file_list = []
result = []
for x in hrefs:
    if x != None:
        result.append(x)

# function to download files from a link
def download_file(download_url, filename):
    response = urllib.request.urlopen(download_url)
    file = open(filename, 'wb')
    file.write(response.read())
    file.close()


#Get data and data dictionary per year, by finding all of the href's that have the formatted name for either the data
# or the dictionary
years = []
for year in range(end_year, start_year, -1):
    string = 'EF' + str(year) + 'CP'
    href_result = []
    try:
        for x in result:
            if string in x:
                href_result.append(x)

        data = href_result.index('https://nces.ed.gov/ipeds/datacenter/data/' + string + '.zip')
        IPEDS_dict = href_result.index('https://nces.ed.gov/ipeds/datacenter/data/' + string + '_Dict.zip')
        file_name = 'fall_enrollment_cip' + str(year) + '.zip'
        dict_name = 'fall_enrollment_cip' + str(year) + 'dict.zip'

        download_file(href_result[data], file_name)
        download_file(href_result[IPEDS_dict], dict_name)

        years.append(year)
        time.sleep(10)

    except Exception as e:
        print(e)
        continue

    zip_ref = zipfile.ZipFile(file_name, 'r')
    zip_ref.extractall(".")
    zip_ref.close()

    zip_ref = zipfile.ZipFile(dict_name, 'r')
    zip_ref.extractall(".")
    zip_ref.close()

max_year = max(years)
IPEDS_dict = pd.read_excel(".\ef" + str(max_year) + "cp.xlsx", sheet_name='varlist')
columns_needed = list(IPEDS_dict['varname'])

all_data = pd.DataFrame([])
for year in years:
    try:
        if os.path.isfile(".\ef" + str(year) + "cp_rv.csv"):
            data = pd.read_csv(".\ef" + str(year) + "cp_rv.csv")
        else:
            data = pd.read_csv(".\ef" + str(year) + "cp.csv")
        data.columns = data.columns.str.replace(' ', '')

        globals()["data_" + str(year)] = data[columns_needed]

        # usable_data.columns = list(dict['varTitle'])

        globals()["data_" + str(year)]['acad_year'] = year
        all_data = pd.concat([all_data, globals()["data_" + str(year)]])
    except Exception as e:
        print(e)
        continue


column_names = pd.Series(IPEDS_dict.varTitle.values, index= IPEDS_dict.varname).to_dict()

all_data_with_labels = all_data.rename(columns=column_names)

xls = pd.ExcelFile('IPEDS_dictionaries.xlsx')
cip = pd.read_excel(xls, 'CIP')
los = pd.read_excel(xls, 'student level')
los_orig = pd.read_excel(xls, 'student level orig')
major = pd.read_excel(xls, 'major field')
attend = pd.read_excel(xls, 'attendance status')
#
# with pd.ExcelWriter('IPEDS_dictionaries.xlsx') as writer:
#     compiler_check_CIP.to_excel(writer, sheet_name='CIP', index=False)
#     compiler_check_LOS.to_excel(writer, sheet_name='student level', index=False)
#     compiler_check_LOS_orig.to_excel(writer, sheet_name='student level orig', index=False)
#     compiler_check_Major.to_excel(writer, sheet_name='major field', index=False)
#     compiler_check_attend.to_excel(writer, sheet_name='attendance status', index=False)

all_data_with_labels.rename(columns={'CIP Code for major field of study': 'CIP Code for major field of study_value',
                                     'Level of student (original line number on survey form)': 'Level of student (original line number on survey form)_value',
                                     'Level of student': 'Level of student_value',
                                     'Major field of study': 'Major field of study_value',
                                     'Attendance status of student': 'Attendance status of student_value'},
                            inplace=True)
all_data_with_labels_CIP = all_data_with_labels.merge(cip)
all_data_with_labels_los = all_data_with_labels_CIP.merge(los)
all_data_with_labels_los_orig = all_data_with_labels_los.merge(los_orig)
all_data_with_labels_major = all_data_with_labels_los_orig.merge(major)
all_data_with_labels_attend = all_data_with_labels_major.merge(attend)

institutions = pd.read_csv(
    r'S:\AA\IE\IR\IR-Files\Data Sources & Repository\Benchmarking Data Repository\IPEDS Data\IPEDS_INSTITUTIONS\IPEDS_INSTITUTIONS.csv')
all_data_with_labels_ins = all_data_with_labels_attend.merge(institutions,
                                                            left_on='Unique identification number of the institution',
                                                            right_on='UnitID')

all_data_with_labels_ins = all_data_with_labels_ins.rename(columns=str.lower)

all_data_with_labels_ins.to_csv('final_output_fall_enrollment.csv', index=False)

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

engine = create_engine(
    oracle_connection_string.format(
        username=username,
        password=password,
        hostname=host_name,
        port=port,
        service_name=service_name,
    )
)

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
