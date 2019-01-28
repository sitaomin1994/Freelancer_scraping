import pymysql
import requests
import json
import time
import re
from datetime import datetime

'''
TABLE PROJECT
    project_id  INT
    owner_id INT
    bid_period INT
    title TEXT 
    currency_code VARCHAR(100),
    currency_rate FLOAT
    discription TEXT
    urgent BOOL
    location_city VARCHAR(100)
    location_country VARCHAR(100)
    project_type VARCHAR(20),
    hireme BOOL,
    recommended_freelancer TEXT NULL,
    tags_categories TEXT,
    tags_name TEXT,
    qualification TEXT NULL,
    invited_freelancer TEXT NULL,
    time_free_bid_expire INT,
    sub_status VARCHAR(20),
    project_language VARCHAR(100),
    project_url VARCHAR(500),
    time_submitted BIGINT,
    budget_min FLOAT,
    budget_max FLOAT,
    bids_count INT,
    bids_stats FLOAT,
    
    selected_bids_id INT DEFAULT -1,
    selected_bids_time_awarded DATETIME,
    selected_bids_bidder_id INT DEFAULT -1,
    number_of_selected_bids INT DEFAULT -1,
    
    hireme_initial_bids_id INT DEFUALT -1,
    hireme_initial_time_awarded DATETIME,
    hireme_initial_bidder_id INT DEFAULT -1,
    
    
    employer_verifications TEXT,
    employer_username  VARCHAR(500) DEFAULT 'null',
    employer_displayname VARCHAR(500) DEFAULT 'null',
    employer_primary_language VARCHAR(500) DEFAULT 'null',
    employer_nreviews INT DEFAULT -1,
    employer_rating FLOAT DEFAULT -1,
    employer_membership VARCHAR(100) DEFAULT 'null',
    employer_country VARCHAR(500) DEFAULT 'null'
    employer_since DATETIME

1. connect to database ---- connecting to mysql database

2. get projects information
    -  create table in database
    -  requests url
    -  extract projects information
    -  save each project information to database

error handling, if there is an error just save current state and resolve the error and continue
'''

'''
token pool
'''
token_pool = ['vRFZUqclBzv7VzpyTJ3vfrc8RwvoGq', 'AXGnj1OHV5Le3jKhrdobCl4tZ2q9cm', 'YwQ2ZFfJehpUj7UEOGpUn3y5pcpUtJ',
              'IHPDP3HoqjWX1tTqUtMu4EAqxlMRlW', '5qfr3iCqMgBerHmZJDMO3Co2AV3a4d', 'qIo7Id3Dcdw528IzQU59K94I3cXWHZ',
              'jDaI7PSRa8H375Bn4B81mb6IlvS3sc', 'FtQ20tausNVHvCpKBInosYFjv4FPTP', 'Z3XgZKiXp7QgPwqRKB9tnKcFnLt1eb'
                                                                                  'pRq8XNGo0J14kA7JnXOXy2KJfSjsCR',
              'zURlph7AJqSNtcUFlZUxmKlr5R8XRd', '0oUrcZIfew5qHbvgpZE5rN5s2Le4UF']

'''
generate new token
'''
def convert_utc_to_date(time):
    return datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')


def generate_new_token(index):
    if (index == len(token_pool) - 1):
        new_index = 0
    else:
        new_index = index + 1

    return new_index, token_pool[new_index]

'''
extract project infomation
'''

def get_product_list(project_list, cursor, connection):

    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)
    total_count = 0
    error_set = []

    token_index = 0
    token_index, token = generate_new_token(token_index)
    count = 1

    times = int((len(project_list)-1)/100)

    print(times)

    for i in range(0,times+1):

        '''
        change token
        '''
        if (count == 500):
            token_index, token = generate_new_token(token_index)
            count = 1
        else:
            count = count + 1

        projects_id = project_list[i*100:(i+1)*100]
        #print(projects)
        h = {"Freelancer-OAuth-V1": token}
        params = {
            'projects[]': projects_id,
            'frontend_project_statuses[]': 2,
            'full_description': True,
            'job_details': True,
            'qualification_details': True,
            'selected_bids': True,
            'user_details': True,
            'hireme_details': True,
            'invited_freelancer_details': True,
            'location_details':True,
            #'recommended_freelancer_details': True,  # this can be done for just one project at the time
            'user_status': True,
            'user_responsiveness': True,
            'user_employer_reputation': True,
            'user_membership_details': True,
            'user_financial_details': True
        }

        url = 'https://www.freelancer.com/api/projects/0.1/projects/'

        response = requests.get(url, headers = h, params = params)
        print(json.dumps(response.json()))

        ERROR_SIGN_PROJECT = False
        ERROR_SIGN_REQUEST = False
        error_set = []

        if response.status_code == 200:
            projects = response.json()["result"]["projects"]
            selected_bids = response.json()["result"]["selected_bids"]
            users = response.json()['result']['users']

            total_count += len(projects)


            #insert project infomation
            for iter, project in enumerate(projects):
                result = []
                result.append(project['id'])                                                                            #project id - int
                result.append(project['owner_id'] if project["owner_id"] != None else 0)                                #owner_id - int
                result.append(project['bidperiod'] if project['bidperiod'] != None else -1)                             #bid period - int
                result.append(project['title'].replace("'","").replace("\\","") if project['title'] != None else 'null')#title - string

                try:
                    result.append(project['currency']['code'] if project['currency']['code']!= None else 'null')        #currency_code - string
                except:
                    result.append("null")

                try:
                    result.append(project['currency']['exchange_rate']
                                  if project['currency']['exchange_rate']!=None else -1)                                #currency_exchange_rate - float
                except:
                    result.append(-1)

                result.append(emoji_pattern.sub(r'', project['description'].replace('\n','')
                                                      .replace('\r','').replace("'","").replace("\\",""))
                                                      if project['description']!=None else 'null')                      #description - string

                try:
                    result.append(project['upgrades']['urgent'] if project['upgrades']['urgent'] != None else None)    #urgent - boolean
                except:
                    result.append(None)

                try:
                    result.append(json.dumps(project['location']['city']))                                              #city - string
                except:
                    result.append('null')

                try:
                    result.append(json.dumps(project['location']['country']['name']))                                   #country - string
                except:
                    result.append("null")


                result.append(project['type'] if project['type'] != None else 'null')                                   #project_type - string - fixed or hourly
                result.append(project['hireme'] if project['hireme']!= None else None)                                 #hireme - boolean
                result.append(json.dumps(project['recommended_freelancers']).replace("'",""))                                           #recommended_freelancer --- string

                try:
                    result.append(json.dumps([ele['category']['name'] for ele in project['jobs']]).replace("'",""))                     #tag_categories --- string
                except:
                    result.append("null")

                try:
                    result.append(json.dumps([ele['name'] for ele in project['jobs']]).replace("'",""))                                 #tag_names
                except:
                    result.append("null")

                result.append(json.dumps(project['qualifications']))                                                    #qualification --- string
                result.append(json.dumps(project['invited_freelancers']))                                               #invited_freelancer --- string
                result.append(convert_utc_to_date(project['time_free_bids_expire'] if project['time_free_bids_expire'] else 0))             #time free bids expire --- number
                result.append(project['sub_status'] if project['sub_status']!= None else 'null')                        #sub status --- string --- eg. closed_awarded
                result.append(project['language'].replace("'","") if project['language'] != None else 'null')                           #language --- string
                result.append(project['seo_url'].replace("'","") if  project['seo_url']  != None else 'null')                           #project_url --- string
                result.append(convert_utc_to_date(project['time_submitted']
                              if project['time_submitted']  != None else 0))                                            #project_time_submitted - number

                try:
                    result.append(project['budget']['minimum'] if project['budget']['minimum'] != None else -1)         #budget_minimum --- float
                except:
                    result.append(-1)

                try:
                    result.append(project['budget']['maximum'] if project['budget']['maximum'] != None else -1)         #budget_maximum --- float
                except:
                    result.append(-1)

                try:
                    result.append(project['bid_stats']['bid_count']
                                  if project['bid_stats']['bid_count'] != None else -1)                                 #bids_status_count --- number
                except:
                    result.append(-1)

                try:
                    result.append(project['bid_stats']['bid_avg'] if project['bid_stats']['bid_avg'] != None else -1)   #bids_status_avg ---- float
                except:
                    result.append(-1)

                result.append(convert_utc_to_date(0))                                                                   #selected_bid_time_award ----datetime

                try:
                    result.append(project['hireme_initial_bid']['id']
                                  if project['hireme_initial_bid']['id'] is not None else -1)                           #hireme_initial bid id
                except:
                    result.append(-1)
                try:
                    result.append(convert_utc_to_date(project['hireme_initial_bid']['time_awarded']
                                  if project['hireme_initial_bid']['time_awarded'] is not None else 0))                 #hireme_initail time awarded
                except:
                    result.append(convert_utc_to_date(0))
                try:
                    result.append(project['hireme_initial_bid']['bidder_id']                                            #hireme_initial bidder_id
                                  if project['hireme_initial_bid']['bidder_id'] is not None else -1)
                except:
                    result.append(-1)


                sql = "REPLACE INTO PROJECT(project_id, owner_id, bid_period, title, currency_code, currency_rate,"\
                      "discription, urgent, location_city, location_country, project_type, hireme, recommended_freelancer,"\
                      "tags_categories, tags_name, qualification, invited_freelancer, time_free_bid_expire, sub_status,"\
                      "project_language, project_url, time_submitted, budget_min, budget_max, bids_count, bids_stats," \
                      "selected_bids_time_awarded, hireme_initial_bids_id, hireme_initial_time_awarded, hireme_initial_bidder_id)"\
                      "VALUES (%s, %s, %s, '%s', '%s', %s, '%s', %s, '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, '%s', %s, '%s', %s)" % tuple(result)

                #print(sql)
                #[print(ele) for ele in result]
                cursor.execute(sql)
                connection.commit()

                SQL_SAVE_PROGRESS = "UPDATE PROJECT_PROGRESS SET has_record = 1 WHERE project_id = %s;" % (project['id'])
                cursor.execute(SQL_SAVE_PROGRESS)
                connection.commit()
                #print('iter %d info finished'%(iter))

            #insert selected bid information
            if selected_bids != None:
                for key, values in  selected_bids.items():


                    #bid info
                    try:
                        selected_bid_id = values[0]["id"] if values[0]["id"] is not None else -1
                    except:
                        selected_bid_id = -1
                    try:
                        selected_bid_time_awarded = values[0]["time_awarded"] if values[0]["time_awarded"] is not None else 0
                    except:
                        selected_bid_time_awarded = 0
                    try:
                        selectd_bid_bidder_id = values[0]["bidder_id"] if values[0]["bidder_id"] is not None else -1
                    except:
                        selectd_bid_bidder_id = -1

                    #number of bids
                    if values != None:
                        number_of_bids = len(values)
                    else:
                        number_of_bids = 0

                    sql = "UPDATE PROJECT SET selected_bids_time_awarded = '%s', selected_bids_id = %s, selected_bids_bidder_id = %s,number_of_selected_bids = %s"\
                          " WHERE project_id = %s" %(convert_utc_to_date(selected_bid_time_awarded), selected_bid_id, selectd_bid_bidder_id, number_of_bids, key)
                    #print(sql)

                    cursor.execute(sql)
                    connection.commit()

                    #print("update round %d finished"%(i))
                    #print("number of selected bid %d"%(len(values)))

                #insert related employer information
            if users != None:

                for key,values in users.items():

                    #user_infomation
                    user_info = []
                    user_info.append(json.dumps(values['status']).replace("'",""))
                    user_info.append(json.dumps(values['username']).replace("'",""))
                    user_info.append(json.dumps(values['display_name']).replace("'",""))
                    user_info.append(json.dumps(values['primary_language']).replace("'",""))

                    try:
                        user_info.append(values['employer_reputation']['entire_history']['reviews'] if values['employer_reputation']['entire_history']['reviews'] is not None else -1)
                    except:
                        user_info.append(-1)

                    try:
                        user_info.append(values['employer_reputation']['entire_history']['overall'] if values['employer_reputation']['entire_history']['overall'] is not None else -1)
                    except:
                        user_info.append(-1)

                    try:
                        user_info.append(json.dumps(values['membership_package']['name']).replace("'",""))
                    except:
                        user_info.append('null')

                    try:
                        user_info.append(json.dumps(values['location']['country']['name']).replace("'",""))
                    except:
                        user_info.append('null')

                    user_info.append(convert_utc_to_date(values['registration_date'] if values['registration_date'] is not None else 0))

                    user_info.append(key)

                    SQL_insert_user_info = "UPDATE PROJECT SET employer_verifications = '%s', employer_username = '%s', employer_displayname = '%s', "\
                                       "employer_primary_language = '%s', employer_nreviews = %s, employer_rating = %s, employer_membership = '%s', "\
                                       "employer_country = '%s',  employer_since = '%s' WHERE owner_id = %s" %(tuple(user_info))

                    cursor.execute(SQL_insert_user_info)
                    connection.commit()


            print("number of projects in round %d : %d"%(i, len(projects)))

        else:
            error = response.content
            print(error)
            print("request error happen on round", i)
            ERROR_SIGN_PROJECT = True

        if ERROR_SIGN_PROJECT == True:
            SQL_SAVE_PROGRESS_ALL = "UPDATE PROJECT_PROGRESS SET extracted_info = 0 WHERE project_id = %s;"
            cursor.executemany(SQL_SAVE_PROGRESS_ALL, [str(project) for project in projects_id])
            connection.commit()
        else:
            SQL_SAVE_PROGRESS_ALL = "UPDATE PROJECT_PROGRESS SET extracted_info = 1 WHERE project_id = %s;"
            cursor.executemany(SQL_SAVE_PROGRESS_ALL, [str(project) for project in projects_id])
            connection.commit()

        print('round %d finished' % (i))
        print('request error:', ERROR_SIGN_REQUEST)
        print("====================================================================================================================================")

    print('total project extracted: ', total_count)

    return 0


'''
main
'''
# connecting to database and create table
connection  = pymysql.connect("mshresearch.marshall.usc.edu","itroncos","ismr_pswd","freelancer" )
cursor = connection.cursor()
#cursor.execute("DROP TABLE IF EXISTS PROJECT")

first_time_sign = int(input("are you first time run project program?(yes - 1 or non 0)"))

if(first_time_sign == 1):

    sql = """CREATE TABLE IF NOT EXISTS PROJECT (
               project_id  INT,
               owner_id INT,
               bid_period INT,
               title TEXT ,
               currency_code VARCHAR(100),
               currency_rate FLOAT,
               discription TEXT,
               urgent BOOL,
               location_city VARCHAR(100),
               location_country VARCHAR(100),
               project_type VARCHAR(20),
               hireme BOOL,
               recommended_freelancer TEXT NULL,
               tags_categories TEXT,
               tags_name TEXT,
               qualification TEXT NULL,
               invited_freelancer TEXT NULL,
               time_free_bid_expire DATETIME,
               sub_status VARCHAR(20),
               project_language VARCHAR(100),
               project_url VARCHAR(500),
               time_submitted DATETIME,
               budget_min FLOAT,
               budget_max FLOAT,
               bids_count INT,
               bids_stats FLOAT,
               selected_bids_id INT DEFAULT -1,
               selected_bids_time_awarded DATETIME,
               selected_bids_bidder_id INT DEFAULT -1,
               number_of_selected_bids INT DEFAULT -1,
               hireme_initial_bids_id INT DEFAULT -1,
               hireme_initial_time_awarded DATETIME,
               hireme_initial_bidder_id INT DEFAULT -1,
               employer_verifications TEXT,
               employer_username  VARCHAR(500) DEFAULT 'null',
               employer_displayname VARCHAR(500) DEFAULT 'null',
               employer_primary_language VARCHAR(500) DEFAULT 'null',
               employer_nreviews INT DEFAULT -1,
               employer_rating FLOAT DEFAULT -1,
               employer_membership VARCHAR(100) DEFAULT 'null',
               employer_country VARCHAR(500) DEFAULT 'null',
               employer_since DATETIME,
               PRIMARY KEY(project_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8
            """

    cursor.execute(sql)
    connection.commit()

    SQL_create_project_progress = """ CREATE TABLE IF NOT EXISTS PROJECT_PROGRESS (
                                            project_id INT AUTO_INCREMENT,
                                            extracted_info BOOL DEFAULT 0,
                                            has_record BOOL DEFAULT 0,
                                            PRIMARY KEY(project_id))
                                          """
    cursor.execute(SQL_create_project_progress)
    connection.commit()

    SQL_set_auto_increment = """ alter table PROJECT_PROGRESS AUTO_INCREMENT= 15982540"""
    SQL_set_charset_project_progress = "ALTER TABLE PROJECT_PROGRESS CHARACTER SET = utf8"
    SQL_set_charset_project = "ALTER TABLE PROJECT CHARACTER SET = utf8"
    cursor.execute(SQL_set_auto_increment)
    connection.commit()
    cursor.execute(SQL_set_charset_project)
    connection.commit()
    cursor.execute(SQL_set_charset_project_progress)
    connection.commit()
    index_list = []

    for i in range(200000):
        index_list.append([None])

    SQL_initial_index = "INSERT INTO PROJECT_PROGRESS(project_id)VALUES(%s)"
    cursor.executemany(SQL_initial_index, index_list)
    connection.commit()


SQL_count_projects = "SELECT COUNT(project_id) FROM PROJECT_PROGRESS"
SQL_extracted_projects = "SELECT COUNT(project_id) FROM PROJECT_PROGRESS WHERE extracted_info = 1"
cursor.execute(SQL_count_projects)
number_of_project_in_progress_table = cursor.fetchall()
cursor.execute(SQL_extracted_projects)
number_of_project_extracted = cursor.fetchall()
SQL_has_record_projects = "SELECT COUNT(project_id) FROM PROJECT_PROGRESS WHERE extracted_info = 1 and has_record = 1"
cursor.execute(SQL_has_record_projects)
number_of_project_has_record = cursor.fetchall()


print("number of projects in project progress table:", number_of_project_in_progress_table[0][0])
print("number of project extracted", number_of_project_extracted[0][0])
print("number of project has records", number_of_project_has_record[0][0])

add_sign = int(input("do you want to add projects to projects progress table? yes - 1 or no - 0 "))

if add_sign == 1:
    try:
        add_number = int(input("how many projects do you want to add?"))
        index_list = []
        for i in range(add_number):
            index_list.append([None])

        SQL_initial_index = "INSERT INTO PROJECT_PROGRESS(project_id)VALUES(%s)"
        cursor.executemany(SQL_initial_index, index_list)
        connection.commit()
    except:
        connection.rollback()
        print("error happen!")
    finally:
        SQL_count_projects = "SELECT COUNT(project_id) FROM PROJECT_PROGRESS"
        SQL_extracted_projects = "SELECT COUNT(project_id) FROM PROJECT_PROGRESS WHERE extracted_info = 1"
        cursor.execute(SQL_count_projects)
        number_of_project_in_progress_table = cursor.fetchall()
        cursor.execute(SQL_extracted_projects)
        number_of_project_extracted = cursor.fetchall()
        SQL_has_record_projects = "SELECT COUNT(project_id) FROM PROJECT_PROGRESS WHERE extracted_info = 1 and has_record = 1"
        cursor.execute(SQL_has_record_projects)
        number_of_project_has_record = cursor.fetchall()

        print("number of projects in project progress table:", number_of_project_in_progress_table[0][0])
        print("number of project extracted", number_of_project_extracted[0][0])
        print("number of project has records", number_of_project_has_record[0][0])


number_extracted = int(input('how many project do you want to extracted?'))

SQL_get_begin_id = "SELECT project_id FROM PROJECT_PROGRESS WHERE extracted_info = 0 LIMIT %s"%(number_extracted)
cursor.execute(SQL_get_begin_id)

result = cursor.fetchall()

project_list = [ele[0] for ele in result]

print(project_list)
print(len(project_list))

start = time.time()

get_product_list(project_list, cursor, connection)

end = time.time()

print("time spends", end - start)
connection.close()


