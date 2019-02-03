import pymysql
import time
import requests
import json
import re
from urllib.parse import urlencode
from datetime import datetime

'''
# BIDS_PROGRESS TABLE is to records what projects we have already fetched their bids info
# BIDS TABLE information:
  
  bids_id: INT
  project_id: INT
  user_id:INT
  position_rank: INT
  time_submitted: BIGINT
  amount_offered: FLOAT
  hourly_rate_offered: FLOAT
  days_offered: INT
  hireme_counter_offer: INT
  highlighted: BOOL
  sponsored: FLOAT
  negotiated_offer: TEXT
  retracted: BOOL
  application_entry:TEXT
+ award_status: VARCHAR(20)
+ paid_status:VARCHAR(20)
+ complete_status: VARCHAR(20)
+ time_awarded: VARCHAR(20)
  earning_score_category: FLOAT

  njobs_all： INT
  njobs_completed_all：INT 
  pjobs_ontime_all: FLOAT 
  pjobs_onbudget_all: FLOAT 
  nreviews_all: INT 
  rating_all: FLOAT
  rating_communication_all: FLOAT
  rating_expertise_all: FLOAT
  rating_hireagain_all: FLOAT
  rating_quality_all: FLOAT
  rating_professionalism_all: FLOAT 

  njobs_3months: INT
  njobs_completed_3months: INT 
  pjobs_ontime_3months: FLOAT
  pjobs_onbudget_3months: FLOAT
  nreviews_3months: INT
  rating_3months: FLOAT
  rating_communication_3months: FLOAT
  rating_expertise_3months: FLOAT
  rating_hireagain_3months: FLOAT 
  rating_quality_3months: FLOAT 
  rating_professionalism_3months: FLOAT
  
+ njobs_12months: INT
+ njobs_completed_12months: INT 
+ pjobs_ontime_12months: FLOAT
+ pjobs_onbudget_12months: FLOAT
+ nreviews_12months: INT
+ rating_12months: FLOAT
+ rating_communication_12months: FLOAT
+ rating_expertise_12months: FLOAT
+ rating_hireagain_12months: FLOAT 
+ rating_quality_12months: FLOAT 
+ rating_professionalism_12months: FLOAT
 
  avatar_big_url TEXT 
  avatar_url TEXT 
  member_category TEXT 
  portolio_count INT 
  primary_language VARCHAR(20) 
  primary_currency VARCHAR(20)
  freelancer_hourly_rate FLOAT 
  country VARCHAR(20)
  city VARCHAR(20)
  status (verifications) TEXT
  prefered_freelancer BOOL 
  qualifications TEXT
  qualifications_type 
  qualifications_score
  qualifications_percentile

Steps:
1. connected to database 
2. update BIDS_PROGRESS table --- copy all new projects to BIDS_PROGRESS from PROJECT table
3. get all projects that we haven't fetched bids yet.
4. determine how many projects you want to extracts their bids information
5. make requests, extract bids information and insert information to BIDS table
6. update BIDS_PROGRESS table, mark the project we have already extracted
'''

'''
token pool
'''
token_pool = ['vRFZUqclBzv7VzpyTJ3vfrc8RwvoGq', 'AXGnj1OHV5Le3jKhrdobCl4tZ2q9cm','YwQ2ZFfJehpUj7UEOGpUn3y5pcpUtJ',
              'IHPDP3HoqjWX1tTqUtMu4EAqxlMRlW','5qfr3iCqMgBerHmZJDMO3Co2AV3a4d','qIo7Id3Dcdw528IzQU59K94I3cXWHZ',
              'jDaI7PSRa8H375Bn4B81mb6IlvS3sc','FtQ20tausNVHvCpKBInosYFjv4FPTP','Z3XgZKiXp7QgPwqRKB9tnKcFnLt1eb'
              'pRq8XNGo0J14kA7JnXOXy2KJfSjsCR','zURlph7AJqSNtcUFlZUxmKlr5R8XRd','0oUrcZIfew5qHbvgpZE5rN5s2Le4UF']


def convert_utc_to_date(time):
    return datetime.utcfromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

'''
generate new token from token pool
'''
def generate_new_token(index):

    if(index == len(token_pool)-1):
        new_index = 0
    else:
        new_index = index+1

    return new_index, token_pool[new_index]

'''
function to get bids info
'''
def get_bids_info(project_list, times, cursor, connection):


    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               u"\U00002702-\U000027B0"
                               u"\U000024C2-\U0001F251"
                               "]+", flags=re.UNICODE)

    token_index = 0

    token_index, token = generate_new_token(token_index)
    count = 1

    number_of_bids = 0
    for i in range(times):

        '''
        change token
        '''
        if (count == 300):
            token_index, token = generate_new_token(token_index)
            count = 1
        else:
            count = count+1

        hasBids = True
        offset = 0
        bids_count = 0
        ERROR_SIGN_BIDS = False
        ERROR_SIGN_REQUEST = False
        error_set = []

        while(hasBids):
            '''
            make request
            '''
            project_id = project_list[i][0]
            h = {"Freelancer-OAuth-V1": token}
            params = {
                'reputation':True,
                'project_id': project_id,
                'reputation': True,
                'user_details': True,
                'user_avatar': True,
                'user_membership_details': True,
                'user_reputation': True,
                'user_reputation_extra': True,
                'user_status': True,
                'user_portfolio_details': True,
                'user_preferred_details': True,
                'user_qualification_details': True,
                'offset': offset
            }
            url = 'https://www.freelancer.com/api/projects/0.1/projects/' + str(project_id) + '/bids/'

            #print(project_id)
            #print(url)
            response = requests.get(url, headers=h, params=params)
            offset += 100
            #print(json.dumps(response.json()))

            if response.status_code == 200:
                bids = response.json()['result']['bids']
                users = response.json()['result']['users']

                if len(bids) == 0:
                    hasBids = False
                    break
                else:
                    bids_count += len(bids)
                    number_of_bids += len(bids)

                for index,bid in enumerate(bids):
                    result = []
                    # get bid information
                    result.extend([
                            bid['id'],
                            bid['project_id'] if bid['project_id'] != None else -1,
                            bid['bidder_id'] if bid['bidder_id'] != None else -1,
                            index,
                            convert_utc_to_date(bid['time_submitted'] if bid['time_submitted'] != None else 0),
                            bid['amount'] if bid['amount'] != None else -1,
                            bid['hourly_rate'] if bid['hourly_rate'] != None else -1,
                            bid['period'] if bid['period'] != None else -1,
                            bid['hireme_counter_offer'] if bid['hireme_counter_offer'] != None else -1,
                            bid['highlighted'] if bid['highlighted'] != None else False,
                            bid['sponsored'] if bid['sponsored'] != None else -1,
                            json.dumps(bid['negotiated_offer']).replace("'","").replace("\\",""),
                            bid['retracted'] if bid['retracted'] != None else False,
                            emoji_pattern.sub(r'', bid['description'].replace('\n', '')
                                 .replace('\r', '').replace("'", "").replace("\\","")) if bid['description'] != None else 'null',
                            bid['award_status'] if bid['award_status'] != None else "null",
                            bid['paid_status'] if bid['paid_status'] != None else "null",
                            bid['complete_status'] if bid['complete_status'] != None else "null",
                            json.dumps(bid["time_awarded"]),

                            bid['reputation']['earnings_score'] if bid['reputation']['earnings_score'] != None else -1,
                            bid["reputation"]["entire_history"]["all"],
                            bid["reputation"]["entire_history"]["complete"],
                            bid["reputation"]["entire_history"]["on_time"],
                            bid["reputation"]["entire_history"]["on_budget"],
                            bid.get("reputation").get("entire_history").get("reviews"),
                            bid.get("reputation").get("entire_history").get("overall"),
                            bid.get("reputation").get("entire_history").get("category_ratings").get("communication"),
                            bid.get("reputation").get("entire_history").get("category_ratings").get("expertise"),
                            bid.get("reputation").get("entire_history").get("category_ratings").get("hire_again"),
                            bid.get("reputation").get("entire_history").get("category_ratings").get("quality"),
                            bid.get("reputation").get("entire_history").get("category_ratings").get("professionalism"),
                            bid.get("reputation").get("last3months").get("all"),
                            bid.get("reputation").get("last3months").get("complete"),
                            bid.get("reputation").get("last3months").get("on_time"),
                            bid.get("reputation").get("last3months").get("on_budget"),
                            bid.get("reputation").get("last3months").get("reviews"),
                            bid.get("reputation").get("last3months").get("overall"),
                            bid.get("reputation").get("last3months").get("category_ratings").get("communication"),
                            bid.get("reputation").get("last3months").get("category_ratings").get("expertise"),
                            bid.get("reputation").get("last3months").get("category_ratings").get("hire_again"),
                            bid.get("reputation").get("last3months").get("category_ratings").get("quality"),
                            bid.get("reputation").get("last3months").get("category_ratings").get("professionalism"),
                            bid.get("reputation").get("last12months").get("all"),
                            bid.get("reputation").get("last12months").get("complete"),
                            bid.get("reputation").get("last12months").get("on_time"),
                            bid.get("reputation").get("last12months").get("on_budget"),
                            bid.get("reputation").get("last12months").get("reviews"),
                            bid.get("reputation").get("last12months").get("overall"),
                            bid.get("reputation").get("last12months").get("category_ratings").get("communication"),
                            bid.get("reputation").get("last12months").get("category_ratings").get("expertise"),
                            bid.get("reputation").get("last12months").get("category_ratings").get("hire_again"),
                            bid.get("reputation").get("last12months").get("category_ratings").get("quality"),
                            bid.get("reputation").get("last12months").get("category_ratings").get("professionalism")
                    ])

                    #get bidder information
                    if(bid['bidder_id'] != None):
                        user_id = str(bid['bidder_id'])
                        if user_id in users:
                            user_info = users[user_id]

                            result.append(user_info['avatar_large_cdn'].replace("'","").replace("\\","") if user_info['avatar_large_cdn'] != None else "null")
                            result.append(user_info['avatar_cdn'].replace("'","").replace("\\","") if user_info['avatar_cdn'] != None else "null")
                            try:
                                result.append(json.dumps(user_info['membership_package']['name']).replace("'","").replace("\\",""))
                            except:
                                result.append('null')

                            result.append(user_info['portfolio_count'] if user_info['portfolio_count'] != None else -1)

                            result.append(json.dumps(user_info['primary_language']).replace("'","").replace("\\",""))
                            try:
                                result.append(json.dumps(user_info['primary_currency']['country']).replace("'","").replace("\\",""))
                            except:
                                result.append('null')

                            result.append(user_info['hourly_rate'] if user_info['hourly_rate'] != None else -1)

                            try:
                                result.append(json.dumps(user_info['location']['country']['name']).replace("'","").replace("\\",""))
                            except:
                                result.append('null')
                            try:
                                result.append(json.dumps(user_info['location']['city']).replace("'","").replace("\\",""))
                            except:
                                result.append('null')

                            result.append(json.dumps(user_info['status']).replace("'","").replace("\\",""))
                            result.append(user_info['preferred_freelancer'] if user_info['preferred_freelancer'] != None else False)
                            result.append(json.dumps(user_info['qualifications']).replace("'","").replace("\\",""))

                            try:
                                result.append(json.dumps([q.get('type') for q in user_info['qualifications']]).replace("'","").replace("\\",""))
                            except:
                                result.append("null")

                            try:
                                result.append(json.dumps([q.get('score_percentage') for q in user_info['qualifications']]).replace("'","").replace("\\",""))
                            except:
                                result.append("null")

                            try:
                                result.append(json.dumps([q.get('user_percentile') for q in user_info['qualifications']]).replace("'","").replace("\\",""))
                            except:
                                result.append("null")

                        else:
                            result.extend(["null","null", "null", -1, "null", "null", -1,"null","null","null", False, "null","null","null","null"])
                    else:
                        result.extend(["null", "null", "null", -1, "null", "null", -1, "null", "null", "null", False, "null","null","null","null"])


                    SQL_insert_bids = "REPLACE INTO BIDS(bids_id, project_id, user_id, position_rank, time_submitted, amount_offered,"\
		                              "hourly_rate_offered, days_offered, hireme_counter_offer, highlighted, sponsored, negotiated_offer,"\
                                      "retracted, application_entry, award_status, paid_status, complete_status, time_awarded,"\
                                      "earning_score_category, njobs_all, njobs_completed_all,  pjobs_ontime_all,  pjobs_onbudget_all,"\
                                      "nreviews_all, rating_all, rating_communication_all, rating_expertise_all, rating_hireagain_all,"\
                                      "rating_quality_all, rating_professionalism_all, njobs_3months, njobs_completed_3months,"\
                                      "pjobs_ontime_3months, pjobs_onbudget_3months, nreviews_3months, rating_3months, rating_communication_3months,"\
                                      "rating_expertise_3months, rating_hireagain_3months, rating_quality_3months, rating_professionalism_3months,"\
                                      "njobs_12months, njobs_completed_12months, pjobs_ontime_12months, pjobs_onbudget_12months, nreviews_12months,"\
                                      "rating_12months, rating_communication_12months, rating_expertise_12months, rating_hireagain_12months,"\
                                      "rating_quality_12months, rating_professionalism_12months, avatar_big_url, avatar_url, member_category,"\
                                      "portolio_count, primary_language, primary_currency, freelancer_hourly_rate, country, city,"\
                                      "status_verifications, prefered_freelancer,  qualifications,qualifications_type ,qualifications_score, qualifications_percentile) VALUES"\
                                      " (%s, %s, %s, %s, '%s', %s, %s, %s, %s, %s, %s, '%s', %s, '%s', '%s',  '%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s,"\
                                      " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                      " %s, %s, '%s', '%s', '%s',  %s, '%s', '%s', %s, '%s', '%s', '%s', %s, '%s','%s','%s','%s')" % tuple(result)
                    #print(SQL_insert_bids)
                    cursor.execute(SQL_insert_bids)
                    connection.commit()
                    #print("iter %d bid %d finished"%(i, index))
                    #except:
                    #    error_sign = True
                    #    print('insert error at round %d iterator %d project_id %d'%(i,iter, project_list[iter]["id"]))
                    #    error_set.append(index)
                    #    connection.rollback()
            else:
                error = response.content
                print(error)
                print(i)
                ERROR_SIGN_REQUEST = True
                token_index, token = generate_new_token(token_index)



        #save information to BIDS_PROGRESS
        if(ERROR_SIGN_BIDS == False and ERROR_SIGN_REQUEST == False):
            SQL_SAVE_PROGRESS = "UPDATE BIDS_PROGRESS SET extracted = 1, bids_count = %s, error_type = 'null', error_details = 'null' WHERE project_id = %s;"%(bids_count, project_id)
        elif ERROR_SIGN_REQUEST == True:
            SQL_SAVE_PROGRESS = "UPDATE BIDS_PROGRESS SET extracted = 0, error_type = 'request_error' WHERE project_id = %s;"%(project_id)
        else:
            SQL_SAVE_PROGRESS = "UPDATE BIDS_PROGRESS SET extracted = 0, error_type = 'bids_error', error_details = '%s', bids_count = %S WHERE project_id = %s;"%(json.dumps(error_set),bids_count, project_id, json.dumps(error_set))

        #print(SQL_SAVE_PROGRESS)
        print(ERROR_SIGN_BIDS)
        print(ERROR_SIGN_REQUEST)
        print("project %d finished"%(i))
        print("number of bids: %d"%(bids_count))
        print("======================================================================================================================================================")
        cursor.execute(SQL_SAVE_PROGRESS)
        connection.commit()

        print("total bids extracted: %d"%(number_of_bids))
    return 0


'''
main program
'''
# connecting to database
connection  = pymysql.connect("mshresearch.marshall.usc.edu","itroncos","ismr_pswd","freelancer" )
cursor = connection.cursor()

first_time_sign = int(input("are you first time run bids programm? yes - 1 or no - 0"))

if first_time_sign == 1:
    SQL_create_bid_table = """
        create TABLE IF NOT EXISTS BIDS(
		    bids_id INT,
		    project_id INT,
		    user_id INT,
		    position_rank INT,
		    time_submitted DATETIME,
		    amount_offered FLOAT,
		    hourly_rate_offered FLOAT,
		    days_offered INT,
		    hireme_counter_offer INT,
		    highlighted BOOL,
		    sponsored FLOAT,
		    negotiated_offer TEXT,
		    retracted BOOL,
		    application_entry TEXT,
		    award_status VARCHAR(20),
		
		    paid_status VARCHAR(20),
		    complete_status VARCHAR(20),
		    time_awarded VARCHAR(100),
		    earning_score_category FLOAT,
		    njobs_all INT,
		
		    njobs_completed_all INT, 
		    pjobs_ontime_all FLOAT, 
		    pjobs_onbudget_all FLOAT, 
		    nreviews_all INT, 
		    rating_all FLOAT,
		
		    rating_communication_all FLOAT,
		    rating_expertise_all FLOAT,
		    rating_hireagain_all FLOAT,
		    rating_quality_all FLOAT,
		    rating_professionalism_all FLOAT,
		    njobs_3months INT,
		    njobs_completed_3months INT,
		    pjobs_ontime_3months FLOAT,
		    pjobs_onbudget_3months FLOAT,
		    nreviews_3months INT,
	
		    rating_3months FLOAT,
		    rating_communication_3months FLOAT,
		    rating_expertise_3months FLOAT,
		    rating_hireagain_3months FLOAT,
		    rating_quality_3months FLOAT, 
		    rating_professionalism_3months FLOAT,
		    njobs_12months INT,
		    njobs_completed_12months INT,
		    pjobs_ontime_12months FLOAT,
		    pjobs_onbudget_12months FLOAT,
		
		    nreviews_12months INT,
		    rating_12months FLOAT,
		    rating_communication_12months FLOAT,
		    rating_expertise_12months FLOAT,
		    rating_hireagain_12months FLOAT,
		
		    rating_quality_12months FLOAT, 
		    rating_professionalism_12months FLOAT,
		    avatar_big_url TEXT, 
		    avatar_url TEXT, 
		    member_category TEXT,
		
		    portolio_count INT, 
		    primary_language VARCHAR(100), 
		    primary_currency VARCHAR(20),
		    freelancer_hourly_rate FLOAT, 
		    country VARCHAR(1000),
		    city VARCHAR(100),
		    status_verifications TEXT,
		    prefered_freelancer BOOL, 
		    qualifications TEXT,
		    qualifications_type TEXT,
            qualifications_score TEXT,
            qualifications_percentile TEXT,
		    primary key(bids_id));
    """
    cursor.execute(SQL_create_bid_table)
    connection.commit()

    SQL_creat_bid_progress_table = """CREATE TABLE IF NOT EXISTS BIDS_PROGRESS(
                                            project_id INT, 
                                            extracted BOOL DEFAULT 0,
                                            error_type VARCHAR(20) DEFAULT 'null',
                                            error_details VARCHAR(200) DEFAULT 'null',
                                            bids_count INT,
                                            PRIMARY KEY(project_id))"""
    #print(SQL_creat_bid_progress_table)
    cursor.execute(SQL_creat_bid_progress_table)
    connection.commit()

#update and get progress information
update_progress = int(input("do you want to update project ids of bids_progress table? yes -1 or no -0"))
if update_progress == 1:
    SQL_update_progress_pj = """INSERT INTO BIDS_PROGRESS(project_id) SELECT project_id FROM PROJECT WHERE PROJECT.project_id NOT IN (SELECT project_id FROM BIDS_PROGRESS)"""
    cursor.execute(SQL_update_progress_pj)
    connection.commit()


SQL_total_project = """SELECT COUNT(project_id) FROM BIDS_PROGRESS"""
cursor.execute(SQL_total_project)
number_of_project = cursor.fetchall()
print('There are %d project total in progress table'%(number_of_project[0][0]))

SQL_all_pj_not_extracted = """SELECT project_id FROM BIDS_PROGRESS WHERE extracted = 0 ORDER BY project_id ASC;"""
cursor.execute(SQL_all_pj_not_extracted)
project_id_list = cursor.fetchall()
print("There are %d projects whose bids haven't been extracted."% (len(project_id_list)))


number_to_extracted = int(input("how many projects to extract?"))
start = time.time()
get_bids_info(project_id_list, number_to_extracted, cursor, connection)
end = time.time()

print("Finished. Total time spend: ", end - start)

connection.close()



