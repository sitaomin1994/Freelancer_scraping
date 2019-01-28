import pymysql
import time
import requests
import json
import re
from urllib.parse import urlencode

'''
# FREELANCERS_PROGRESS TABLE is to records what projects we have already fetched their bids info
# FREELANCERS TABLE information:
	
user_id: INT
status(verification info): TEXT
primary_language: VARCHAR(20) 
membership_package_name: TEXT
user_name: TEXT
display_name: TEXT
role: VARCHAR(20) 
user_url: TEXT
hourly_rate: FLOAT
tagline: TEXT
location_city: VARCHAR(100)
location_country: VARCHAR(100)
profile_description: LONGTEXT
avatar_large: TEXT
avatar: TEXT
njobs_all: INT
njobs_completed_all: INT 
pjobs_ontime_all: FLOAT
pjobs_onbudget_all: FLOAT
nreviews_all: INT 
rating_all: FLOAT 
rating_communication_all: FLOAT  
rating_expertise_all: FLOAT  
rating_hireagain_all: FLOAT  
rating_quality_all: FLOAT  
rating_professionalism_all: FLOAT  
njobs_12months: INT
njobs_completed_12months: INT
pjobs_ontime_12months: FLOAT 
pjobs_onbudget_12months: FLOAT  
nreviews_12months: INT 
rating_12months: FLOAT 
rating_communication_12months: FLOAT  
rating_expertise_12months: FLOAT  
rating_hireagain_12months: FLOAT 
rating_quality_12months: FLOAT 
rating_professionalism_12months: FLOAT  
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
earning_score: FLOAT 
currency: VARCHAR(100)
preferred_freelancer: bool 
registration_date: BIGINT
qualifications: INT
qualifications_type: TEXT 
qualifications_score: TEXT
qualifications_percentile: TEXT


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
def get_freelancer_info(freelancer_id_list, number_to_extracted, cursor, connection):
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

	counter = 0

	for counter in range(0, number_to_extracted): # get info for every 100 users per query (limit)
		freelancer_id = freelancer_id_list[100*(counter):100*(counter+1)]

		'''
		change token
		'''
		if (count == 500):
			token_index, token = generate_new_token(token_index)
			count = 1
		else:
			count = count+1
	
		'''
		make request
		'''
		h = {"Freelancer-OAuth-V1": token}
		params = {
							'users[]': [freelancer_id], 
							'avatar':True, 
							'profile_description':True, 
							'display_info':True, 
							'portfolio_details':True,
							'preferred_details':True, 'jobs':True,  'country_details':True, 'reputation':True, 'reputation_extra':True,
							'qualification_details':True, 'membership_details':True, 'user_recommendations':True
							} #  'cover_image':True, 'past_cover_images':True, COVER IS NOT USEFUL, avater requires profile_description = True
		url = 'https://www.freelancer.com/api/users/0.1/users/'# + str(freelancer_id) + '/'
		response = requests.get(url, headers=h, params=params)
		data_all = response.json()['result'] # data for the 100 freeelancers

		if response.status_code == 200:
			for i in freelancer_id: # for each of the 100 freelancers, save the info
				data = data_all.get('users').get(str(i))
				result = []
				# get freelancer profile information
				result.extend([
												data.get('id'),
												json.dumps(data.get('status')),
												data.get('primary_language'),
												data.get('membership_package').get('name'),
												data.get('username').replace("'",""),
												emoji_pattern.sub(r'', data.get('display_name').replace('\n', '')
                             .replace('\r', '').replace("'", "")) if data.get('display_name') != None else 'null',
												data.get('role'),
												str('https://www.freelancer.com/u/') + data.get('username'),
												data.get('hourly_rate') if data.get('hourly_rate') != None else -1,
												emoji_pattern.sub(r'', data.get('tagline').replace('\n', '')
                             .replace('\r', '').replace("'", "")) if data.get('tagline') != None else 'null',
												json.dumps(data.get('location').get('city')).replace("'","") ,
												json.dumps(data.get('location').get('country').get('name')).replace("'",""),
												emoji_pattern.sub(r'', data.get('profile_description').replace('\n', '')
														 .replace('\r', '').replace("'", "")) if data.get('profile_description') != None else 'null',
												data.get('avatar_large_cdn').replace("'","") if data.get('avatar_large_cdn') != None else "null",
												data.get('avatar_cdn').replace("'","") if data.get('avatar_cdn') != None else "null",
												data.get('reputation').get('entire_history').get('all'),
												data.get('reputation').get('entire_history').get('complete'),
												data.get('reputation').get('entire_history').get('on_time'),
												data.get('reputation').get('entire_history').get('on_budget'),
												data.get('reputation').get('entire_history').get('reviews'),
												data.get('reputation').get('entire_history').get('overall'),
												data.get('reputation').get('entire_history').get('category_ratings').get('communication'),
												data.get('reputation').get('entire_history').get('category_ratings').get('expertise'),
												data.get('reputation').get('entire_history').get('category_ratings').get('hire_again'),
												data.get('reputation').get('entire_history').get('category_ratings').get('quality'),
												data.get('reputation').get('entire_history').get('category_ratings').get('professionalism'),
												data.get('reputation').get('last12months').get('all'),
												data.get('reputation').get('last12months').get('complete'),
												data.get('reputation').get('last12months').get('on_time'), 
												data.get('reputation').get('last12months').get('on_budget'),
												data.get('reputation').get('last12months').get('reviews'),
												data.get('reputation').get('last12months').get('overall'),
												data.get('reputation').get('last12months').get('category_ratings').get('communication'),
												data.get('reputation').get('last12months').get('category_ratings').get('expertise'),
												data.get('reputation').get('last12months').get('category_ratings').get('hire_again'),
												data.get('reputation').get('last12months').get('category_ratings').get('quality'),
												data.get('reputation').get('last12months').get('category_ratings').get('professionalism'),
												data.get('reputation').get('last3months').get('all'),
												data.get('reputation').get('last3months').get('complete'),
												data.get('reputation').get('last3months').get('on_time'), 
												data.get('reputation').get('last3months').get('on_budget'),
												data.get('reputation').get('last3months').get('reviews'),
												data.get('reputation').get('last3months').get('overall'),
												data.get('reputation').get('last3months').get('category_ratings').get('communication'),
												data.get('reputation').get('last3months').get('category_ratings').get('expertise'),
												data.get('reputation').get('last3months').get('category_ratings').get('hire_again'),
												data.get('reputation').get('last3months').get('category_ratings').get('quality'),
												data.get('reputation').get('last3months').get('category_ratings').get('professionalism'),
												data.get('reputation').get('earnings_score'),
												data.get('primary_currency').get('code'),
												data.get('preferred_freelancer'),
												data.get('registration_date'),
												len(data.get('qualifications')),
												json.dumps([a.get('type') for a in data.get('qualifications')]) if data.get('qualifications') != None else "null",
												json.dumps([a.get('score_percentage') for a in data.get('qualifications')]) if data.get('qualifications') != None else "null",
												json.dumps([a.get('user_percentile') for a in data.get('qualifications')]) if data.get('qualifications') != None else "null"
								])

				#print(emoji_pattern.sub(r'', data.get('profile_description').replace('\n', '')
				#										 .replace('\r', '').replace("'", "")) if data.get('profile_description') != None else 'null',
				#								data.get('avatar_large_cdn').replace("'","") if data.get('avatar_large_cdn') != None else "null")

				SQL_insert_freelancer_profile = "REPLACE INTO FREELANCERS(user_id, status, primary_language, membership_package_name, user_name, display_name,"\
																								"role, user_url, hourly_rate, tagline, location_city, location_country, profile_description,"\
																								"avatar_large, avatar, njobs_all, njobs_completed_all, pjobs_ontime_all, pjobs_onbudget_all, "\
																								"nreviews_all, rating_all, rating_communication_all, rating_expertise_all, rating_hireagain_all, rating_quality_all, rating_professionalism_all, "\
																								"njobs_12months, njobs_completed_12months, pjobs_ontime_12months, pjobs_onbudget_12months, nreviews_12months,"\
																								"rating_12months, rating_communication_12months, rating_expertise_12months, rating_hireagain_12months, rating_quality_12months, rating_professionalism_12months,"\
																								"njobs_3months, njobs_completed_3months, pjobs_ontime_3months, pjobs_onbudget_3months, nreviews_3months, rating_3months, "\
																								"rating_communication_3months, rating_expertise_3months, rating_hireagain_3months, rating_quality_3months, rating_professionalism_3months, "\
																								"earning_score, currency, preferred_freelancer, registration_date, "\
																								"qualifications, qualifications_type, qualifications_score, qualifications_percentile)"\
																								"VALUES(%s, '%s', '%s', '%s', '%s',  '%s', "\
																								"'%s', '%s', %s, '%s', '%s', '%s', '%s', "\
																								"'%s', '%s',  %s, %s, %s, %s, "\
																								"%s, %s, %s, %s, %s, %s, %s, "\
																								"%s, %s, %s, %s, %s, "\
																								" %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "\
																								"%s, '%s', %s, %s, "\
																								"%s, '%s', '%s',  '%s')" % tuple(result)

				#print(SQL_insert_freelancer_profile)
				cursor.execute(SQL_insert_freelancer_profile)
				connection.commit()

				#save information to FREELANCERS_PROGRESS
				SQL_SAVE_PROGRESS = "UPDATE FREELANCERS_PROGRESS SET extracted = 1 WHERE user_id = %s;"%i
				cursor.execute(SQL_SAVE_PROGRESS)
				connection.commit()

		else:
			error = response.content
			print(error)
			time.sleep(5)
			break

		print(counter)
		counter += 1 # move to the next 100 freelncers on the list

	return 0


'''
main program
'''
connection  = pymysql.connect("mshresearch.marshall.usc.edu", "itroncos", "ismr_pswd","freelancer" )

cursor = connection.cursor()


create_progress_table = int(input("create freelancers_progress for the first time (if NO 0, if YES 1)?"))

if create_progress_table == 1:
		sql = """CREATE TABLE IF NOT EXISTS FREELANCERS_PROGRESS AS
						select distinct user_id 
						from BIDS
						order by user_id asc
				 """
		cursor.execute(sql)
		sql = """ALTER TABLE FREELANCERS_PROGRESS ADD COLUMN extracted INT default '0'
				 """
		cursor.execute(sql)


		sql = """CREATE TABLE IF NOT EXISTS FREELANCERS (
				 user_id INT, 
				 status TEXT,
				 primary_language VARCHAR(20), 
				 membership_package_name TEXT,
				 user_name TEXT,
				 display_name TEXT,
				 role VARCHAR(20), 
				 user_url TEXT,
				 hourly_rate FLOAT,
				 tagline TEXT,
				 location_city VARCHAR(100),
				 location_country VARCHAR(100),
				 profile_description LONGTEXT,
				 avatar_large TEXT,
				 avatar TEXT,
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
				 earning_score FLOAT, 
				 currency VARCHAR(100),
				 preferred_freelancer bool, 
				 registration_date BIGINT,
				 qualifications INT,
				 qualifications_type TEXT, 
				 qualifications_score TEXT,
				 qualifications_percentile TEXT,
				 PRIMARY KEY(user_id)) ENGINE=InnoDB DEFAULT CHARSET=utf8"""
		cursor.execute(sql)


SQL_all_freelancers_id = """SELECT DISTINCT user_id FROM FREELANCERS_PROGRESS WHERE extracted = 0 ORDER BY user_id ASC;"""

cursor.execute(SQL_all_freelancers_id)

freelancer_id_list = []
for row in cursor.fetchall():
				freelancer_id_list.append(row[0])

print("There are %d freelancers whose profiles haven't been extracted."% (len(freelancer_id_list)))
number_to_extracted = int(input("how many freelancers to extract?"))

start = time.time()
get_freelancer_info(freelancer_id_list, number_to_extracted, cursor, connection)
end = time.time()

print("Finished. Total time spend: ", end - start)

connection.close()



