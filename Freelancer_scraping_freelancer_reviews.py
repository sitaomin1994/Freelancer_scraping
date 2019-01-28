import pymysql
import time
import requests
import json
import re
from urllib.parse import urlencode
from datetime import datetime

'''
# freelancer reviews TABLE information:
  
review_id     INT
freelancer_id INT
employeer_id INT
project_id INT
project_url TEXT  default null
date DATETIME       
rating  FLOAT
rating_communication FLOAT
rating_expertise FLOAT
rating_hireagain FLOAT
rating_quality FLOAT
rating_professionalism FLOAT
on_time VARCHAR(20)
on_budget VARCHAR(20)
entry TEXT
bid_amount FLOAT
paid_amount FLOAT
review_status TEXT


Steps:
1. connected to database 
2. update freelancer reviews table --- copy all new projects to BIDS_PROGRESS from PROJECT table
3. get all projects that we haven't fetched bids yet.
4. determine how many projects you want to extracts their bids information
5. make requests, extract bids information and insert information to BIDS table
6. update BIDS_PROGRESS table, mark the project we have already extracted
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
generate new token from token pool
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
function to get bids info
'''


def get_bids_info(freelancer_list, times, cursor, connection):
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


        freelancer_id = freelancer_list[i]
        '''
        change token
        '''
        if (count == 500):
            token_index, token = generate_new_token(token_index)
            count = 1
        else:
            count = count + 1

        '''
        make request
        '''
        ERROR_SIGN_REVIEWS = False
        ERROR_SIGN_REQUEST = False
        error_set = []

        hasReviews = True
        reviews_count = 0
        offset = 0
        while(hasReviews):
            h = {"Freelancer-OAuth-V1": token}
            params = {
                'to_users[]': freelancer_id,
                'review_types':['project'],
                'ratings':True,
                'role':'freelancer',
                'offset': offset
            }
            url = 'https://www.freelancer.com/api/projects/0.1/reviews/'

            # print(project_id)
            #print(url)
            response = requests.get(url, headers=h, params=params)
            offset += 100

            print(json.dumps(response.json()))
            if response.status_code == 200:
                reviews = response.json()['result']['reviews']
                if len(reviews) == 0:
                    hasReviews = False
                    break
                else:
                    reviews_count += len(reviews)

                for index, review in enumerate(reviews):
                    result = []
                    #get review basic information
                    result.append(review['id'] if review['id'] is not None else -1)
                    result.append(review['to_user_id'] if review['to_user_id'] is not None else -1)
                    result.append(review['from_user_id'] if review['from_user_id'] is not None else -1)
                    result.append(review['project_id'] if review['project_id'] is not None else -1)
                    #project url
                    try:
                        result.append(json.dumps(review['review_context']['seo_url']).replace("\"",''))
                    except:
                        result.append('null')
                    #submit time
                    result.append(convert_utc_to_date(review['submitdate'] if review['submitdate'] is not None else 0))

                    #reviews rating
                    result.append(review['rating'] if review['rating'] is not None else -1)
                    try:
                        result.append(review.get('rating_details').get('category_ratings').get('communication')
                                      if review.get('rating_details').get('category_ratings').get('communication') is not None else -1)
                    except:
                        result.append(-1)

                    try:
                        result.append(review.get('rating_details').get('category_ratings').get('expertise')
                                      if review.get('rating_details').get('category_ratings').get('expertise') is not None else -1)
                    except:
                        result.append(-1)

                    try:
                        result.append(review.get('rating_details').get('category_ratings').get('hire_again')
                                      if review.get('rating_details').get('category_ratings').get('hire_again') is not None else -1)
                    except:
                        result.append(-1)

                    try:
                        result.append(review.get('rating_details').get('category_ratings').get('quality')
                                      if review.get('rating_details').get('category_ratings').get('quality') is not None else -1)
                    except:
                        result.append(-1)

                    try:
                        result.append(review.get('rating_details').get('category_ratings').get('professionalism')
                                      if review.get('rating_details').get('category_ratings').get('professionalism') is not None else -1)
                    except:
                        result.append(-1)

                    # on time
                    try:
                        result.append(review.get('rating_details').get('on_time_display') if review.get('rating_details').get('on_time_display') is not None else 'null')
                    except:
                        result.append('null')
                    # on budget
                    try:
                        result.append(review.get('rating_details').get('on_budget_display') if review.get('rating_details').get('on_budget_display') is not None else 'null')
                    except:
                        result.append('null')

                    #description
                    result.append(emoji_pattern.sub(r'', review['description'].replace('\n', '')
                             .replace('\r', '').replace("'", "")) if review['description'] != None else 'null')
                    #bid amount
                    result.append(review.get('bid_amount') if review['bid_amount'] is not None else -1)
                    #paid amount
                    result.append(review.get('paid_amount') if review['paid_amount'] is not None else -1)
                    #project status
                    result.append(json.dumps(review.get('review_project_status')).replace("\"", ''))

                    # try:
                    # [print(j, type(ele),ele) for j, ele in enumerate(result)]

                    SQL_insert_review = "REPLACE INTO FREELANCER_REVIEW(review_id, freelancer_id, employeer_id,"\
                                      "project_id, project_url, sumbit_date, rating, rating_communication, rating_expertise,"\
                                      "rating_hireagain, rating_quality, rating_professionalism, on_time, on_budget,"\
                                      "entry, bid_amount, paid_amount, review_status) VALUES(%s, %s, %s, %s, '%s', '%s', %s, %s, %s"\
                                      ", %s, %s, %s, '%s', '%s', '%s', %s, %s, '%s')" %  tuple(result)
                    #print(SQL_insert_review)
                    cursor.execute(SQL_insert_review)
                    connection.commit()
                    #print("iter %d bid %d finished" % (i, index))
                    # except:
                    #    error_sign = True
                    #    print('insert error at round %d iterator %d project_id %d'%(i,iter, project_list[iter]["id"]))
                    #    error_set.append(index)
                    #    connection.rollback()

            else:
                error = response.content
                print(error)
                print(i)
                time.sleep(5)
                ERROR_SIGN_REQUEST = True
        print('Freelancer %d finished'%(i))
        print('Number of reviews',reviews_count)


        # save information to BIDS_PROGRESS
        print('reviews extracted error:',ERROR_SIGN_REVIEWS)
        print('request error',ERROR_SIGN_REQUEST)
        print("====================================================================================================================================")
        if (ERROR_SIGN_REVIEWS == False and ERROR_SIGN_REQUEST == False):
           SQL_SAVE_PROGRESS = "UPDATE FREELANCER_REVIEW_PROGRESS SET extracted = 1, review_count = %s, error_type = 'null', error_details = 'null' WHERE freelancer_id = %s;" % (
            reviews_count, freelancer_id)
        elif ERROR_SIGN_REQUEST == True:
            SQL_SAVE_PROGRESS = "UPDATE FREELANCER_REVIEW_PROGRESS SET extracted = 0, error_type = 'request_error' WHERE freelancer_id = %s;" % (
               freelancer_id)
        else:
            SQL_SAVE_PROGRESS = "UPDATE FREELANCER_REVIEW_PROGRESS SET extracted = 0, error_type = 'reviews_error', error_details = '%s', bids_count = %S WHERE freelancer_id = %s;" % (
            json.dumps(error_set), reviews_count, freelancer_id, json.dumps(error_set))

        #print(SQL_SAVE_PROGRESS)
        cursor.execute(SQL_SAVE_PROGRESS)
        connection.commit()

    return 0


'''
main program
'''
# connecting to database
connection = pymysql.connect("mshresearch.marshall.usc.edu", "itroncos", "ismr_pswd", "freelancer")
cursor = connection.cursor()

first_time_sign = int(input("run this program for the first time? (yes 1 or no 0)"))
if(first_time_sign == 1):
        # create freelancer review progress table
        SQL_create_freelancer_review_progress = """
                                        CREATE TABLE IF NOT EXISTS FREELANCER_REVIEW_PROGRESS (
                                        freelancer_id INT,
                                        extracted BOOL DEFAULT 0,
                                        error_type VARCHAR(20) DEFAULT 'null',
                                        error_details TEXT,
                                        review_count INT DEFAULT 0,
                                        PRIMARY KEY(freelancer_id))
                                        """
        cursor.execute(SQL_create_freelancer_review_progress)
        connection.commit()

        # create freelancer reviews table
        SQL_create_freelancer_review_table = """
                                     CREATE TABLE IF NOT EXISTS FREELANCER_REVIEW(
                                            review_id INT DEFAULT -1,  
                                            freelancer_id INT DEFAULT -1,
                                            employeer_id INT DEFAULT -1,
                                            project_id INT DEFAULT -1,
                                            project_url TEXT,
                                            sumbit_date DATETIME,         
                                            rating  FLOAT DEFAULT -1,
                                            rating_communication FLOAT DEFAULT -1,
                                            rating_expertise FLOAT DEFAULT -1,
                                            rating_hireagain FLOAT DEFAULT -1,
                                            rating_quality FLOAT DEFAULT -1,
                                            rating_professionalism FLOAT DEFAULT -1,
                                            on_time VARCHAR(20) DEFAULT -1,
                                            on_budget VARCHAR(20) DEFAULT -1,
                                            entry TEXT,
                                            bid_amount FLOAT,
                                            paid_amount FLOAT,
                                            review_status TEXT,
                                            PRIMARY KEY(review_id))
                                    """
        cursor.execute(SQL_create_freelancer_review_table)
        connection.commit()

#update and get progress information
update_freelancer_review_progress = int(input("Do you want to update the freelancer_review progress or not?(yes 1 or no 0)"))

if(update_freelancer_review_progress == 1):
    SQL_update_freelancer_review_progress = """
                              INSERT INTO FREELANCER_REVIEW_PROGRESS (freelancer_id) SELECT user_id FROM FREELANCERS WHERE FREELANCERS.user_id NOT IN (SELECT freelancer_id FROM FREELANCER_REVIEW_PROGRESS);
                             """
    cursor.execute(SQL_update_freelancer_review_progress)
    connection.commit()


#get freelancer id list
SQL_all_freelancer_review_not_extracted = """
                                          SELECT freelancer_id FROM FREELANCER_REVIEW_PROGRESS WHERE extracted = 0 ORDER BY freelancer_id ASC;
                                          """

cursor.execute(SQL_all_freelancer_review_not_extracted)

freelancer_id_list = cursor.fetchall()

print("There are %d freelancers whose reviews haven't been extracted." % (len(freelancer_id_list)))
number_to_extracted = int(input("how many freelancers to extract?"))
freelancer_list = [ freelancer_id[0] for freelancer_id in freelancer_id_list]

start = time.time()
get_bids_info(freelancer_list, number_to_extracted, cursor, connection)
end = time.time()

print("Finished. Total time spend: ", end - start)

connection.close()



