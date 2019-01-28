import csv
import requests
from bs4 import BeautifulSoup, SoupStrainer
import datetime as dt
import codecs
import pandas as pd
import lxml
import numpy as np
import urllib.request
import os
import time
import asyncio
import aiohttp


def getInnerText(elem):
	if elem and elem.contents:
		return elem.contents[0].strip().replace('\n', '');
	return ''

def getProjectID(elem):
	if len(elem) > 0:
		elem = elem[0]
		if elem and elem.contents:
			return elem.contents[1].strip().replace('\n', '')
	return ''


def get_project(projectUrl, login_request, projectsID, employerNreviews, employerAvgreviews, employerLocation,
                      employerVerifications, projectStatus, winnerID):

    start1 = time.time()
    res = requests.get(projectUrl)
    end1 = time.time()

    start2 = time.time()
    content = BeautifulSoup(res.content, 'lxml',
                             parse_only=SoupStrainer(['a', 'span', 'div', 'p', 'li']))  # 'html.parser')
    projectIDWrapper = content.find_all('p', class_='PageProjectViewLogout-detail-tags') or []
    projectID = getProjectID(projectIDWrapper[-1:])
    project_status = ''
    winner_id = []

    if projectID == '':  # website is asking for login, couldn't get the info for this project
        login_request += 1
        projectsID.append(str('login required'))
        employerNreviews.append(str('login required'))
        employerAvgreviews.append(str('login required'))
        employerLocation.append(str('login required'))
        employerVerifications.append(str('login required'))
        projectStatus.append(str('login required'))
        winnerID.append(str('login required'))
    else:
        projectsID.append(projectID)
        employerNreviews.append(getInnerText(content.find('span',class_='Rating-review')))
        employerAvgreviews.append(content.find('span',class_="Rating Rating--labeled profile-user-rating PageProjectViewLogout-detail-reputation-item")[
                                      'data-star_rating'])
        employerLocation.append(getInnerText(
            content.find_all('span', class_='PageProjectViewLogout-detail-reputation-item-locationItem')[1]))
        employerVerifications.append([item['data-qtsb-label'] for item in content.find_all('li',class_="is-verified verified-item Tooltip--top")])  # CHECK THIS, LIST OF LIST MAYBE PROBLEMATIC
        # project status
        if (content.find('span', class_="Icon Icon--small Icon--light")):
            project_status = 'Completed'  # {Completed} projects have special notation
        else:
            project_status = getInnerText(content.find('span', class_="promotion-tag"))  # {Open, Ended, project In Progress}
            winner_id = ['notyet', 'notyet']
        if (project_status != 'Open'):  # status of the project: ended (w/out winner) vs. in progress
            if content.find('div', class_="PageProjectViewLogout-awardedTo"):
                winner_id = [getInnerText(content.find_all('a', class_="FreelancerInfo-username")[0]),content.find_all('a', class_="FreelancerInfo-username")[0]['data-freelancer-profile-lnk']]  # register the winner
            else:
                winner_id = ['none', 'none']

    projectStatus.append(project_status)
    winnerID.append(winner_id)  # CHECK THIS, LIST OF LIST MAYBE PROBLEMATIC
    end2 = time.time()

    return end1-start1, end2-start2

def projectListings(numPages, keyword):  # took 7 minutes to (3, no keyword)

    login_request = 0
    projectsID = []
    employerNreviews = []
    employerAvgreviews = []
    employerLocation = []
    employerVerifications = []
    projectStatus = []
    winnerID = []
    urls = []
    title = []
    date = []
    daysleft = []
    description = []
    tags = []
    price = []
    bids = []
    category = []
    status = []

    for pageNum in range(1, numPages + 1):

        # url = 'https://www.freelancer.com/jobs/%s' % pageNum # open anc closed jobs
        start1 = time.time()
        url = 'https://www.freelancer.com/jobs/%s' % pageNum
        if keyword:
            extra = '/?status=all&results=100&keyword=%s' % keyword
            url += extra
        else:
            extra = '/?status=all&results=100'
            url += extra

        res = requests.get(url)
        end1 = time.time()

        start2 = time.time()
        content = BeautifulSoup(res.content, 'lxml',
                                parse_only=SoupStrainer(['a', 'span', 'div', 'p', 'li']))  # 'html.parser')

        projectList = content.find(id='project-list').find_all(class_='JobSearchCard-item')

        print('Scraping job listings on page: %s' % url)

        aux = []

        for project in projectList:
            urls.append('https://www.freelancer.com%s' % project.find('a', class_='JobSearchCard-primary-heading-link')['href'])
            title.append(getInnerText(project.find('a', class_='JobSearchCard-primary-heading-link')))
            date.append(dt.datetime.now().isoformat())
            daysleft.append(getInnerText(project.find('span', class_='JobSearchCard-primary-heading-days')))
            description.append(project.find('p', class_='JobSearchCard-primary-description'))
            tags.append(', '.join([getInnerText(tag) for tag in project.find_all('a', class_='JobSearchCard-primary-tagsLink')]))
            price.append(project.find('div', class_='JobSearchCard-secondary-price'))
            bids.append(project.find('div', class_='JobSearchCard-secondary-entry'))
            aux.append(project.find('span', class_="Icon JobSearchCard-primary-heading-Icon"))

        category += ['Job' if j is None else 'Contest' for j in aux]  # we have to exclude contest at the end
        status += [d if d == 'Ended' else 'Open' for d in daysleft]

        end2 = time.time()

        print(end1-start1, end2-start2)

        req_total = 0
        parse_total = 0

        for url in urls:
            req_time, parse_time = get_project(url,login_request, projectsID, employerNreviews, employerAvgreviews, employerLocation, employerVerifications, projectStatus, winnerID)
            req_total += req_time
            parse_total += parse_time

        print(req_total, parse_total)

        '''
        # Table: Jobs List
        urls += ['https://www.freelancer.com%s' % project.find('a', class_='JobSearchCard-primary-heading-link')['href']
                 for project in projectList]  # url to the job contest
        
        title += [getInnerText(project.find('a', class_='JobSearchCard-primary-heading-link')) for project in
                  projectList]  # job title
        
        date += [dt.datetime.now().isoformat() for project in projectList]  # scraping date
        
        daysleft += [getInnerText(project.find('span', class_='JobSearchCard-primary-heading-days')) for project in
                     projectList]  # remaining days
        
        description += [getInnerText(project.find('p', class_='JobSearchCard-primary-description')) for project in
                        projectList]
        tags += [
            ', '.join([getInnerText(tag) for tag in project.find_all('a', class_='JobSearchCard-primary-tagsLink')]) for
            project in projectList]
        
        price += [getInnerText(project.find('div', class_='JobSearchCard-secondary-price')) for project in projectList]
        bids += [getInnerText(project.find('div', class_='JobSearchCard-secondary-entry')) for project in projectList]

        # Table: Jobs Status
        # urls
        # date
        aux = [project.find('span', class_="Icon JobSearchCard-primary-heading-Icon") for project in projectList]
        category += ['Job' if j is None else 'Contest' for j in aux]  # we have to exclude contest at the end
        status += [d if d == 'Ended' else 'Open' for d in daysleft]
        # winner
        '''
        '''
        for projectUrl in urls:

            res2 = requests.get(projectUrl)

            content2 = BeautifulSoup(res2.content, 'lxml',
                                     parse_only=SoupStrainer(['a', 'span', 'div', 'p', 'li']))  # 'html.parser')
            projectIDWrapper = content2.find_all('p', class_='PageProjectViewLogout-detail-tags') or []
            projectID = getProjectID(projectIDWrapper[-1:])

            if projectID == '':  # website is asking for login, couldn't get the info for this project
                login_request += 1
                projectsID.append(str('login required'))
                employerNreviews.append(str('login required'))
                employerAvgreviews.append(str('login required'))
                employerLocation.append(str('login required'))
                employerVerifications.append(str('login required'))
                projectStatus.append(str('login required'))
                winnerID.append(str('login required'))
                continue
            else:
                projectsID.append(projectID)
                employerNreviews.append(getInnerText(content2.find('span',
                                                                   class_='Rating-review')))  # .split()[1] # same under 'div' gives the list of freelancers ratings
                employerAvgreviews.append(content2.find('span',
                                                        class_="Rating Rating--labeled profile-user-rating PageProjectViewLogout-detail-reputation-item")[
                                              'data-star_rating'])
                employerLocation.append(getInnerText(
                    content2.find_all('span', class_='PageProjectViewLogout-detail-reputation-item-locationItem')[1]))
                employerVerifications.append([item['data-qtsb-label'] for item in content2.find_all('li',
                                                                                                    class_="is-verified verified-item Tooltip--top")])  # CHECK THIS, LIST OF LIST MAYBE PROBLEMATIC
                # project status
                if (content2.find('span', class_="Icon Icon--small Icon--light")):
                    project_status = 'Completed'  # {Completed} projects have special notation
                else:
                    project_status = getInnerText(
                        content2.find('span', class_="promotion-tag"))  # {Open, Ended, project In Progress}
                    winner_id = ['notyet', 'notyet']
                if (project_status != 'Open'):  # status of the project: ended (w/out winner) vs. in progress
                    if content2.find('div', class_="PageProjectViewLogout-awardedTo"):
                        winner_id = [getInnerText(content2.find_all('a', class_="FreelancerInfo-username")[0]),
                                     content2.find_all('a', class_="FreelancerInfo-username")[0][
                                         'data-freelancer-profile-lnk']]  # register the winner
                    else:
                        winner_id = ['none', 'none']  # outside option
            projectStatus.append(project_status)
            winnerID.append(winner_id)  # CHECK THIS, LIST OF LIST MAYBE PROBLEMATIC
            '''

    # create tables and export data
    table_joblist = pd.DataFrame(list(zip(date, urls, title, daysleft, description, tags, price, status)),
                                 columns=['date', 'project_url', 'title', 'daysleft', 'description', 'tags', 'price',
                                          'status'])
    table_jobdescription = pd.DataFrame(
        list(zip(urls, projectsID, employerNreviews, employerAvgreviews, employerLocation, employerVerifications)),
        columns=['project_url', 'projects_id', 'employer_nreviews', 'employer_avgreviews', 'employer_location',
                 'employer_verifications'])
    table_jobstatus = pd.DataFrame(list(zip(urls, projectsID, projectStatus, winnerID, date)),
                                   columns=['project_url', 'project_id', 'project_status', 'winner_id', 'date'])

    table_joblist.to_csv(r'table_projectList.txt', header=None, index=None, sep=' ', mode='a')
    table_jobdescription.to_csv(r'table_projectDescription.txt', header=None, index=None, sep=' ', mode='a')
    table_jobstatus.to_csv(r'table_projectStatus.txt', header=None, index=None, sep=' ', mode='a')




numPages = 1
keyword = ''

start = time.time()
projectListings(numPages, keyword)
end = time.time()

cost = end - start
print('Cost time:', cost)

#projectApplicantions()
#freelancersData(keyword)
