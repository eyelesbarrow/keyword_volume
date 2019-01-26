import pandas as pd
import json
from bwproject import BWProject
from bwdata import BWData
from bwresources import BWQueries
import logging
import os
from collections import OrderedDict
logger = logging.getLogger("bwapi")



#file = "/home/kla/Documents/Scripts/sample_csv.csv" # change this to current file
file_date = pd.Timestamp('today')
path = '' # add working directory - where everything will be saved - here 
os.chdir(path)

YOUR_ACCOUNT = ""
YOUR_PASSWORD = ""
YOUR_PROJECT = ""

project = BWProject(username=YOUR_ACCOUNT, password=YOUR_PASSWORD, project=YOUR_PROJECT)
queries = BWQueries(project)

def get_keyword_volume('filepath'):
	"""function to retrieve volume of query on brandwatch. The file should be a 1-column csv file that has a header."""
	filepath_df = pd.read_csv('filepath')
	keywords_list = filepath_df.iloc[:, 0].tolist()
	print(keywords_list)
	#gets volume of search words by looping through the german_words list



	overall_bmw_mentions = [] #queries that are correct
	needs_fixing = []         # queries that threw an error and needs to be corrected

	for words in keywords_list:

	    try:
	        bmw_mentions = queries.get_chart(name = 'DAC_BMW_DE',
	                         startDate="2017-01-01",
	                         endDate="2018-12-31", 
	                         x_axis='queries',
	                         y_axis='volume',
	                         tag = "bmw-only bmw",
	                         breakdown_by = 'queries',
	                         search = words)


	        results = bmw_mentions['results']
	        response_dict = {}

	        for i in range(len(results)):
	            bmw = results[i]['values'] #only gets the relevant data from the api call for easier reading and parsing

	            for b in bmw:
	                b['data'] = words #appends the specific search word/term in the dictionary for easier reading and parsing


	            response_dict[words] = bmw #creates a dict where search word is key and the values are data pulled from the api

	        overall_bmw_mentions.append(response_dict) #stores results from each api call loop in 1 list 
	        
	    except RuntimeError:
	        needs_fixing.append(words)



	    file_name = filepath.split("/")[-1]
	    error_list = pd.DataFrame(needs_fixing)
	    error_list.to_csv(file_name + '_errors_{:%m%d%Y}.csv'.format(file_date))
		
	    rows = []

		for bmw_mentions in overall_bmw_mentions: #overall_bmw_mentions is a list
		    for mentions in bmw_mentions.values(): #bmw_mentions is a dictionary
		        for mention_data in mentions: #mentions is a list; mention_data is a dictionary
		            row = OrderedDict()
		            row['query']= mention_data['name']
		            row['search'] = mention_data['data']
		            row['value'] = mention_data['value']
		            rows.append(row)

		df = pd.DataFrame.from_dict(rows)
		df.to_csv(file_name + '_result_{:%m%d%Y}.csv'.format(file_date))



