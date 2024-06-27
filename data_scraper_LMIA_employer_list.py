# import libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np

# read HTML URL from Wikipedia

URL = "https://open.canada.ca/data/en/dataset/90fed587-1364-4f33-a9ee-208181dc0b97"
page = requests.get(URL)
soup = BeautifulSoup(page.content, "html.parser")

# write the page to a file for a better understanding
# This is relevant only for dev work
# with open('page.html', 'w', encoding='utf-8') as f:
#     f.write(page.text)

# read URL's from HTML
url_list = soup.find_all("span", {"property":"url"})
print("There are ", len(url_list), " links with data")

# read the language of the datasets
lang_list = soup.find_all("span", {"property":"inLanguage"})
print("There are ", len(lang_list), " languages")

# remove 1st element from language list as it does not correspond to a dataset
lang_list.pop(0)
print("Now, there are ", len(lang_list), " languages")

# onvert the language list to a string list. Otherwise we end up with NULLs
lang_list_str = [str(elem) for elem in lang_list]

# create a dataframe based on the arrays above
df_links = pd.DataFrame({'data_link_raw':url_list, 'data_language_raw':lang_list_str})
df_links.head()

# convert dataframe columns from object type to string
df_links['data_link_raw'] = df_links['data_link_raw'].astype('string')
df_links['data_language_raw'] = df_links['data_language_raw'].astype('string') 

# replace al the unnecessary html tags in the column values
df_links['data_link'] = df_links['data_link_raw'].str.replace('<span property="url">', '').str.replace('</span>','')
df_links['data_language'] = df_links['data_language_raw'].str.replace('" property="inLanguage"> </span>', '').str.replace('<span content="','')
df_links.head()

# add file_name and file_format in separate columns
df_links['file_name'] = df_links['data_link'].str.split('/').str[-1].str.split('.').str[0]
df_links['file_format'] = df_links['data_link'].str.split('.').str[-1]
df_links.head(n=20)

# check a the number of files (links) by language
df_links.groupby(['data_language']).count()

# create separate datasets for each language.
df_links_en = df_links[df_links['data_language'] == 'en'][['data_link', 'data_language','file_name','file_format']]
df_links_en.head()

df_links_fr = df_links[df_links['data_language'] == 'fr'][['data_link', 'data_language','file_name','file_format']]
df_links_fr.head()

# we'll focus on English first. Scrape and download English files
links_en_list = df_links_en['data_link'].tolist()
file_name_en_list = df_links_en['file_name'].tolist()
file_format_en_list = df_links_en['file_format'].tolist()

# create a discionary of DataFrames by reading the files from the links, based on the file format
# encoding of the files are described here: https://open.canada.ca/en/working-data-api/structured-data
all_dataframes = {}

for (name, format, link) in zip(file_name_en_list, file_format_en_list, links_en_list):
    if format == 'csv':
        print('-----Getting the ' + format + ' file ' + name + '.' + format)
        all_dataframes[name] = pd.read_csv(link, encoding = 'ISO–8859–1')
        print(u'\u2713' + ' Successfully extracted ' + name + '.' + format + '\n')
    elif format == 'xls' or format == 'xlsx':
        print('-----Getting the ' + format + ' file ' + name + '.' + format)
        all_dataframes[name] = pd.read_excel(link)
        print(u'\u2713' + ' Successfully extracted ' + name + '.' + format + '\n')

# print the dictionary keys
for key in all_dataframes.keys():
    print(key)

###############################################################################
# Files don't have consistent names and values and 
# will be processed in different batches.
###############################################################################
# Period 1: June 20 - December 31 2014 & Full 2015
# Build a function that will process the dataset.

def process_2014_2015(df_from_dict):
    # Extract the dataframe from the dictionary of DataFrames
    df_name = pd.DataFrame.from_dict(all_dataframes[df_from_dict])
    # Add proper raw names for the columns
    df_name.columns = ['Employer_raw', 'Address_raw', 'Positions_raw']
    # The csv file is nested. i.e. ha the province listed on top and the data below.
    # Move the province from the top to each corresponding rows
    df_name['Province'] = np.where(df_name['Address_raw'].isnull(), df_name['Employer_raw'], np.nan)
    # Fill the Province when it's empty
    df_name['Province'] = df_name['Province'].ffill()
    # filter out unneccessary rows [such as Notes & repeated columns names]
    df_name = df_name[((df_name['Address_raw'].notnull())
                                    & (df_name['Positions_raw'].notnull())
                                    & (df_name['Employer_raw'] != 'Employer')
                                    & (df_name['Address_raw'] != 'Address')
                                    & (df_name['Positions_raw'] != 'Positions'))
                                    | (df_name['Employer_raw'] == 'Other employers')
                                    ]
    # rename the column properly at the end by removing the 'raw' part
    df_name = df_name.rename(columns={"Employer_raw": "Employer", 
                                        "Address_raw": "Address",
                                        "Positions_raw": "Positions"})
    # add a few missing columns for future
    df_name['NOC'] = np.nan
    df_name['Stream'] = np.nan
    df_name['Incorporate_status'] = np.nan
    df_name['LMIAs'] = np.nan
    return(df_name)

employers_2014 = process_2014_2015('positive_employers_en')
employers_2015 = process_2014_2015('2015_positive_employers_en')


###############################################################################
# Period 2: 1 Jan - 31 Dec 2016
# Build a function that will process the dataset.

def process_2016(df_from_dict):
    # Extract the dataframe from the dictionary of DataFrames
    df_name = pd.DataFrame.from_dict(all_dataframes[df_from_dict])
    # Add proper raw names for the columns
    df_name.columns = ['Province_raw', 'Employer_raw', 'Address_raw', 'NOC_raw', 'Positions_raw']
    # Fill the Province when it's empty
    df_name['Province_raw'] = df_name['Province_raw'].ffill()
    # filter out unneccessary rows [such as Notes & repeated columns names]
    df_name = df_name[((~df_name['Province_raw'].str.contains('Notes:', na=False))
                                    & (~df_name['Province_raw'].str.contains('1. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('2. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('3. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('4. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('5. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('6. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('7. ', na=False))
                                    & (df_name['Employer_raw'] != 'Employer')
                                    & (df_name['Address_raw'] != 'Address')
                                    & (df_name['NOC_raw'] != 'Occupation')
                                    & (df_name['Positions_raw'] != 'Positions Approved')
                                    )]
    # rename the column properly at the end by removing the 'raw' part
    df_name = df_name.rename(columns={"Province_raw": "Province",
                                        "Employer_raw": "Employer", 
                                        "Address_raw": "Address",
                                        "NOC_raw":"NOC",
                                        "Positions_raw": "Positions"})
    # add a few missing columns for future
    df_name['Stream'] = np.nan
    df_name['Incorporate_status'] = np.nan
    df_name['LMIAs'] = np.nan
    return(df_name)

employers_2016 = process_2016('2016_positive_employer_en')


###############################################################################
# Period 3: 2017-Q1-Q2 to 2021-Q3
# Build a function that will process the dataset.

def process_2017_q1_to_2021_q3(df_from_dict):
    # Extract the dataframe from the dictionary of DataFrames
    df_name = pd.DataFrame.from_dict(all_dataframes[df_from_dict])
    # Add proper raw names for the columns
    df_name.columns = ['Province_raw', 'Stream_raw', 'Employer_raw', 'Address_raw', 'NOC_raw', 'Positions_raw']
    # Fill the Province & Stream when it's empty
    df_name['Stream_raw'] = df_name['Stream_raw'].ffill()
    df_name['Province_raw'] = df_name['Province_raw'].ffill()
    # filter out unneccessary rows [such as Notes & repeated columns names]
    df_name = df_name[((~df_name['Province_raw'].str.contains('Notes:', na=False))
                                    & (~df_name['Province_raw'].str.contains('1. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('2. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('3. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('4. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('5. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('6. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('7. ', na=False))
                                    & (df_name['Stream_raw'] != 'Stream') 
                                    & (df_name['Stream_raw'] != 'Program Stream')
                                    & (df_name['Employer_raw'] != 'Employer')
                                    & (df_name['Address_raw'] != 'Address')
                                    & (df_name['NOC_raw'] != 'Occupation')
                                    & (df_name['Positions_raw'] != 'Positions Approved') 
                                    & (df_name['Positions_raw'] != 'Approved Positions')
                                    )]
    # rename the column properly at the end by removing the 'raw' part
    df_name = df_name.rename(columns={"Province_raw": "Province",
                                                "Stream_raw":"Stream",
                                                "Employer_raw": "Employer", 
                                                "Address_raw": "Address",
                                                "NOC_raw":"NOC",
                                                "Positions_raw": "Positions"})
    # add a few missing columns for future
    df_name['Incorporate_status'] = np.nan
    df_name['LMIAs'] = np.nan
    return(df_name)

# Process the datasets by running the above function

employers_2017_q1_q2 = process_2017_q1_to_2021_q3('2017q1q2_positive_en')
employers_2017_q3 = process_2017_q1_to_2021_q3('2017q3_positive_employer_stream_en')
employers_2018_q1 = process_2017_q1_to_2021_q3('2018q1_positive_employer_en')
employers_2018_q2 = process_2017_q1_to_2021_q3('2018q2_positive_employer_en')
employers_2017_q4 = process_2017_q1_to_2021_q3('2017q4_positive_employer_en')
employers_2018_q3 = process_2017_q1_to_2021_q3('2018q3_positive_en')
employers_2018_q4 = process_2017_q1_to_2021_q3('2018q4_positive_en')
employers_2019_q1 = process_2017_q1_to_2021_q3('tfwp_2019q1_employer_positive_en')
employers_2019_q2 = process_2017_q1_to_2021_q3('tfwp_2019q2_employer_positive_en')
employers_2019_q3 = process_2017_q1_to_2021_q3('tfwp_2019q3_positive_en')
employers_2019_q4 = process_2017_q1_to_2021_q3('tfwp_2019q4_positive_en')
employers_2020_q1 = process_2017_q1_to_2021_q3('tfwp_2020q1_positive_en')
employers_2020_q2 = process_2017_q1_to_2021_q3('useb-dgcetfw-tetdip-piddiviaionline-publicationemployer-list2020-employer-list2020q22020q2csv202')
employers_2020_q3 = process_2017_q1_to_2021_q3('tfwp_2020q3_positive_en')
employers_2020_q4 = process_2017_q1_to_2021_q3('useb-dgcetfw-tetdip-piddiviaionline-publicationemployer-list2020-employer-list2020q4tfwp_2020q4')
employers_2021_q1 = process_2017_q1_to_2021_q3('useb-dgcetfw-tetdip-piddiviaionline-publicationemployer-list2021-employer-listq1-2021tfwp_2021q')
employers_2021_q2 = process_2017_q1_to_2021_q3('TFWP_2021Q2_Positive_EN')
employers_2021_q3 = process_2017_q1_to_2021_q3('TFWP_2021Q3_Positive_EN')


###############################################################################
# Period 4: 2021-Q4 - 2024Q1
# Build a function that will process the dataset.

def process_2021_q4_to_2024_q1(df_from_dict):
    # Extract the dataframe from the dictionary of DataFrames
    df_name = pd.DataFrame.from_dict(all_dataframes[df_from_dict])
    # Add proper raw names for the columns
    df_name.columns = ['Province_raw', 'Stream_raw', 'Employer_raw', 'Address_raw', 'NOC_raw', 'Incorporate_status_raw', 'LMIAs_raw','Positions_raw']
    # filter out unneccessary rows [such as Notes & repeated columns names]
    df_name = df_name[((~df_name['Province_raw'].str.contains('Notes:', na=False))
                                    & (~df_name['Province_raw'].str.contains('1. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('2. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('3. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('4. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('5. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('6. ', na=False))
                                    & (~df_name['Province_raw'].str.contains('7. ', na=False))
                                    & (df_name['Stream_raw'] != 'Program Stream')
                                    & (df_name['Employer_raw'] != 'Employer')
                                    & (df_name['Address_raw'] != 'Address')
                                    & (df_name['NOC_raw'] != 'Occupation')
                                    & (df_name['Incorporate_status_raw'] != 'Incorporate Status')
                                    & (df_name['LMIAs_raw'] != 'Approved LMIAs')
                                    & (df_name['Positions_raw'] != 'Approved Positions')
                                    )]
    # rename the column properly at the end by removin the 'raw' part
    df_name = df_name.rename(columns={"Province_raw": "Province",
                                                "Stream_raw":"Stream",
                                                "Employer_raw": "Employer", 
                                                "Address_raw": "Address",
                                                "NOC_raw":"NOC",
                                                "Incorporate_status_raw":"Incorporate_status",
                                                "LMIAs_raw":"LMIAs",
                                                "Positions_raw": "Positions"})
    return(df_name)

# Run the above function for each period [potential for automation] 

employers_2021_q4 = process_2021_q4_to_2024_q1('useb-dgcetfw-tetdip-piddiviaionline-publicationemployer-list2021-employer-list2021q4finaltfwp_2')
employers_2022_q1 = process_2021_q4_to_2024_q1('tfwp_2022q1_positive_en')
employers_2022_q2 = process_2021_q4_to_2024_q1('tfwp_2022q2_positive_en')
employers_2022_q3 = process_2021_q4_to_2024_q1('tfwp_2022q3_positive_en')
employers_2022_q4 = process_2021_q4_to_2024_q1('tfwp_2022q4_pos_en')
employers_2023_q1 = process_2021_q4_to_2024_q1('tfwp_2023q1_pos_en')
employers_2023_q2 = process_2021_q4_to_2024_q1('tfwp_2023q2_pos_en')
employers_2023_q3 = process_2021_q4_to_2024_q1('tfwp_2023q3_pos_en')
employers_2023_q4 = process_2021_q4_to_2024_q1('tfwp_2023q4_pos_en')
employers_2024_q1 = process_2021_q4_to_2024_q1('tfwp_2024q1_pos_en')

###############################################################################
# Add the Year and period columns to each dataset 
# [potential for more automation in the future]

datasets_final = [
    employers_2014, employers_2015, employers_2016,
    employers_2017_q1_q2, employers_2017_q3, employers_2017_q4,
    employers_2018_q1, employers_2018_q2, employers_2018_q3, employers_2018_q4,
    employers_2019_q1, employers_2019_q2, employers_2019_q3, employers_2019_q4,
    employers_2020_q1, employers_2020_q2, employers_2020_q3, employers_2020_q4,
    employers_2021_q1, employers_2021_q2, employers_2021_q3, employers_2021_q4, 
    employers_2022_q1, employers_2022_q2, employers_2022_q3, employers_2022_q4, 
    employers_2023_q1, employers_2023_q2, employers_2023_q3, employers_2023_q4,
    employers_2024_q1]

years_final = [
    '2014', '2015', '2016',
    '2017', '2017', '2017',
    '2018', '2018', '2018', '2018',
    '2019', '2019', '2019', '2019',
    '2020', '2020', '2020', '2020',
    '2021', '2021', '2021', '2021',
    '2022', '2022', '2022', '2022',
    '2023', '2023', '2023', '2023',
    '2024'
    ]

period_final = [
    '', '', '',
    'Q1-Q2', 'Q3', 'Q4',
    'Q1', 'Q2', 'Q3', 'Q4',
    'Q1', 'Q2', 'Q3', 'Q4',
    'Q1', 'Q2', 'Q3', 'Q4',
    'Q1', 'Q2', 'Q3', 'Q4',
    'Q1', 'Q2', 'Q3', 'Q4',
    'Q1', 'Q2', 'Q3', 'Q4',
    'Q1'
    ]

for (dataset, year, period) in zip(datasets_final, years_final, period_final):
    dataset['YEAR'] = year
    dataset['PERIOD'] = period
    
employers_2023_q2.head()
employers_2024_q1.tail()

# Merge all dataframes into one master dataframe
master_df = pd.concat(datasets_final)
master_df.describe()

# Final touch - split NOC column in 2: NOC code and NOC label
master_df['NOC_code'] = master_df['NOC'].str[:4]
master_df['NOC_label'] = master_df['NOC'].str[5:]

###############################################################################
# Export the data to a sinfle master file
master_df_name = 'Employer_list_positive_LMIA_Canada.csv'

print('Export the master file to 1 single csv file: ' + master_df_name)
master_df.to_csv(master_df_name)
print('Exported ' + master_df_name + '\n')

###############################################################################
# Split the dataframe into smaller chunks and export each chunk to csv
# Define the number of rows per each file exported:
rows_per_file = 100000
# Define the filename (it wil contain _the_file_number appended):
base_filename = 'Employer_list_positive_LMIA_Canada'

print('Splitting the master ' + master_df_name + ' file into multiple csv files of ' 
      + str(rows_per_file) + 'rows each')

# Build the split and export function
def split_and_export_csv(df, rows_per_file, base_filename):
    # Calculate the number of files needed
    num_files = (len(df) // rows_per_file) + (1 if len(df) % rows_per_file != 0 else 0)
    # Loop and save each subset
    for i in range(num_files):
        start_row = i * rows_per_file
        end_row = (i + 1) * rows_per_file
        chunk = df.iloc[start_row:end_row]
        print('Started the export of ' + f'{base_filename}_{i+1}.csv')
        chunk.to_csv(f'{base_filename}_{i+1}.csv')
        print(f'Exported {base_filename}_{i+1}.csv' + '\n')


# Run the export function
split_and_export_csv(master_df, rows_per_file, base_filename)
