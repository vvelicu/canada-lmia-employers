###############################################################################
# Files don't have consistent names and values and 
# will be processed in different batches.
###############################################################################
# Period 1: June 20 - December 31 2014
# extract the dataset from dictionary
employers_2014_raw = pd.DataFrame.from_dict(all_dataframes['positive_employers_en'])
# rename the columns
employers_2014_raw.columns = ['Employer_raw', 'Address_raw', 'Positions_raw']
employers_2014_raw.head(n=10)

# The csv file is nested. i.e. ha the province listed on top and the data below.
# Move the province from the top to each corresponding rows
employers_2014_raw['Province'] = np.where(employers_2014_raw['Address_raw'].isnull(), employers_2014_raw['Employer_raw'], np.nan)
employers_2014_raw['Province'] = employers_2014_raw['Province'].ffill()
employers_2014_raw.head()

# filter out unneccesarry information (multiple column lablels, total, etc.)
employers_2014 = employers_2014_raw[((employers_2014_raw['Address_raw'].notnull())
                                    & (employers_2014_raw['Positions_raw'].notnull())
                                    & (employers_2014_raw['Employer_raw'] != 'Employer')
                                    & (employers_2014_raw['Address_raw'] != 'Address')
                                    & (employers_2014_raw['Positions_raw'] != 'Positions'))
                                    | (employers_2014_raw['Employer_raw'] == 'Other employers')]

# add the period columns
employers_2014.loc[:,'YEAR'] = '2014'
employers_2014.loc[:,'PERIOD'] = 'H2'
employers_2014.head()

# rename the column properly
employers_2014 = employers_2014.rename(columns={"Employer_raw": "Employer", 
                               "Address_raw": "Address",
                               "Positions_raw": "Positions"})
employers_2014.head()


###############################################################################
# Period 2: 1 Jan - 31 Dec 2015  [Same as 2014]
employers_2015_raw = pd.DataFrame.from_dict(all_dataframes['2015_positive_employers_en'])
employers_2015_raw.head()


###############################################################################
# Period 3: 1 Jan - 31 Dec 2016  [Different format]
employers_2016_raw = pd.DataFrame.from_dict(all_dataframes['2016_positive_employer_en'])
employers_2016_raw.head(n=30)

# Rename the columns. 
employers_2016_raw.columns = ['Province_raw', 'Employer_raw', 'Address_raw', 'NOC_2011', 'Positions_raw']
employers_2016_raw.head()

# Fill the province when it's empty
employers_2016_raw['Province_raw'] = employers_2016_raw['Province_raw'].ffill()

# filter out unneccesarry rows (multiple column labels, totals, etc.)
filter_list_2016 = ['Notes:',
                    '''1. The source for all information in this report is Employment and Social Development Canada's (ESDC) Foreign Worker System (FWS).''',
                    '2. This list excludes all personal names, such as employers of caregivers or business names that use or include personal names. For this reason, the list is not complete and does not reflect all employers who requested or received an LMIA. Should an employer wish to contact ESDC concerning the accuracy of their information, please contact NA-TFWP-PTET@hrsdc-rhdcc.gc.ca.',
                    '3. The employer name is manually entered in FWS. As such, accuracy of the names is subject to potential data entry error and inconsistent spelling.',
                    '''5. The FWS tracks TFW positions only, not TFWs who are issued a work permit or who enter Canada. Not all positions approved result in a work permit or a TFW entering Canada. For information on the number of work permits issued, please consult Immigration Refugees and Citizenship Canada's (IRCC) Facts and Figures: http://www.cic.gc.ca/english/resources/statistics/menu-fact.asp''',
                    '6. The numbers appearing in this release may differ slightly from those reported in previous releases of LMIA statistics due to data updates that occur over time.'
                    ]

employers_2016 = employers_2016_raw[((~employers_2016_raw['Province_raw'].isin(filter_list_2016))
                                    & (employers_2016_raw['Employer_raw'] != 'Employer')
                                    & (employers_2016_raw['Address_raw'] != 'Address')
                                    & (employers_2016_raw['Positions_raw'] != 'Positions Approved'))]
# one element is hard to include in the filter list. I have to use 'like' to filter it out
employers_2016 = employers_2016[~employers_2016['Province_raw'].str.contains('4. Effective November 2016, NOC 2011 is being used.')]
employers_2016.tail(n=10)
employers_2016.head()

# add the period columns
employers_2016.loc[:,'YEAR'] = '2016'
employers_2016.loc[:,'PERIOD'] = 'H1+H2'
employers_2016.head()

# rename the column properly
employers_2016 = employers_2016.rename(columns={"Province_raw": "Province",
                                                "Employer_raw": "Employer", 
                                                "Address_raw": "Address",
                                                "Positions_raw": "Positions"})
employers_2016.head()





###############################################################################
# Period 4: 2017-Q1-Q2 to 2021-Q3
employers_2017_q1q2_raw = pd.DataFrame.from_dict(all_dataframes['2017q1q2_positive_en'])
employers_2017_q1q2_raw.head()

# Rename the columns. 
employers_2017_q1q2_raw.columns = ['Province_raw', 'Stream_raw', 'Employer_raw', 'Address_raw', 'NOC_2011', 'Positions_raw']
employers_2017_q1q2_raw.head()

# Fill the Province & Stream when it's empty
employers_2017_q1q2_raw['Stream_raw'] = employers_2017_q1q2_raw['Stream_raw'].ffill()
employers_2017_q1q2_raw['Province_raw'] = employers_2017_q1q2_raw['Province_raw'].ffill()
employers_2017_q1q2_raw.tail(n=10)

# filter out unneccesarry rows (multiple column labels, totals, etc.)
employers_2017_q1q2 = employers_2017_q1q2_raw[((~employers_2017_q1q2_raw['Province_raw'].isin(filter_list_2016))
                                    & (employers_2017_q1q2_raw['Stream_raw'] != 'Stream')
                                    & (employers_2017_q1q2_raw['Employer_raw'] != 'Employer')
                                    & (employers_2017_q1q2_raw['Address_raw'] != 'Address')
                                    & (employers_2017_q1q2_raw['Positions_raw'] != 'Positions Approved'))]
# one element is hard to include in the filter list. I have to use 'like' to filter it out
employers_2017_q1q2 = employers_2017_q1q2[~employers_2017_q1q2['Province_raw'].str.contains('4. Effective November 2016, NOC 2011 is being used.')]
employers_2017_q1q2.tail(n=10)
employers_2017_q1q2.head()





# Period 5: 2021-Q4 - 2024Q1
employers_2021_q4_raw = pd.DataFrame.from_dict(all_dataframes['useb-dgcetfw-tetdip-piddiviaionline-publicationemployer-list2021-employer-list2021q4finaltfwp_2'])
employers_2021_q4_raw.head()

# Rename the columns. 
employers_2021_q4_raw.columns = ['Province_raw', 'Stream_raw', 'Employer_raw', 'Address_raw', 'NOC_raw', 'Incorporate_status_raw', 'LMIAs_raw','Positions_raw']
employers_2021_q4_raw.head()
employers_2021_q4_raw.tail(n=10)

# filter out unneccesarry rows (multiple column labels, totals, etc.)
filter_list_2021 =[
    'Notes:',
    '''1. The source for all information in this report is Employment and Social Development Canada's (ESDC) LMIA System.''',
    '2. This list excludes all personal names, such as employers of caregivers or business names that use or include personal names. For this reason, the list is not complete and does not reflect all employers who requested or received an LMIA. Should an employer wish to contact ESDC concerning the accuracy of their information, please contact NA-TFWP-PTET@hrsdc-rhdcc.gc.ca.',
    '3. The employer name is manually entered in system. As such, accuracy of the names is subject to potential data entry error and inconsistent spelling.',
    '4. Effective November 2016, NOC 2011 is being used. Prior to November 2016, all occupations under NOC 2006 were converted to the respective NOC 2011 occupation. Empirical concordance released by Statistics Canada was used to determine appropriate linkages.'
    '5. Effective February 2018, LMIAs in support of Permanent Residence (PR) are excluded from TFWP statistics reporting, unless reported separately. This may impact statistics reported over time.',
    '''6. The LMIA System tracks TFW positions only, not TFWs who are issued a work permit or who enter Canada. Not all positions approved result in a work permit or a TFW entering Canada. For information on the number of work permits issued, please consult Immigration Refugees and Citizenship Canada's (IRCC) Facts and Figures: http://www.cic.gc.ca/english/resources/statistics/menu-fact.asp''',
    '7. The numbers appearing in this release may differ slightly from those reported in previous releases of LMIA statistics due to data updates that occur over time.'
]

employers_2021_q4 = employers_2021_q4_raw[((~employers_2021_q4_raw['Province_raw'].isin(filter_list_2021))
                                    & (employers_2021_q4_raw['Stream_raw'] != 'Program Stream')
                                    & (employers_2021_q4_raw['Employer_raw'] != 'Employer')
                                    & (employers_2021_q4_raw['Address_raw'] != 'Address')
                                    & (employers_2021_q4_raw['NOC_raw'] != 'Occupation')
                                    & (employers_2021_q4_raw['Incorporate_status_raw'] != 'Incorporate Status')
                                    & (employers_2021_q4_raw['LMIAs_raw'] != 'Approved LMIAs')
                                    & (employers_2021_q4_raw['Positions_raw'] != 'Approved Positions')
                                    )]
employers_2021_q4.tail()
employers_2021_q4.head()
# one element is hard to include in the filter list. I have to use 'like' to filter it out
employers_2021_q4 = employers_2021_q4[~employers_2021_q4['Province_raw'].str.contains('4. Effective November 2016, NOC 2011 is being used.')]
employers_2021_q4 = employers_2021_q4[~employers_2021_q4['Province_raw'].str.contains('5. Effective February 2018, LMIAs in support of')]
employers_2021_q4.tail(n=10)
employers_2021_q4.head()

# rename the column properly
employers_2021_q4 = employers_2021_q4.rename(columns={"Province_raw": "Province",
                                                "Stream_raw":"Stream",
                                                "Employer_raw": "Employer", 
                                                "Address_raw": "Address",
                                                "NOC_raw":"NOC",
                                                "Incorporate_status_raw":"Incorporate_status",
                                                "LMIAs_raw":"LMIAs",
                                                "Positions_raw": "Positions"})
employers_2021_q4.head()

# add the period columns
employers_2021_q4.loc[:,'YEAR'] = '2021'
employers_2021_q4.loc[:,'PERIOD'] = 'Q4'
employers_2021_q4.head()

employers_2021_q4.head()
