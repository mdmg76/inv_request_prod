# current version=1.00 (19-10-2021)

import pandas as pd
from pandas.core import frame
import pyinputplus as pyip
import datetime, os
from pathlib import Path

uddoh = pyip.inputInt('Please enter days on hand you need to keep: ',min=1, max=30)  # get and validate input of a user defined days on hand
two_month_past = datetime.date.today() - datetime.timedelta(days=60)    # to use for data check for 60 days or less

files_loc = Path.cwd()  # read the current working directory
files_list = list(files_loc.glob('OP Pharmacy Retail Dispense Report Q*')) # get the list of the quarterly retail dispense reports in path format
source_rowa_db = [os.path.basename(name) for name in list(files_loc.glob('*database*'))][0] # get the rowa database file name
current_stock = [os.path.basename(name) for name in list(files_loc.glob('extended*'))][0] # get the current rowa stock file name
file_name = [os.path.basename(i) for i in files_list]   # clean the names of files

source_rowa_df = pd.read_excel(source_rowa_db, engine='pyxlsb')  # read the rowa database file

current_stock_df = pd.read_csv(current_stock, sep=';')  # read the current stock csv

current_stock_df.drop(current_stock_df.columns[[2,4,5,6,7,8,9,10,11,12,14,15,16,17,19]],axis=1,inplace=True)    # delete unneeded columns by their index
current_stock_df.reset_index(level=0, inplace=True) #step to fix misaligned columns headers
current_stock_df.reset_index(level=0, inplace=True) #step to fix misaligned columns headers
current_stock_df.rename(columns={
    'index':'diff',
    'level_0':'Pack',
    'Quantity':'NDC',
    'Partial quantity':'Description',
    'Article name':'UOM',
    'Input date':'Units',
    'External code':'PHXCODE'
}, inplace=True) # rename columns to logical names
current_stock_df = current_stock_df[['PHXCODE','NDC','Description','UOM','Pack','diff','Units']]    # rearrange columns
current_stock_df.loc[current_stock_df['PHXCODE'].str.len()==9, 'PHXCODE']=current_stock_df['PHXCODE'].str[:9]   # get rid of extra letter at the end of PHX code 
current_stock_df['UOM'].replace({'cap': 'ea', 'tab': 'ea', 'tablet': 'ea'}, inplace=True)   # unify non-mL units of measure to ea
current_stock_df = current_stock_df.groupby(['PHXCODE','NDC','Description','UOM','diff','Pack'],as_index=False).sum('Units')    #consolidate duplicate lines summing units
current_stock_df['diff'] = current_stock_df['diff'].str[:1].astype(int) # clean diff columns and convert data to number
current_stock_df['net_pack'] = current_stock_df['Pack'] - current_stock_df['diff']  # get the net packs number
current_stock_df = pd.merge(current_stock_df, source_rowa_df[['NDC', 'MaxSubQty']], on='NDC')   #lookup and add packsize from database
current_stock_df.loc[current_stock_df['UOM']=='mL', 'QOH'] = current_stock_df['net_pack']   # calculate quantity on had of bottles
current_stock_df.loc[current_stock_df['UOM']!='mL', 'QOH'] = current_stock_df['net_pack'] * current_stock_df['MaxSubQty'] + current_stock_df['Units']   # calculate quantity on had of non-bottles
current_stock_df['UOM'].replace({'mL': 'Bot',}, inplace=True)   # rename units of measures from mL to bottles
current_stock_df = current_stock_df[['PHXCODE', 'Description', 'UOM', 'QOH']]   # delete unneeded columns
current_stock_df = current_stock_df.groupby(['PHXCODE'], as_index=False).sum('QOH') #consolidate duplicate lines summing QOH

all_excels = pd.DataFrame() # create an empty dataframe to merge all  excels under
for file in file_name:  # iterate over the list of retail dispense reports
    excel_file0 = pd.read_excel(file, sheet_name=None, engine='pyxlsb')  # read all the worksheets in all the excel files
    excel_file1 = pd.concat(excel_file0.values())   # combine the values of all the read sheets
    all_excels = all_excels.append(excel_file1, ignore_index=True)  # append those values to the empty dataframe created earlier

df_file = all_excels[['DISPDTTM','NDC_CODE','PHXCODE','DRUGDESCRIPTION','DISP_QTY']].copy() # remove all unneeded columns
df_file['DISPDTTM'] = pd.to_numeric(df_file['DISPDTTM'], errors='coerce')   # identify all cells in the dispense date columns with invalid data (convert to N/A)
df_file.dropna(inplace=True)       # remove all rows with N/A invalid data
df_file['DISPDTTM'] = pd.to_datetime(df_file['DISPDTTM'], unit='D', origin='1899-12-30')    # restore date format from numbers to dd-mm-yyyy hh:mm:ss
df_file['DISPDTTM'] = df_file['DISPDTTM'].dt.date   # convert datetime to date only dd-mm-yyyy
if df_file.iat[0,0] < two_month_past:   # check if data contains more than 60 days
    last_60_d_consum = df_file[df_file['DISPDTTM'] > two_month_past]    # if yes get only the last 60 days
else:   # else
    last_60_d_consum = df_file  # get all the data
last_60_d_consum = last_60_d_consum[['NDC_CODE','PHXCODE','DRUGDESCRIPTION','DISP_QTY']].copy()  # remove the date column
last_60_d_consum = last_60_d_consum.groupby(['NDC_CODE','PHXCODE','DRUGDESCRIPTION'],as_index=False).sum('DISP_QTY')    # consolidate duplicate NDC and sum
last_60_d_consum.rename({'NDC_CODE': 'NDC'}, axis=1, inplace=True)  # rename column
last_60_d_consum = pd.merge(last_60_d_consum, source_rowa_df[['NDC', 'UOM']], on='NDC') # lookup and add data from source part-1
last_60_d_consum = pd.merge(last_60_d_consum, source_rowa_df[['NDC', 'MaxSubQty']], on='NDC')   # lookup and add data from source part-1
last_60_d_consum['UOM'].replace({'cap': 'ea', 'tab': 'ea'}, inplace=True)   # rename similar UOMs into one
last_60_d_consum.loc[last_60_d_consum['UOM'] == 'mL', '60_days_consumptn'] = last_60_d_consum['DISP_QTY'] / last_60_d_consum['MaxSubQty'] # convert mL UOM to bot
last_60_d_consum.loc[last_60_d_consum['UOM'] != 'mL', '60_days_consumptn'] = last_60_d_consum['DISP_QTY'] # keep non mL UOM as is
last_60_d_consum['UOM'].replace({'mL': 'Bot',}, inplace=True)   # rename UOM after conversion
last_60_d_consum = last_60_d_consum[['PHXCODE','DRUGDESCRIPTION','UOM','60_days_consumptn']].copy()   # remove the NDC column
last_60_d_consum = last_60_d_consum.groupby(['PHXCODE','DRUGDESCRIPTION','UOM'], as_index=False).sum('60_days_consumptn') # consolidate duplicate PHX and sum
last_60_d_consum.drop(last_60_d_consum.loc[last_60_d_consum['60_days_consumptn']<=0].index, inplace=True) # remove rows with zero QTY
last_60_d_consum = pd.merge(last_60_d_consum, current_stock_df[['PHXCODE', 'QOH']], on='PHXCODE')
last_60_d_consum['DOH'] = last_60_d_consum['QOH'] / (last_60_d_consum['60_days_consumptn'] / 60)   # calculate days on hand
last_60_d_consum['DOH'] = last_60_d_consum['DOH'].astype(int)   # convert DOH data to number format
last_60_d_consum.loc[last_60_d_consum['DOH'] < uddoh, 'rem_DOH'] = uddoh - last_60_d_consum['DOH']  # evaluate remaining days on hand vs user defined needed days on hand
last_60_d_consum.loc[last_60_d_consum['DOH'] >= uddoh, 'rem_DOH'] = 0    # evaluate remaining days on hand vs user defined needed days on hand
last_60_d_consum['rem_DOH'] = last_60_d_consum['rem_DOH'].astype(int)   # convert rem_DOH data to number format
last_60_d_consum.loc[last_60_d_consum['rem_DOH']>0, 'Request_QTY'] = (last_60_d_consum['QOH'] / last_60_d_consum['DOH'] * last_60_d_consum['rem_DOH']).round().astype(int)    #calculate request quantity
last_60_d_consum.loc[last_60_d_consum['rem_DOH']==0, 'Request_QTY'] = 'No order needed' # return "No order needed" if if days on hand is more than user defined
last_60_d_consum.loc[last_60_d_consum['DOH']==0, 'Request_QTY']='Please calculate manually' # return "Please calculate manually" if zero on hand in rowa
last_60_d_consum = last_60_d_consum[['PHXCODE','DRUGDESCRIPTION','UOM','60_days_consumptn','QOH','DOH','Request_QTY']]  #rearrange columns and delete unneeded 
sheet1 = last_60_d_consum[last_60_d_consum['Request_QTY']!='No order needed']
sheet2 = last_60_d_consum[last_60_d_consum['Request_QTY']=='No order needed']
output = pd.ExcelWriter('Today OP Requisition.xlsx', engine='xlsxwriter')
sheets = {'Order Needed':sheet1, 'No Order Needed':sheet2}
for sheet, frame in sheets.items():
    frame.to_excel(output, sheet_name=sheet, index=False)
output.save()

# sheet1.to_excel('Today OP Requisition.xlsx',sheet_name='Order Needed' , index=False)
# sheet2.to_excel('Today OP Requisition.xlsx',sheet_name='No Order Needed' , index=False)
# last_60_d_consum.to_excel('Today OP Requisition.xlsx', index=False)   # write the formated consumption file to an output excel file


############### TO DO ==> split the output files or into tabs #################
