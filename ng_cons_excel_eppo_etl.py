import pandas as pd
from pathlib import Path
pd.set_option('display.width', 300)
rootSource = Path('source_csv_data')
rootOutput = Path('transformed_data')

def iterate_col_name(header_list,startPos):
    for pos in header_list:
        if (pos.startswith('Unnamed:')) == True:
            header_list[startPos] = header_list[startPos - 1]
        startPos += 1
    return header_list

# Get YEAR & MONTH column value
def getElementValue(elementVal,choice):
    bufferVal = []
    lastStr = ''
    if choice == 'Y':
        for pos in elementVal:
            if str(pos).isdigit():
                bufferVal.append(pos)
                lastStr = pos
            else:
                bufferVal.append(lastStr)
    elif choice == 'M':
        for pos in elementVal:
            if not str(pos).isdigit():
                bufferVal.append(str(pos).strip())
                lastStr = pos
            else:
                bufferVal.append(lastStr)
    return bufferVal

print('ng consumption transformation start!.')

output_file = 'Pandas ng consumption.csv'
ng_excel_file = "Table 3.2-2 Consumption of Natural Gas by sector.xlsx"
xl = pd.ExcelFile(rootSource / ng_excel_file)

ng_cons_lev1_df = xl.parse("T3.2-2M", header=[3], index_col=[0])
ng_cons_lev2_df = xl.parse("T3.2-2M", header=[5], index_col=[0])
ng_cons_lev3_df = xl.parse("T3.2-2M", header=[6], index_col=[0])

segtor_list = ng_cons_lev2_df.iloc[0]
subSegtor_list = ng_cons_lev3_df.iloc[0]

sector_ng_header_list = ['header1','header2','header3','header4','header5','header6','header7','header8']

start_position = 0
subSector1_ng_header_list = iterate_col_name(list(ng_cons_lev2_df.columns),start_position)
subSector2_ng_header_list = iterate_col_name(list(ng_cons_lev3_df.columns),start_position)

sector_ng_header_list[3] = "Remove"
sector_ng_header_list[7] = "Remove"
subSector1_ng_header_list[3] = "Remove"
subSector1_ng_header_list[7] = "Remove"
subSector2_ng_header_list[3] = "Remove"
subSector2_ng_header_list[7] = "Remove"
ng_cons_lev1_df.columns = sector_ng_header_list
ng_cons_lev2_df.columns = subSector1_ng_header_list
ng_cons_lev3_df.columns = subSector2_ng_header_list

ng_cons_lev1_df = ng_cons_lev1_df.iloc[1:-2]
ng_cons_lev2_df = ng_cons_lev2_df.iloc[1:-2]
ng_cons_lev3_df = ng_cons_lev3_df.iloc[0:-2]

ng_cons_lev1_df.drop("Remove", axis=1, inplace=True)
ng_cons_lev2_df.drop("Remove", axis=1, inplace=True)
ng_cons_lev3_df.drop("Remove", axis=1, inplace=True)
ng_cons_lev1_df.drop("   YTD              ", axis=0, inplace=True)
ng_cons_lev2_df.drop("   YTD              ", axis=0, inplace=True)
ng_cons_lev3_df.drop("   YTD              ", axis=0, inplace=True)
ng_cons_lev1_df.drop("MONTH", axis=0, inplace=True)

ng_segLev2_cleanList = [x for x in ng_cons_lev3_df if str(x) != 'nan' and 'Total' not in str(x)]
ng_segLevAdd_cleanList = [x for x in ng_cons_lev2_df.columns if str(x) != 'nan' and 'Electricity' not in str(x)]
ng_segLev2_cleanList.extend(ng_segLevAdd_cleanList)

yrsArray = getElementValue(ng_cons_lev1_df.index,'Y')
monArray = getElementValue(ng_cons_lev1_df.index,'M')

ng_cons_lev1_df.insert(loc=0, column='YEAR_PRD', value=yrsArray)
ng_cons_lev1_df.set_index('YEAR_PRD', append=True, inplace=True)
ng_cons_lev1_df.insert(loc=0, column='MONTH_PRD', value=monArray)
ng_cons_lev1_df.set_index('MONTH_PRD', append=True, inplace=True)

ng_cons_lev1_df = ng_cons_lev1_df.iloc[1:,0:]

ng_cons_lev1_df.set_index('header1', append=True, inplace=True)
ng_cons_lev1_df.index.names = ['MONTH', 'YEAR_PRD', 'MONTH_PRD','header1']
ng_cons_lev1_df.drop(level='header1',index='Electricity', axis=0, inplace=True)
ng_cons_lev1_df = ng_cons_lev1_df.reset_index(level=['header1'])

column_array = []
column_array.append(ng_cons_lev2_df.columns)
column_array.append(ng_segLev2_cleanList)
ng_cons_lev1_df.columns = column_array

ng_cons_lev1_df.insert(loc=0, column='QTY', value='QTY')
ng_cons_lev1_df.set_index('QTY', append=True, inplace=True)

ng_cons_lev1_df.index.names = ['MONTH', 'YEAR_PRD', 'MONTH_PRD','QTY']

ng_cons_lev1_df = ng_cons_lev1_df.reset_index(level=['MONTH'])
ng_cons_lev1_df.drop('MONTH', axis=1,level = 0,inplace = True)

ng_cons_lev1_df = ng_cons_lev1_df.stack(0)
ng_cons_lev1_df = ng_cons_lev1_df.stack(0)
ng_cons_lev1_df = ng_cons_lev1_df.unstack(2)
ng_cons_lev1_df.index.names = ['YEAR_PRD', 'MONTH_PRD','SECTOR','SUBSECTOR']
ng_cons_lev1_df.to_csv(rootOutput / output_file,header = True)

print('###### Program Completed. #######')