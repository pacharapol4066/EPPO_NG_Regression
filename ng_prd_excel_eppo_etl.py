import pandas as pd
from pathlib import Path
pd.set_option('display.width', 300)
rootSource = Path('source_csv_data')
rootOutput = Path('transformed_data')

# ฟังก์ชั่นทำซ้ำข้อความในคอลัมน์ที่มีการ Merge ให้เป็นค่าเดียวกัน
def iterate_col_name(header_list,startPos):
    for pos in header_list:
        if (pos.startswith('Unnamed:')) == True:
            header_list[startPos] = header_list[startPos - 1]
        startPos += 1
    return header_list

# ฟังก์ชั่นคัดลอกค่าเดือนหรือปีลงใน list เพื่อนำไปกำหนดค่าในคอลัมน์ใหม่
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

print('ng production transformation start!.')

# ให้ Pandas อ่านไฟล์ Excel
output_file = "Pandas ng production.csv"
ng_excel_file = "Table 3.1-1 Production and Import of Natural Gas.xlsx"
xl = pd.ExcelFile(rootSource / ng_excel_file)

# กำหนดช่วงของข้อมูลที่ต้องการอ่าน กำหนดสองชุด แหล่งต้นทางผลิต และ แท่นขุดเจาะ
ng_prd_df = xl.parse("tab38c", header=[4], index_col=[0])   # tab38c เป็นชื่อชีทที่ต้องการแปลงข้อมูล
ng_plant_df = ng_prd_df.iloc[0]

# ทำซ้ำข้อความแหล่งต้นทางผลิตในคอลัมน์ที่ถูก Merge ซึ่งจะเป็นค่าว่าง ให้เป็นค่าเดียวกันทั้งหมด
start_position = 0
source_ng_header_list = iterate_col_name(list(ng_prd_df.columns),start_position)

# กำหนดคอลัมน์ที่มีค่า Total เพื่อที่จะลบ ด้วยการแทนค่าด้วยคำว่า Remove
source_ng_header_list[20] = "Remove"
source_ng_header_list[14] = "Remove"
source_ng_header_list[19] = "Remove"
ng_prd_df.columns = source_ng_header_list

# ตัดแถวข้อความที่ไม่ต้องการออกจาก Dataframe
ng_prd_df = ng_prd_df.iloc[2:-10]

# ลบคอลัมน์ที่มีค่า Total ประกอบอยู่ออกจาก Dataframe
ng_prd_df.drop("Remove", axis=1, inplace=True)

# ลบแถวที่มีค่า YTD ออกจาก Dataframe
ng_prd_df.drop("      YTD", axis=0, inplace=True)

# คัดลอกชื่อแท่นขุดเจาะลง list 
ng_plant_cleanList = [x for x in ng_plant_df if str(x) != 'nan' and 'Total' not in str(x)]

# เอาชื่อแท่นขุดเจาะประกอบกับแหล่งต้นทางผลิต
column_array = []
column_array.append(ng_prd_df.columns)
column_array.append(ng_plant_cleanList)
ng_prd_df.columns = column_array        # ปรับหัวคอลัมน์ Dataframe ใหม่

# เก็บค่าเดือนและปีข้อมูลลง list
yrsArray = getElementValue(ng_prd_df.index,'Y')
monArray = getElementValue(ng_prd_df.index,'M')

# สร้างคอลัมน์ใหม่เพื่อเก็บเดือนและปีพร้อมเอาค่าเดือนและปีใส่ลงคอลัมน์นั้นๆ
ng_prd_df.insert(loc=0, column='YEAR_PRD', value=yrsArray)
ng_prd_df.set_index('YEAR_PRD', append=True, inplace=True)
ng_prd_df.insert(loc=0, column='MONTH_PRD', value=monArray)
ng_prd_df.set_index('MONTH_PRD', append=True, inplace=True)

# สร้างคอลัมน์ใหม่ เก็บข้อความ 'QTY'
ng_prd_df.insert(loc=0, column='QTY', value='QTY')
ng_prd_df.set_index('QTY', append=True, inplace=True)

# Reset index 1
ng_prd_df.index.names = ['MONTH', 'YEAR_PRD', 'MONTH_PRD','QTY']

# Reset MONTH Column from index & remove
ng_prd_df = ng_prd_df.reset_index(level=['MONTH'])
ng_prd_df.drop('MONTH', axis=1,level = 0,inplace = True)

ng_prd_df = ng_prd_df.unstack(2)    # ย้าย index 'QTY' ไปเป็น Column
ng_prd_df = ng_prd_df.stack(0)      # ย้าย แหล่งต้นทางผลิต ลงมาเป็น index
ng_prd_df = ng_prd_df.stack(0)      # ย้าย แท่นขุดเจาะ ลงมาเป็น index

# Reset index 2
ng_prd_df.index.names = ['YEAR_PRD', 'MONTH_PRD', 'SOURCE', 'PLANT']

# เขียนข้อมูลลงไฟล์ .csv
ng_prd_df.to_csv(rootOutput / output_file,header = True)

print('###### Program Completed. #######')