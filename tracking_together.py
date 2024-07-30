import pandas as pd
import os

import xlrd
pd.options.mode.chained_assignment = None

def calculate_cost(df):
    return

def process_excel(input_file, date, merchant_name):
    # 读取输入的Excel文件
    df_order = pd.read_excel(input_file)
    # cast '订单号' to string
    df_order['订单号'] = df_order['订单号'].astype(str)

    df_order.dropna(how='all', inplace=True)
    #df_order = df_order[['型号', '订单号', '姓名', '地址1', '地址2','城市', '州', '邮编']]
    df_order.loc[df_order['型号'] == 'JHH-OG06白', '型号'] = 'JHH-OG06 White'
    df_order.loc[df_order['型号'] == 'JHH-OG06灰', '型号'] = 'JHH-OG06 Grey'
    print(df_order.head())
    # read the list from file_names.txt
    df_tracking_concat = pd.DataFrame()

    if os.path.exists(f'data/{date}/Tracking/{date}_file_names.txt'):
        with open(f'data/{date}/Tracking/{date}_file_names.txt', 'r') as f:
            file_names = f.read().splitlines()

        # read each tracking file from file_names, read them as pandas dataframe, and concat them to df_order with the key 'Order ID'
        for file_name in file_names:
            # file_name exists
            print(f'Reading: data/{date}/Tracking/{file_name}')
            if not os.path.exists(f'data/{date}/Tracking/{file_name}'):
                continue

            df_tracking = pd.read_excel(f'data/{date}/Tracking/{file_name}')
            df_tracking['Order ID'] = df_tracking['Order ID'].astype(str)
            
            # Recipient	Company	Email	Tracking Number	Cost	Status	Error Message	Ship Date	Label Created Date	Estimated Delivery Time	Weight (oz)	Zone	Package Length	Package Width	Package Height	Tracking Status	Tracking Info	Tracking Date	Address Line 1	Address Line 2	City	State	Zipcode	Country	Carrier	Service	Order ID	Rubber Stamp 1
            df_tracking = df_tracking[['Order ID', 'Tracking Number', 'Cost','Recipient', 'Rubber Stamp 1', 'Address Line 1', 'Address Line 2',	'City',	'State', 'Zipcode']]
            df_tracking['承运中介'] = 'pirateship'
            df_tracking_concat = pd.concat([df_tracking_concat, df_tracking])
    if os.path.exists(f'data/{date}/Tracking/{date}_water_tracking.xls'):
        workbook = xlrd.open_workbook(f'data/{date}/Tracking/{date}_water_tracking.xls', ignore_workbook_corruption=True)
        df_water_tracking = pd.read_excel(workbook)
        df_water_tracking = df_water_tracking[['订单编号', '产品SKU', '快递单号']]
        df_water_tracking.rename(columns={'订单编号':'Order ID', '产品SKU': 'Rubber Stamp 1', '快递单号': 'Tracking Number'}, inplace=True)
        df_water_tracking['Cost'] = 3.0
        df_water_tracking.loc[df_water_tracking['Rubber Stamp 1'].isin(['HE-M001']), 'Cost'] = 2
        df_water_tracking.loc[df_water_tracking['Rubber Stamp 1'].isin(['YX2425']), 'Cost'] = 7
        df_water_tracking['Recipient'] = ''
        df_water_tracking['Address Line 1'] = ''
        df_water_tracking['Address Line 2'] = ''
        df_water_tracking['City'] = ''
        df_water_tracking['State'] = ''
        df_water_tracking['Zipcode'] = ''
        df_water_tracking['承运中介'] = '水'
        #print(df_water_tracking.head())
        df_tracking_concat = pd.concat([df_tracking_concat, df_water_tracking])
        print(df_tracking_concat.head())
    
    if df_tracking_concat.empty:
        print('No tracking file found!')
    else:
        df_order = df_order.merge(df_tracking_concat, how='left', left_on='订单号', right_on='Order ID')
    
    # for merchant_name == 'DCZ' and column '型号' in ['MY-FYY-01', 'MY-FYY-03', 'MY-FYY-03-PDD'], add 0.5 to 'Cost'
    if merchant_name == 'DCZ':
        df_order.loc[df_order['型号'].isin(['MY-FYY-01', 'MY-FYY-03', 'MY-FYY-03-PDD']), 'Cost'] += 0.5
    # save to excel
    df_order.to_excel(f'data/{date}/Tracking/{date}_{merchant_name}_订单_with_tracking.xlsx', index=False)

def main(): 
    date = '2024_07_30'

    merchant_name_list = ['DCZ', 'Crafty']

    for merchant_name in merchant_name_list:
        input_file = f'data/{date}/{date}_{merchant_name}.xlsx'
        if os.path.exists(input_file):
            process_excel(input_file, date, merchant_name)

if __name__ == "__main__":
    main()
    print("All done!")