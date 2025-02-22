import pandas as pd
import os
pd.options.mode.chained_assignment = None

# 使用示例

date = '2024_07_13'

store_name = 'Crafty'
#store_name = 'DCZ'

#input_file = '20240710DCZ.xlsx'
input_file = f'{date}_{store_name}.xlsx'

def process_excel(store_name, date, input_file):
    # 读取输入的Excel文件
    df_order = pd.read_excel(f'{store_name}/{date}/{input_file}')

    df_order.dropna(how='all', inplace=True)
    df_order = df_order[['型号', '订单号', '姓名', '地址1', '地址2','城市', '州', '邮编']]

    # read the list from file_names.txt
    with open(f'{store_name}/{date}/tracking/{date}_file_names.txt', 'r') as f:
        file_names = f.read().splitlines()


    # read each tracking file from file_names, read them as pandas dataframe, and concat them to df_order with the key 'Order ID'
    df_tracking_concat = pd.DataFrame()
    
    # 添加pirateship的快递单号
    for file_name in file_names:
        # file_name exists
        print(f'Reading: {store_name}/{date}/Tracking/{file_name}')
        if not os.path.exists(f'{store_name}/{date}/Tracking/{file_name}'):
            continue

        df_tracking = pd.read_excel(f'{store_name}/{date}/Tracking/{file_name}')
        
        # Recipient	Company	Email	Tracking Number	Cost	Status	Error Message	Ship Date	Label Created Date	Estimated Delivery Time	Weight (oz)	Zone	Package Length	Package Width	Package Height	Tracking Status	Tracking Info	Tracking Date	Address Line 1	Address Line 2	City	State	Zipcode	Country	Carrier	Service	Order ID	Rubber Stamp 1
        df_tracking = df_tracking[['Order ID', 'Tracking Number', 'Cost','Recipient', 'Rubber Stamp 1', 'Address Line 1', 'Address Line 2',	'City',	'State', 'Zipcode']]

        df_tracking_concat = pd.concat([df_tracking_concat, df_tracking])

    # 添加水单的快递单号
    if os.path.exists(f'{store_name}/{date}/Tracking/{date}_water_tracking.xls'):
        df_water_tracking = pd.read_excel(f'{store_name}/{date}/Tracking/{date}_water_tracking.xls')
        df_water_tracking = df_water_tracking[['订单编号', '产品SKU', '快递单号']]
        df_water_tracking.rename(columns={'订单编号':'Order ID', '产品SKU': 'Rubber Stamp 1', '快递单号': 'Tracking Number'}, inplace=True)
        df_water_tracking['Cost'] = 3.8
        df_water_tracking['Recipient'] = ''
        df_water_tracking['Address Line 1'] = ''
        df_water_tracking['Address Line 2'] = ''
        df_water_tracking['City'] = ''
        df_water_tracking['State'] = ''
        df_water_tracking['Zipcode'] = ''
        #print(df_water_tracking.head())
        df_tracking_concat = pd.concat([df_tracking_concat, df_water_tracking])

    #print(df_tracking_concat.head())
    df_order = df_order.merge(df_tracking_concat, how='left', left_on='订单号', right_on='Order ID')
    
    # for store_name == 'DCZ' and column '型号' in ['MY-FYY-01', 'MY-FYY-03', 'MY-FYY-03-PDD'], add 0.5 to 'Cost'
    if store_name == 'DCZ':
        df_order.loc[df_order['型号'].isin(['MY-FYY-01', 'MY-FYY-03', 'MY-FYY-03-PDD']), 'Cost'] += 0.5

    # save to excel
    df_order.to_excel(f'{store_name}/{date}/Tracking/{date}_订单_with_tracking.xlsx', index=False)


process_excel(store_name, date, input_file)