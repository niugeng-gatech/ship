import pandas as pd
import os
pd.options.mode.chained_assignment = None
# show all columns
#pd.set_option('display.max_columns', None)

# 使用示例



def read_excel(merchant_name, date, input_file):
    # 读取输入的Excel文件
    print(f'读取 data/{date}/{input_file}...')
    df = pd.read_excel(f'data/{date}/{input_file}')
    df.dropna(how='all', inplace=True)

    # cast '数量' as int
    df['数量'] = df['数量'].astype(int)

    # cast '订单时间' as datetime
    df['订单时间'] = pd.to_datetime(df['订单时间'], errors='coerce')

    # if '型号' == JHH-OG06白, then '型号' = JHH-OG06 White; if '型号' == JHH-OG06灰, then '型号' = JHH-OG06 Grey; otherwise '型号' = '型号'
    df.loc[df['型号'] == 'JHH-OG06白', '型号'] = 'JHH-OG06 White'
    df.loc[df['型号'] == 'JHH-OG06灰', '型号'] = 'JHH-OG06 Grey'
    #print(df.head())
    return df

def check_duplicated(df):
    duplicate_flag = False
    # check if there are any rows that have the same '订单号' or same concatenation of '姓名' and '邮编'
    duplicated_order_num = df[df.duplicated(subset='订单号', keep=False)]
    df_no_dup_order_num = df.drop_duplicates(subset='订单号', keep=False)
    duplicated_name_zipcode = df_no_dup_order_num[df_no_dup_order_num.duplicated(subset=['店铺', '姓名', '地址1',  '邮编'], keep=False)]
    # if there are duplicated '订单号' or duplicated concatenation of '姓名' and '邮编', then return True and print the duplicated rows
    if not duplicated_order_num.empty:
        print('重复的订单号:')
        print(duplicated_order_num)
        print()
        duplicate_flag = True

    if not duplicated_name_zipcode.empty:
        print('重复的店铺-姓名-地址1-邮编:')
        print(duplicated_name_zipcode)
        print()
        duplicate_flag = True

    return duplicate_flag

def check_address(df):
    # if the row with '地址1' doesn't have any space in the value, then print that row
    address_no_space = df[df['地址1'].str.contains(' ')==False]
    if not address_no_space.empty:
        print('地址1中没有空格:')
        print(address_no_space[['名称','型号','店铺', '订单号','姓名', '地址1', '邮编']])
        print()
    
    df = df[~df.index.isin(address_no_space.index)]
    # add a new column '地址1_clean' to store the '地址1' with removing dot and - in the value
    df['地址1_clean'] = df['地址1'].str.replace('[.-]', '', regex=True)

    # add a new column '地址1_POBOX' to store the '地址1' with removing space in the value
    df['地址1_POBOX'] = df['地址1_clean'].str.replace(' ', '', regex=True)
    df = df[~df.index.isin(df[df['地址1_POBOX'].str.contains('POBOX', case=False)].index)]

    # if the row with '地址1' doesn't begin with a number in the value, then print that row
    address_no_number = df[df['地址1_clean'].str[0].str.isnumeric()==False]
    if not address_no_number.empty:
        print('地址1中不是以数字开头:')
        print(address_no_number[['名称','型号','店铺', '订单号','姓名', '地址1', '邮编']])
        print()

    df = df[~df.index.isin(address_no_number.index)]

    # if the row with '地址1' begin with a number in the value but don't have space to seperate the numbers and text, then print that row
    address_no_space_after_number = df[df['地址1_clean'].str.contains(r'^\d+ ', regex=True)==False]
    if not address_no_space_after_number.empty:
        print('地址1中数字后没有空格:')
        print(address_no_space_after_number[['名称','型号','店铺', '订单号','姓名', '地址1', '邮编']])
        print()

    # if the row with '地址1' contians '城市' in the value t, then print that row
    address_city = df[df['地址1_clean'].str.contains('城市', case=False)]
    if not address_city.empty:
        print('地址1中包含城市:')
        print(address_city[['名称','型号','店铺', '订单号','姓名', '地址1', '邮编']])
        print()

def process_carrier(df_order, Upload_flag, date):
    
    df_sku_weight = pd.read_csv('sku_weight.csv')
    df_sku_weight['weight'] = df_sku_weight['weight'].astype(float)
    #print(df_sku_weight.head())

    # 分支1：处理水单
    df_order_water = df_order[df_order['merchant_name'] == 'Crafty'].copy()
    special_rows = pd.DataFrame()
    special_rows_sorted = pd.DataFrame()
    if not df_order_water.empty:
        # if df_order_water has the column '中介'
        if '承运中介' in df_order_water.columns:
            # 水单的特殊行
            special_rows = df_order_water[((df_order_water['州'].isin(['FL', 'AK', 'PR', 'HI'])) & (df_order_water['型号'].isin(['XMYQSB', 'QSB-01', 'YS-10', 'YS-06'])))
                                             | (df_order_water['型号'].isin(['HE-M001', 'JHH-OG06白', 'JHH-OG06灰'])) 
                                             | (df_order_water['承运中介'].isin(['水']))]
        else:
            special_rows = df_order_water[((df_order_water['州'].isin(['FL', 'AK', 'PR', 'HI'])) & (df_order_water['型号'].isin(['XMYQSB', 'QSB-01', 'YS-10', 'YS-06']))) 
                                             | (df_order_water['型号'].isin(['HE-M001', 'JHH-OG06白', 'JHH-OG06灰']))]


    if not special_rows.empty:
        special_rows['承运中介'] = '水'
        special_rows['承运物流'] = 'USPS'
        special_rows['快递单号'] = ''

        # 按'数量','型号'列整理相同数量,型号的行，再按'订单时间'排序
        special_rows_sorted = special_rows.sort_values(by=['数量', '型号', '订单时间'])

        df_water = special_rows_sorted.copy()
        # rename columns consuming a dictionary: 姓名: Name, 地址1: Address, 地址2: Address2, 城市: City, 州: Abbreviation, 邮编: ZIP/Postal code, 订单号: order num, 电话: phone num1, 数量: Quantity, 型号: Item-sku, 订单时间: OrderTime, 承运中介: Carrier, 承运物流: Shipping, 快递单号: Tracking
        df_water.rename(columns={'姓名': 'Name', '地址1': 'Address', '地址2': 'Address2', '城市': 'City', '州': 'Abbreviation', '邮编': 'ZIP/Postal code', '订单号': 'order num', '数量': 'Quantity', '型号': 'Item-sku', '订单时间': 'OrderTime', '承运中介': 'Carrier', '承运物流': 'Shipping', '快递单号': 'Tracking'}, inplace=True)
        # only select the columns we need
        #df_water['weight'] = ''
        df_water['phone num1'] = ''
        df_water['Item-sku2'] = ''
        df_water['sku'] = df_water['Item-sku']
        
        # if 'Quantity' == 2 then 'Item-sku' = 'Item-sku' + 'x2'
        df_water.loc[df_water['Quantity'] >= 2, 'Item-sku'] = df_water['Item-sku'] + ' x' + df_water['Quantity'].astype(str)

        # join df_water and df_sku_weight on 'Item-sku'
        df_water = df_water.merge(df_sku_weight, how='left', left_on='sku', right_on='sku')
        df_water['weight'] = df_water['weight'] * df_water['Quantity']
        #print(df_water.head())

        df_water = df_water[['Name', 'Address', 'Address2', 'City','ZIP/Postal code',  'Abbreviation', 'weight', 'phone num1', 'order num', 'Item-sku', 'Item-sku2']]
        #print(df_water.head())
        if Upload_flag:
            with pd.ExcelWriter(f'data/{date}/Upload/usps_order_{date}.xlsx') as writer:
                df_water.to_excel(writer, sheet_name='output', index=False)
    
    # df_remain is the rows after removing special_rows from df
    df_remain = df_order.copy()
    df_remain = df_remain[~df_remain.index.isin(special_rows.index)]
    #print(df_remain.head())

    # 分支2：处理正规单
    def check_and_move_rows(df):
        zipcode_column_first_5_digits = df['邮编'].astype(str).str[:5]

        # read UPS_Remote_zipcode.csv as string
        df_UPS_Remote_zipcode = pd.read_csv('UPS_Remote_zipcode.csv', dtype={'zipcode': str})
        df_UPS_DAS_zipcode = pd.read_csv('UPS_DAS_zipcode.csv', dtype={'zipcode': str})

        # union df_UPS_Remote_zipcode and df_UPS_DAS_zipcode as df_zipcode
        df_zipcode = pd.concat([df_UPS_Remote_zipcode, df_UPS_DAS_zipcode])
        # print(df_zipcode.head())

        usps_rows = df[(zipcode_column_first_5_digits.isin(df_zipcode['zipcode'].astype(str).str[:5].unique())) | (df['型号'].isin(['MY-FYY-01', 'MY-FYY-03', 'MY-FYY-03-PDD']))]
        if not usps_rows.empty:
            # 符合条件的行放到 'usps'
            usps_rows_sorted = usps_rows.sort_values(by=['数量', '型号', '订单时间'])
            usps_rows_sorted['承运中介'] = 'pirateship'
            usps_rows_sorted['承运物流'] = 'USPS'
            usps_rows_sorted['快递单号'] = ''

            # 剩余数据放到 'ups'
            ups_rows_sorted = df[~df.index.isin(usps_rows.index)].copy()
            ups_rows_sorted['承运中介'] = 'pirateship'
            ups_rows_sorted['承运物流'] = 'ups'
            ups_rows_sorted['快递单号'] = ''
            ups_rows_sorted = ups_rows_sorted.sort_values(by=['数量', '型号', '订单时间'])

            df_final = pd.concat([usps_rows_sorted, ups_rows_sorted])
        else:
            # 全放到 'ups'
            df_final = df.sort_values(by=['数量', '型号', '订单时间'])

        return df_final

    if not df_remain.empty:
        df_remain_sorted = check_and_move_rows(df_remain)

        df_pirateship = df_remain_sorted.copy()
        # rename columns consuming a dictionary: 姓名: Name, 地址1: Address, 地址2: Address2, 城市: City, 州: Abbreviation, 邮编: ZIP/Postal code, 订单号: order num, 电话: phone num1, 数量: Quantity, 型号: Item-sku, 订单时间: OrderTime, 承运中介: Carrier, 承运物流: Shipping, 快递单号: Tracking
        df_pirateship.rename(columns={'姓名': 'Name', '地址1': 'Address', '地址2': 'Address Line 2', '城市': 'City', '州': 'State', '邮编': 'Zipcode', '订单号': 'Order ID', '数量': 'Quantity', '型号': 'Order Items', '订单时间': 'OrderTime', '承运中介': 'Carrier', '承运物流': 'Shipping', '快递单号': 'Tracking'}, inplace=True)
        df_pirateship['Country'] = 'US'
        df_pirateship['Company'] = ''
        df_pirateship['Email'] = ''
        df_pirateship['Phone'] = ''
        
        # if 'Quantity' >= 2 then 'Item-sku' = 'Item-sku' + 'x' + 'Quantity'
        df_pirateship = df_pirateship.merge(df_sku_weight, how='left', left_on='Order Items', right_on='sku')
        df_pirateship['Pounds'] = df_pirateship['weight'] * df_pirateship['Quantity']
        df_pirateship.loc[df_pirateship['Quantity'] >= 2, 'Order Items'] = df_pirateship['Order Items'] + ' x' + df_pirateship['Quantity'].astype(str)
        df_pirateship.loc[df_pirateship['Quantity'] >= 2, 'Length'] = None
        df_pirateship.loc[df_pirateship['Quantity'] >= 2, 'Width'] = None
        df_pirateship.loc[df_pirateship['Quantity'] >= 2, 'Height'] = None
        
        # if '店铺' == 'Da Cheng Zi', then '店铺_renamed' = 'DCZ', otherwise '店铺_renamed' = '店铺'
        df_pirateship['店铺_renamed'] = df_pirateship['店铺']
        df_pirateship.loc[df_pirateship['店铺'].str.upper().isin(['DA CHENG ZI','DACHENGZI']), '店铺_renamed'] = 'DCZ'
        df_pirateship.loc[df_pirateship['店铺'] == 'MTEHFYAF', '店铺_renamed'] = 'MTEH'
        df_pirateship['Rubberstamp1'] = df_pirateship['店铺_renamed'].str.upper()+' ' + df_pirateship['Order Items']

        df_pirateship = df_pirateship[['Email', 'Name', 'Address', 'Address Line 2', 'City','State', 'Zipcode', 'Country', 'Order ID', 'Rubberstamp1', 'Order Items', 'Pounds', 'Length', 'Width', 'Height', 'Shipping', 'merchant_name']]

        if Upload_flag:
            # create a list to save the file names
            file_names = []
            # save df_pirateship seperately to xls based on each pair of 'Order Items' and '承运物流'
            for shipping_method, df_group in df_pirateship.groupby('Shipping'):
                for order_items, df_order_items in df_group.groupby('Order Items'):
                    df_order_items.to_excel(f'data/{date}/Upload/{date}_{order_items}_{shipping_method}.xlsx', index=False)
                    file_names.append(f'{date}_{order_items}_{shipping_method} - Tracking Numbers.xlsx')

            # save the file names to a txt file for append tracking number in the future
            with open(f'data/{date}/Tracking/{date}_file_names.txt', 'w') as f:
                for file_name in file_names:
                    f.write(file_name + '\n')

    else:
        df_remain_sorted = pd.DataFrame()

    
    #水单和正规单合并
    if not special_rows_sorted.empty:
        df_output= pd.concat([special_rows_sorted, df_remain_sorted])
    else:
        df_output = df_remain_sorted
    #print(df_output.head())
    # save the df_output seperately to xlsx based on 'merchant_name'
    for merchant_name, df_group in df_output.groupby('merchant_name'):
        with pd.ExcelWriter(f'data/{date}/{date}_{merchant_name}_订单排序.xlsx') as writer:
            df_group.to_excel(writer, sheet_name='output', index=False)
            print(f'{merchant_name} 单品统计:')
            print(df_group.groupby('型号')['数量'].sum())
            print()

    # print the total count for each '型号', summing up the '数量'
    print('总单品统计:')
    print(df_output.groupby('型号')['数量'].sum())
    print()

    # print the row count and sum of '数量' for each group of '型号' and '数量'
    print('总打单统计:')
    print(df_output.groupby(['型号', '数量']).size())
    print()

    # print the sum of '数量' for each '承运中介' when '承运物流' = 'USPS'
    print('USPS统计:')
    print(df_output[df_output['承运物流'] == 'USPS'].groupby('承运中介')[['数量']].sum())

def main(): 
    date = '2024_07_17'

    merchant_name_list = ['DCZ', 'Crafty']
    Upload_flag = True

    # if folder_path 'Crafty' not exist, create it 
    if not os.path.exists(f'data/{date}'):
        print(f'Error: data/{date} does not exist!')
        os.makedirs(f'data/{date}')
        exit()

    df_order = pd.DataFrame()
    for merchant_name in merchant_name_list:
        if os.path.exists(f'data/{date}/{date}_{merchant_name}.xlsx'):
            print(f'found {date}_{merchant_name}.xlsx')
            df_temp = read_excel(merchant_name, date, f'{date}_{merchant_name}.xlsx')
            df_temp['merchant_name'] = merchant_name
            # concatenate df_temp to df_order and reset the index
            df_order = pd.concat([df_order, df_temp], ignore_index=True)
            
    if df_order.empty:
        print(f'Error: data/{date}/{date}_Crafty.xlsx and data/{date}/{date}_DCZ.xlsx do not exist!')
        exit()

    if Upload_flag and not os.path.exists(f'data/{date}/Upload/'):
        os.makedirs(f'data/{date}/Upload/')

    if Upload_flag and not os.path.exists(f'data/{date}/Tracking/'):
        os.makedirs(f'data/{date}/Tracking/')

    if check_duplicated(df_order):
        print('Error: 出现重复订单号 或 相同店铺-姓名-地址1-邮编的订单，请检查！')
    
    check_address(df_order)

    process_carrier(df_order, Upload_flag, date)
  
if __name__=="__main__": 
    main() 

