import pandas as pd  # type: ignore
import os
pd.options.mode.chained_assignment = None


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

def process_excel(df):

    # 处理签名单
    special_rows = df[(df['型号'].isin(['MY-FYY-01', 'MY-FYY-03-PDD']))]
    if not special_rows.empty:

        special_rows['承运物流'] = 'USPS'
        special_rows['快递单号'] = ''

        special_rows_sorted = special_rows.sort_values(by=['数量', '型号', '订单时间'])
    df_remain = df.copy()
    df_remain = df_remain[~df_remain.index.isin(special_rows.index)]

    # 处理其他
    def check_and_move_rows(df):
        zipcode_column_first_5_digits = df['邮编'].astype(str).str[:5]

        # read UPS_Remote_zipcode.csv
        df_UPS_Remote_zipcode = pd.read_csv('UPS_Remote_zipcode.csv')
        df_UPS_DAS_zipcode = pd.read_csv('UPS_DAS_zipcode.csv')

        # union df_UPS_Remote_zipcode and df_UPS_DAS_zipcode as df_zipcode
        df_zipcode = pd.concat([df_UPS_Remote_zipcode, df_UPS_DAS_zipcode])

        usps_rows = df[zipcode_column_first_5_digits.isin(df_zipcode['zipcode'].astype(str).str[:5].unique())]

        if not usps_rows.empty:
            # 符合条件的行放到 'usps'

            usps_rows_sorted = usps_rows.sort_values(by=['数量', '型号', '订单时间'])
            usps_rows_sorted['承运物流'] = 'USPS'
            usps_rows_sorted['快递单号'] = ''

            # 剩余数据放到 'ups'
            ups_rows_sorted = df[~df.index.isin(usps_rows.index)].copy()
            ups_rows_sorted['承运物流'] = 'UPS'
            ups_rows_sorted['快递单号'] = ''
            ups_rows_sorted = ups_rows_sorted.sort_values(by=['数量', '型号', '订单时间'])

            df_final = pd.concat([usps_rows_sorted, ups_rows_sorted])
        else:
            # 全放到 'ups'
            df_final = df.sort_values(by=['数量', '型号', '订单时间'])
            df_final['承运物流'] = 'UPS'
            df_final['快递单号'] = ''

        return df_final

    df_remain_sorted = check_and_move_rows(df_remain)

    df_output = pd.concat([special_rows_sorted, df_remain_sorted])
    with pd.ExcelWriter('订单排序.xlsx') as writer:
        df_output.to_excel(writer, sheet_name='output', index=False)


# 使用示例
input_file = 'test3_DCZ.xlsx'

df_order = pd.read_excel(input_file)
df_order.dropna(how='all', inplace=True)
df_order['订单时间'] = pd.to_datetime(df_order['订单时间'], errors='coerce')
df_order['最晚发货时间'] = pd.to_datetime(df_order['最晚发货时间'], errors='coerce')

if check_duplicated(df_order):
    print('Error: 出现重复订单号 或 相同店铺-姓名-地址1-邮编的订单，请检查！')

check_address(df_order)

process_excel(df_order)
print('Done')
