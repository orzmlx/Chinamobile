import csv
from tkinter.filedialog import askopenfilenames
from tkinter import Tk
import re
import os
import datetime

print(f'NRcellCu文件必须要有，剩余可添加NRSectorcarrier/FieldReplaceableUnit_productData/UeMC/McpcPCellProfileUeCfg相关数据')

tk = Tk()
tk.withdraw()
data_dict = {}
file_names = askopenfilenames(filetypes=(('CSV File', '*.csv'),))
row_one = ['SubNetwork', 'MeContext', 'city', 'cellName', 'nCI', 'mcpcPCellProfileRef_McpcPCellProfile']
progress_1 = 0
progress_2 = 0
for file in file_names:
    if 'NRCELLCU_20' in file.upper():
        print(f'正在处理数据:{file}')
        progress_1 = 1
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        list_NRCellCU = ['SubNetwork', 'MeContext', 'city', 'cellName', 'nCI', 'mcpcPCellProfileRef']
        list_NRCellCU_index = [top_row.index(j) for j in list_NRCellCU]
        for i in file_read:
            f = re.findall(r'McpcPCellProfile=([^,]+)', i[list_NRCellCU_index[-1]])
            if f:
                f = f[0]
            else:
                f = ''
            tmp_list = [i[j] for j in list_NRCellCU_index[:-1]]

            tmp_list.append(f)  # index_6
            key = f'{i[list_NRCellCU_index[3]]}'
            data_dict[key] = tmp_list

NRSectorCarrier_dict = {}
for file in file_names:
    if 'NRSECTORCARRIER_20' in file.upper() and progress_1 > 0:
        print(f'正在处理数据:{file}')
        progress_2 = 1
        # print(file)
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        index_nRSectorCarrierId = top_row.index('nRSectorCarrierId')
        index_MeContext = top_row.index('MeContext')
        index_reservedBy = top_row.index('reservedBy')
        index_arfcnDL = top_row.index('arfcnDL')
        index_configuredMaxTxPower = top_row.index('configuredMaxTxPower')
        index_sectorEquipmentFunctionRef = top_row.index('sectorEquipmentFunctionRef')
        for i in file_read:
            if i[index_nRSectorCarrierId] == '1':
                SectorCarrierId = re.findall(r'NRCellDU=([^,]+)', i[index_reservedBy])
                SectorCarrierId = SectorCarrierId[0]
            else:
                SectorCarrierId = i[index_nRSectorCarrierId]
            SectorEquipmentFunction = \
                re.findall(r'SectorEquipmentFunction=([^,]+)', i[index_sectorEquipmentFunctionRef])[0]
            NRSectorCarrier_dict[SectorCarrierId] = [i[index_arfcnDL], i[index_configuredMaxTxPower],
                                                     SectorEquipmentFunction]
        row_one += ['NRSectorCarrier_arfcnDL', 'NRSectorCarrier_configuredMaxTxPower',
                    'NRSectorCarrier_sectorEquipmentFunctionRef_SectorEquipmentFunction']
        for k, v in data_dict.items():
            if k in NRSectorCarrier_dict.keys():
                data_dict[k] = v + NRSectorCarrier_dict[v[3]]
            else:
                data_dict[k] = v + ['', '']
        # print(data_dict[k] )

Baseband_dict = {}
rru_dict = {}
for file in file_names:
    if 'FIELDREPLACEABLEUNIT_PRODUCTDATA' in file.upper() and progress_2 > 0:
        print(f'正在处理数据:{file}')
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        index_MO = top_row.index('MO')
        index_MeContext = top_row.index('MeContext')
        index_productName = top_row.index('productName')
        index_serialNumber = top_row.index('serialNumber')

        for i in file_read:
            if i[index_productName][:8] == 'Baseband':
                Baseband_dict[i[index_MeContext]] = [i[index_productName], i[index_serialNumber]]
            if i[index_productName][:3] in ['AIR', 'IRU', 'Rad']:
                rru = re.findall(r'FieldReplaceableUnit=([^,]+)', i[index_MO])[0]
                rru_dict[f'{i[index_MeContext]}&{rru}'] = [i[index_productName], i[index_serialNumber], rru]

        row_one += ['FieldReplaceableUnit_productData_BBUproductName',
                    'FieldReplaceableUnit_productData_BBUserialNumber']
        row_one += ['FieldReplaceableUnit_productData_RRUproductName',
                    'FieldReplaceableUnit_productData_RRUserialNumber',
                    'FieldReplaceableUnit_productData_FieldReplaceableUnit']
        for k, v in data_dict.items():
            if v[1] in Baseband_dict.keys():
                data_dict[k] = v + Baseband_dict[v[1]]
            else:
                data_dict[k] = v + ['', '']

        for k, v in data_dict.items():
            # print(v)
            if f'{v[1]}&{v[8]}' in rru_dict.keys():
                data_dict[k] = v + rru_dict[f'{v[1]}&{v[8]}']
            else:
                data_dict[k] = v + ['', '', '']

uemc_dict = {}
for file in file_names:
    if 'UEMC_20' in file.upper() and progress_1 > 0:
        print(f'正在处理数据:{file}')
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        index_mo = top_row.index('MO')
        index_MeContext = top_row.index('MeContext')
        index_freqSelAtBlindRwrToEutran = top_row.index('freqSelAtBlindRwrToEutran')
        for i in file_read:
            f = re.findall(r'UeMC=([^,]+)', i[index_mo])
            if f:
                uemc_dict[i[index_MeContext]] = f + [i[index_freqSelAtBlindRwrToEutran]]
        row_one += ['UeMC', 'freqSelAtBlindRwrToEutran']
        for k, v in data_dict.items():
            if f"{v[1]}" in uemc_dict.keys():
                data_dict[k] = v + uemc_dict[f"{v[1]}"]
            else:
                data_dict[k] = v + ['', '']

rsrpCandidateB2_dict = {}
for file in file_names:
    if 'MCPCPCELLPROFILEUECFG_RSRPCANDIDATEB2' in file.upper() and progress_1 > 0:
        print(f'正在处理数据:{file}')
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        index_mo = top_row.index('MO')
        index_MeContext = top_row.index('MeContext')
        index_threshold1 = top_row.index('threshold1')
        index_threshold_7 = top_row.index('hysteresis')
        index_threshold_8 = top_row.index('timeToTrigger')
        index_threshold2EUtra = top_row.index('threshold2EUtra')
        for i in file_read:
            if 'McpcPCellProfileUeCfg=Base' in i[index_mo]:
                f = re.findall(r'McpcPCellProfile=([^,]+),McpcPCellProfileUeCfg=([^,]+)', i[index_mo])
                if f:
                    f = list(f[0])
                    # print(f)
                    rsrpCandidateB2_dict[f'{i[index_MeContext]}&{f[0]}'] = f + [i[index_threshold1],
                                                                                i[index_threshold2EUtra],
                                                                                i[index_threshold_7],
                                                                                i[index_threshold_8]]
        row_one += ['McpcPCellProfileUeCfg_Base_rsrpCandidateB2_McpcPCellProfile',
                    'McpcPCellProfileUeCfg_Base_rsrpCandidateB2_McpcPCellProfileUeCfg',
                    'McpcPCellProfileUeCfg_Base_rsrpCandidateB2_threshold1',
                    'McpcPCellProfileUeCfg_Base_rsrpCandidateB2_threshold2EUtra',
                    'McpcPCellProfileUeCfg_Base_rsrpCandidateB2_hysteresis',
                    'McpcPCellProfileUeCfg_Base_rsrpCandidateB2_timeToTrigger']
        for k, v in data_dict.items():
            if v[5] != '':
                if f"{v[1]}&{v[5]}" in rsrpCandidateB2_dict.keys():
                    data_dict[k] = v + rsrpCandidateB2_dict[f"{v[1]}&{v[5]}"]
                else:
                    data_dict[k] = v + ['', '', '', '']

rsrpCandidateB2_5qi1_dict = {}
for file in file_names:
    if 'MCPCPCELLPROFILEUECFG_RSRPCANDIDATEB2' in file.upper() and progress_1 > 0:
        print(f'正在处理数据:{file}')
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        index_mo = top_row.index('MO')
        index_MeContext = top_row.index('MeContext')
        index_threshold1 = top_row.index('threshold1')
        index_threshold_7 = top_row.index('hysteresis')
        index_threshold_8 = top_row.index('timeToTrigger')
        index_threshold2EUtra = top_row.index('threshold2EUtra')
        for i in file_read:
            if 'McpcPCellProfileUeCfg=5QI1' in i[index_mo]:
                f = re.findall(r'McpcPCellProfile=([^,]+),McpcPCellProfileUeCfg=([^,]+)', i[index_mo])
                if f:
                    f = list(f[0])
                    # print(f)
                    rsrpCandidateB2_5qi1_dict[f'{i[index_MeContext]}&{f[0]}'] = f + [i[index_threshold1],
                                                                                     i[index_threshold2EUtra],
                                                                                     i[index_threshold_7],
                                                                                     i[index_threshold_8]]
        row_one += ['McpcPCellProfileUeCfg_5qi1_rsrpCandidateB2_McpcPCellProfile',
                    'McpcPCellProfileUeCfg_5qi1_rsrpCandidateB2_McpcPCellProfileUeCfg',
                    'McpcPCellProfileUeCfg_5qi1_rsrpCandidateB2_threshold1',
                    'McpcPCellProfileUeCfg_5qi1_rsrpCandidateB2_threshold2EUtra'
            , 'McpcPCellProfileUeCfg_5qi1_rsrpCandidateB2_hysteresis',
                    'McpcPCellProfileUeCfg_5qi1_rsrpCandidateB2_timeToTrigger']
        for k, v in data_dict.items():
            if v[5] != '':
                if f"{v[1]}&{v[5]}" in rsrpCandidateB2_5qi1_dict.keys():
                    data_dict[k] = v + rsrpCandidateB2_5qi1_dict[f"{v[1]}&{v[5]}"]
                else:
                    data_dict[k] = v + ['', '', '', '']

rsrpCritical_dict = {}
for file in file_names:
    if 'MCPCPCELLPROFILEUECFG_RSRPCRITICAL' in file.upper() and progress_1 > 0:
        print(f'正在处理数据:{file}')
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        index_mo = top_row.index('MO')
        index_MeContext = top_row.index('MeContext')
        index_threshold = top_row.index('threshold')
        for i in file_read:
            if 'McpcPCellProfileUeCfg=Base' in i[index_mo]:
                f = re.findall(r'McpcPCellProfile=([^,]+),McpcPCellProfileUeCfg=([^,]+)', i[index_mo])
                if f:
                    f = list(f[0])
                    # print(f)
                    rsrpCritical_dict[f'{i[index_MeContext]}&{f[0]}'] = f + [i[index_threshold]]
        row_one += ['McpcPCellProfileUeCfg_rsrpCritical_McpcPCellProfile',
                    'McpcPCellProfileUeCfg_rsrpCritical_McpcPCellProfileUeCfg',
                    'McpcPCellProfileUeCfg_rsrpCritical_threshold']
        for k, v in data_dict.items():
            if v[5] != '':
                if f"{v[1]}&{v[5]}" in rsrpCritical_dict.keys():
                    data_dict[k] = v + rsrpCritical_dict[f"{v[1]}&{v[5]}"]
                else:
                    data_dict[k] = v + ['', '', '']

rsrpSearchZone_dict = {}
for file in file_names:
    if 'MCPCPCELLPROFILEUECFG_RSRPSEARCHZONE' in file.upper() and progress_1 > 0:
        print(f'正在处理数据:{file}')
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        index_mo = top_row.index('MO')
        index_MeContext = top_row.index('MeContext')
        index_threshold = top_row.index('threshold')
        index_threshold_3 = top_row.index('hysteresis')
        index_threshold_4 = top_row.index('timeToTrigger')
        for i in file_read:
            if 'McpcPCellProfileUeCfg=Base' in i[index_mo]:
                f = re.findall(r'McpcPCellProfile=([^,]+),McpcPCellProfileUeCfg=([^,]+)', i[index_mo])
                if f:
                    f = list(f[0])
                    # print(f)
                    rsrpSearchZone_dict[f'{i[index_MeContext]}&{f[0]}'] = f + [i[index_threshold], i[index_threshold_3],
                                                                               i[index_threshold_4]]
        row_one += ['McpcPCellProfileUeCfg_rsrpSearchZone_McpcPCellProfile',
                    'McpcPCellProfileUeCfg_rsrpSearchZone_McpcPCellProfileUeCfg',
                    'McpcPCellProfileUeCfg_rsrpSearchZone_threshold', 'McpcPCellProfileUeCfg_rsrpSearchZone_hysteresis',
                    'McpcPCellProfileUeCfg_rsrpSearchZone_timeToTrigger']
        for k, v in data_dict.items():
            if v[5] != '':
                if f"{v[1]}&{v[5]}" in rsrpSearchZone_dict.keys():
                    data_dict[k] = v + rsrpSearchZone_dict[f"{v[1]}&{v[5]}"]
                else:
                    data_dict[k] = v + ['', '', '']

rsrpCandidateA5_dict = {}
for file in file_names:
    if 'MCPCPCELLPROFILEUECFG_RSRPCANDIDATEA5' in file.upper() and progress_1 > 0:
        print(f'正在处理数据:{file}')
        file_open = open(file, 'r')
        file_read = csv.reader(file_open)
        top_row = next(file_read)
        index_mo = top_row.index('MO')
        index_MeContext = top_row.index('MeContext')
        index_threshold_1 = top_row.index('threshold1')
        index_threshold_2 = top_row.index('threshold2')
        index_threshold_5 = top_row.index('hysteresis')
        index_threshold_6 = top_row.index('timeToTrigger')
        for i in file_read:
            if 'McpcPCellProfileUeCfg=Base' in i[index_mo]:
                f = re.findall(r'McpcPCellProfile=([^,]+),McpcPCellProfileUeCfg=([^,]+)', i[index_mo])
                if f:
                    f = list(f[0])
                    # print(f)
                    rsrpCandidateA5_dict[f'{i[index_MeContext]}&{f[0]}'] = f + [i[index_threshold_1],
                                                                                i[index_threshold_2],
                                                                                i[index_threshold_5],
                                                                                i[index_threshold_6]]
        row_one += ['McpcPCellProfileUeCfg_rsrpCandidateA5_McpcPCellProfile',
                    'McpcPCellProfileUeCfg_rsrpCandidateA5_McpcPCellProfileUeCfg',
                    'McpcPCellProfileUeCfg_rsrpCandidateA5_threshold1',
                    'McpcPCellProfileUeCfg_rsrpCandidateA5_threshold2',
                    'McpcPCellProfileUeCfg_rsrpCandidateA5_hysteresis',
                    'McpcPCellProfileUeCfg_rsrpCandidateA5_timeToTrigger']
        for k, v in data_dict.items():
            if v[5] != '':
                if f"{v[1]}&{v[5]}" in rsrpCandidateA5_dict.keys():
                    data_dict[k] = v + rsrpCandidateA5_dict[f"{v[1]}&{v[5]}"]
                else:
                    data_dict[k] = v + ['', '', '', '']

# for k,v in data_dict.items():
# 	print(k,v)
# print(row_one)


file_path, name = os.path.split(file_names[0])
time_now = datetime.datetime.now().strftime('%y%m%d_%H%M')
# print(time_now)
save_file = f'mcpc数据汇聚_{time_now}.csv'
# print(file_path, save_file)
save_file = os.path.join(file_path, save_file)
print('\n结果保存在:')
print(save_file)
save = open(save_file, 'w', newline='')
csv_save = csv.writer(save)
csv_save.writerow(row_one)
csv_save.writerows(data_dict.values())

save.close()
