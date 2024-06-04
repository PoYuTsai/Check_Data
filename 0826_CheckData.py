#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: PoYuTsai
# Reference: https://pbpython.com/excel-diff-pandas-update.html
import pandas as pd
import sys

class CheckData(object):
    def __init__(self):
        self.SourceDataPath = ''
        self.TargetDataPath = ''
        self.Version = ''
        self.Old_BundleName_all = ''
        self.New_BundleName_all = ''

    # 4G,5G資費方案
    output_columns = ['BundleName', 'ChargingService', 'Priority', 'Bucket', 'initialvalue', 'ThresholdProfile', 'Entity', 'Period']
    # Aggregate View
    output_columnsAV = ['AVName', 'ChargingServices', 'ThresholdProfileGroup']
    # Notification Template
    output_columnsNT = ['Notification Profile', 'Channel']

    # 設定基準資料集路徑
    def setSourceData(self, path):
        self.SourceDataPath = path

    # 設定目標資料集路徑
    def setTargetData(self, path):
        self.TargetDataPath = path

    # 讀取原始資料，sheet欄位名稱，打version tag
    def readSourceData(self, sheetName):
        try:
            version = pd.read_excel(self.SourceDataPath, sheetName, na_values=['NA'])
            version['version'] = "old"
            return version
        except Exception as e:
            print(f'Data access exceptions: {str(e)}')
            return None

    # 讀取目標資料，sheet欄位名稱，打version tag
    def readTargetData(self, sheetName):
        try:
            version = pd.read_excel(self.TargetDataPath, sheetName, na_values=['NA'])
            version['version'] = "new"
            return version
        except Exception as e:
            print(f'Data access exceptions: {str(e)}')
            return None

    # 比較兩個DataFrame，找出不同之處
    def compareTwoDf(self, df1, df2):
        df1 = df1.drop(['version'], axis=1)
        df2 = df2.drop(['version'], axis=1)
        df = pd.concat([df1, df2]).reset_index(drop=True)
        df_gpby = df.groupby(list(df.columns))
        idx_result = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
        df_save = df.reindex(idx_result)
        print(df_save)

    # 設定BundleName
    def set_BundleName(self, old, new):
        self.Old_BundleName_all = set(old['BundleName'])
        self.New_BundleName_all = set(new['BundleName'])
        return self.Old_BundleName_all, self.New_BundleName_all

    # 找出遺失的BundleName
    def dropped_BundleName(self):
        return self.Old_BundleName_all - self.New_BundleName_all

    # 找出新增的BundleName
    def added_BundleName(self):
        return self.New_BundleName_all - self.Old_BundleName_all

    # 獲取變更的資料
    def get_changes(self, old, new):
        all_data = pd.concat([old, new], ignore_index=True)
        Changes = all_data.drop_duplicates(subset=self.output_columns, keep='last')
        return Changes

    # 找出變更的BundleName
    def changed_BundleName(self, Changes):
        dupe_BundleName = Changes[Changes['BundleName'].duplicated()]['BundleName'].tolist()
        dupes = Changes[Changes['BundleName'].isin(dupe_BundleName)]

        change_new = dupes[dupes["version"] == "new"].drop(['version'], axis=1)
        change_old = dupes[dupes["version"] == "old"].drop(['version'], axis=1)

        change_new.set_index(change_new.columns[0], inplace=True)
        change_old.set_index(change_old.columns[0], inplace=True)

        df_all_changes = pd.concat([change_old, change_new], axis='columns', keys=['old', 'new'], join='outer')

        def report_diff(x):
            return x[0] if x[0] == x[1] else f'{x[0]} ---> {x[1]}'

        df_all_changes = df_all_changes.swaplevel(axis='columns')[change_new.columns]
        df_Changed = df_all_changes.groupby(level=0, axis=1).apply(lambda frame: frame.apply(report_diff, axis=1)).reset_index()
        return df_Changed

    # 找出被移除的BundleName
    def removed_BundleName(self, Dropped_BundleName):
        return changes[changes['BundleName'].isin(Dropped_BundleName)]

    # 找出新增的BundleName
    def increased_BundleName(self, Added_BundleName):
        return changes[changes['BundleName'].isin(Added_BundleName)]

    # 存檔至Excel
    def save_to_excel(self, writer, sheet_name, df_modified):
        try:
            if not df_modified.empty:
                df_modified.to_excel(writer, sheet_name, index=False, columns=self.output_columns)
                writer.save()
                print('資料不一致')
            else:
                df_OK = pd.DataFrame({'SPS Compare Data': ['OK!!!']})
                df_OK.to_excel(writer, sheet_name, index=False, columns=['SPS Compare Data'])
                writer.save()
                print('OK，資料完全一致')
        except KeyError:
            pass


if __name__ == '__main__':
    checkDataTask = CheckData()
    checkDataTask.setSourceData(r'C:\Users\ertsai\Desktop\data_compare\NH_test\NH_DataModel_test.xlsx')
    checkDataTask.setTargetData(r'C:\Users\ertsai\Desktop\data_compare\NH_test\NH_Aql_Data_test.xlsx')
    
    readDataResult = checkDataTask.readSourceData('NH_ALL')
    readDataResult2 = checkDataTask.readTargetData('NH_ALL')

    changes = checkDataTask.get_changes(readDataResult, readDataResult2)

    df_changed = checkDataTask.changed_BundleName(changes)
    old_BundleName_all, new_BundleName_all = checkDataTask.set_BundleName(readDataResult, readDataResult2)

    dropped_BundleName = checkDataTask.dropped_BundleName()
    df_removed = checkDataTask.removed_BundleName(dropped_BundleName)

    added_BundleName = checkDataTask.added_BundleName()
    df_added = checkDataTask.increased_BundleName(added_BundleName)

    writer = pd.ExcelWriter(r"C:\Users\ertsai\Desktop\data_compare\NH_test\data-diff.xlsx")
    checkDataTask.save_to_excel(writer, 'Abnormal', df_changed)
    checkDataTask.save_to_excel(writer, 'Less', df_removed)
    checkDataTask.save_to_excel(writer, 'More', df_added)

    sys.exit()
