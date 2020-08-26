#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Author: PoYuTsai
# Reference: https://pbpython.com/excel-diff-pandas-update.html
import pandas as pd
import sys

# 若要比對別的資料需更改:
# 1.新增欲比對的list欄位內容與更改2個function內的變數，ex: self.output_columns --> self.output_columnsAV
# 2.欲輸入/輸出的資料路徑
# 3.欲比對的資料表欄位名稱
# 4.各function裡面的primary key欄位名稱，ex: Change['BundleName'] --> Change['name']

class CheckData(object):
    SourceDataPath = ''
    TargetDataPath = ''
    Old = ''
    New = ''
    Old_BundleName_all = ''
    New_BundleName_all = ''
    Version = ''

    # 4G,5G資費方案
    output_columns = ['BundleName', 'ChargingService',
                      'Priority', 'Bucket',
                      'initialvalue', 'ThresholdProfile',
                      'Entity', 'Period']
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
            version = self.Version = pd.read_excel(self.SourceDataPath, sheetName, na_values=['NA'])
            version['version'] = "old"
            # print(version)
            return version
        except Exception as e:
            print('Data access exceptions ' + str(e.args[0]))
            return 0

    # 讀取目標資料，sheet欄位名稱，打version tag
    def readTargetData(self, sheetName):
        try:
            version = self.Version = pd.read_excel(self.TargetDataPath, sheetName, na_values=['NA'])
            version['version'] = "new"
            # print(version)
            return version
        except Exception as e:
            print('Data access exceptions ' + str(e.args[0]))
            return 0

    # 只有一個欄位要比對的話跑這個function即可
    def compareTwoDf(self, df1, df2):
        df1 = df1.drop(['version'], axis=1)
        df2 = df2.drop(['version'], axis=1)
        df = pd.concat([df1, df2])
        df = df.reset_index(drop=True)
        df_gpby = df.groupby(list(df.columns))
        # get index of unique records
        idx_result = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
        # filter
        df_save = df.reindex(idx_result)
        print(df_save)

    def set_BundleName(self, old, new):
        Old_BundleName_all = self.Old_BundleName_all = set(old['BundleName'])
        New_BundleName_all = self.New_BundleName_all = set(new['BundleName'])
        return Old_BundleName_all, New_BundleName_all

    def dropped_BundleName(self, Old_BundleName_all, New_BundleName_all):
        # 遺失: source data有的資料，但數據庫沒有
        Dropped_BundleName = Old_BundleName_all - New_BundleName_all
        return Dropped_BundleName

    def added_BundleName(self, New_BundleName_all, Old_BundleName_all):
        # 新增: 數據庫有的資料，但source data沒有
        Add_BundleName = New_BundleName_all - Old_BundleName_all
        return Add_BundleName

    def get_changes(self, old, new):
        all_data = pd.concat([old, new], ignore_index=True)
        Changes = all_data.drop_duplicates(subset=self.output_columns, keep='last')
        # print(Changes)
        return Changes

    def changed_BundleName(self, Changes):
        dupe_BundleName = Changes[Changes['BundleName'].duplicated() == True]['BundleName'].tolist()
        dupes = Changes[Changes['BundleName'].isin(dupe_BundleName)]
        # print(dupes)

        change_new = dupes[(dupes["version"] == "new")]
        change_old = dupes[(dupes["version"] == "old")]

        # Drop the temp columns - we don't need them now
        change_new = change_new.drop(['version'], axis=1)
        change_old = change_old.drop(['version'], axis=1)

        # Index on the BundleName
        change_new.set_index(change_new.columns[0], inplace=True)
        change_old.set_index(change_old.columns[0], inplace=True)

        # Combine all the changes together
        df_all_changes = pd.concat([change_old, change_new],
                                   axis='columns',
                                   keys=['old', 'new'],
                                   join='outer')

        # 查看差異
        # print(df_all_changes)

        # Define the diff function to show the changes in each field
        def report_diff(x):
            return x[0] if x[0] == x[1] else '{} ---> {}'.format(*x)

        df_all_changes = df_all_changes.swaplevel(axis='columns')[change_new.columns[0:]]
        # print(df_all_changes)

        df_Changed = df_all_changes.groupby(level=0, axis=1).apply(lambda frame: frame.apply(report_diff, axis=1))
        df_Changed = df_Changed.reset_index()
        # print(df_Changed)
        return df_Changed

    def removed_BundleName(self, Dropped_BundleName):
        # Source data有的資料，但數據庫沒有
        df_Removed = changes[changes['BundleName'].isin(Dropped_BundleName)]
        # print(df_Removed)
        return df_Removed

    def increased_BundleName(self, Added_BundleName):
        # 數據庫有的資料，但Source data沒有
        df_Added = changes[changes['BundleName'].isin(Added_BundleName)]
        # print(df_Added)
        return df_Added

    def save_to_excel(self, writer, sheet_name, df_modified):
        # 存excel
        output_columns = self.output_columns
        try:
            if not df_modified.empty:
                df_modified.to_excel(writer, sheet_name, index=False, columns=output_columns)
                writer.save()
                print('資料不一致')
            else:
                OK_data = {'SPS Compare Data': ['OK!!!']}
                df_OK = pd.DataFrame(OK_data, columns=['SPS Compare Data'])
                df_OK.to_excel(writer, sheet_name, index=False, columns=['SPS Compare Data'])
                writer.save()
                # print(df_OK)
                print('OK，資料完全一致')
        except KeyError:
            pass


# NK測試資料路徑:
# r'C:\Users\ertsai\Desktop\data_compare\NK_test\NK_DataModel_test.xlsx'
# r'C:\Users\ertsai\Desktop\data_compare\NK_test\NK_Aql_Data_test.xlsx'


if __name__ == '__main__':
    checkDataTask = CheckData()
    checkDataTask.setSourceData(r'C:\Users\ertsai\Desktop\data_compare\NH_test\NH_DataModel_test.xlsx')
    checkDataTask.setTargetData(r'C:\Users\ertsai\Desktop\data_compare\NH_test\NH_Aql_Data_test.xlsx')
    readDataResult = checkDataTask.readSourceData('NH_ALL')
    readDataResult2 = checkDataTask.readTargetData('NH_ALL')
    # print(readDataResult)
    # print(readDataResult2)

    # compare one column values
    # result = checkDataTask.compareTwoDf(readDataResult, readDataResult2)
    # print(result)

    changes = checkDataTask.get_changes(readDataResult, readDataResult2)
    # print(changes)

    df_changed = checkDataTask.changed_BundleName(changes)
    # print(df_changed)
    old_BundleName_all, new_BundleName_all = checkDataTask.set_BundleName(readDataResult, readDataResult2)

    dropped_BundleName = checkDataTask.dropped_BundleName(old_BundleName_all, new_BundleName_all)
    df_removed = checkDataTask.removed_BundleName(dropped_BundleName)
    # print(df_removed)

    added_BundleName = checkDataTask.added_BundleName(new_BundleName_all, old_BundleName_all)
    df_added = checkDataTask.increased_BundleName(added_BundleName)
    # print(df_added)

    # 輸出的檔案路徑，重跑程式要改檔名or刪掉原本的
    Writer = pd.ExcelWriter(r"C:\Users\ertsai\Desktop\data_compare\NH_test\data-diff.xlsx")
    checkDataTask.save_to_excel(Writer, 'Abnormal', df_changed)
    checkDataTask.save_to_excel(Writer, 'Less', df_removed)
    checkDataTask.save_to_excel(Writer, 'More', df_added)

    sys.exit()
