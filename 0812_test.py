#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pandas as pd

DataPath1 = r'C:\Users\ertsai\Desktop\data_compare\NH_test\NH_DataModel_test.xlsx'
DataPath2 = r'C:\Users\ertsai\Desktop\data_compare\NH_test\NH_Aql_Data.xlsx'

# 資料欄位:BundleName, ChargingService, Priority, Bucket, initialvalue, ThresholdProfile, Entity, Period
# Read in the two files but call the data old and new and create columns to track
# pd.read_excel('檔案路徑', '資料表sheet名稱', 缺值補NA)
old = pd.read_excel(DataPath1, 'NH_ALL', na_values=['NA'])
new = pd.read_excel(DataPath2, 'NH_ALL', na_values=['NA'])
old['version'] = "old"
new['version'] = "new"
# old
# new

old_BundleName_all = set(old['BundleName'])
new_BundleName_all = set(new['BundleName'])

# 遺失: source data有的資料，但數據庫沒有
dropped_BundleName = old_BundleName_all - new_BundleName_all
# 新增: 數據庫有的資料，但source data沒有
added_BundleName = new_BundleName_all - old_BundleName_all

all_data = pd.concat([old, new], ignore_index=True)
changes = all_data.drop_duplicates(subset=["BundleName", "ChargingService",
                                           "Priority", "Bucket",
                                           "initialvalue", "ThresholdProfile",
                                           "Entity", "Period"], keep='last')
# print(changes)


dupe_BundleName = changes[changes['BundleName'].duplicated() == True]['BundleName'].tolist()
dupes = changes[changes["BundleName"].isin(dupe_BundleName)]
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


# print(df_all_changes)


# Define the diff function to show the changes in each field
def report_diff(x):
    return x[0] if x[0] == x[1] else '{} ---> {}'.format(*x)


df_all_changes = df_all_changes.swaplevel(axis='columns')[change_new.columns[0:]]
# print(df_all_changes)

df_changed = df_all_changes.groupby(level=0, axis=1).apply(lambda frame: frame.apply(report_diff, axis=1))
df_changed = df_changed.reset_index()
# print(df_changed)


# Source data有的資料，但數據庫沒有
df_removed = changes[changes["BundleName"].isin(dropped_BundleName)]
# print(df_removed)

# 數據庫有的資料，但Source data沒有
df_added = changes[changes["BundleName"].isin(added_BundleName)]
# print(df_added)

# 存excel
output_columns = ["BundleName", "ChargingService",
                  "Priority", "Bucket",
                  "initialvalue", "ThresholdProfile",
                  "Entity", "Period"]

writer = pd.ExcelWriter(r"C:\Users\ertsai\Desktop\data_compare\NH_test\my-diff.xlsx")
try:
    if not df_changed.empty:
        df_changed.to_excel(writer, "changed", index=False, columns=output_columns)
except KeyError:
    pass

try:
    if not df_removed.empty:
        df_removed.to_excel(writer, "removed", index=False, columns=output_columns)
except KeyError:
    pass

try:
    if not df_added.empty:
        df_added.to_excel(writer, "added", index=False, columns=output_columns)
except KeyError:
    pass

writer.save()
