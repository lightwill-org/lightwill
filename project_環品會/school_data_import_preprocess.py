## Module Import
import gspread
import pandas as pd
import numpy as np
import string
import copy
import re
import geopandas as gpd
from datetime import datetime, timedelta
# The original code was executed on google colab, so some code may have to be adjusted if running on different enviroments.
from oauth2client.client import GoogleCredentials
from google.colab import auth
from google.auth import default

auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)


## Data Import
偏遠地區學校名錄URL = 'https://docs.google.com/spreadsheets/d/1THjDUUYbp-ArNX7hMKMg0rkZfioBIwN91awhS1WpUnw/edit#gid=1281776040'
原住民地區國小名錄URL = 'https://docs.google.com/spreadsheets/d/1K9dDN35C3DzP5PxOpoyxiYGU8ke85L4jPuZ2YJOZpOY/edit#gid=1049981665'
各級學校分布位置_國小URL = 'https://docs.google.com/spreadsheets/d/11nuCT8Bz_9fuVOP38SCUmpKKURfKOEEvylY4bYsm-VE/edit#gid=578135434'

測試資料URL = 'https://docs.google.com/spreadsheets/d/1yucFW1GHIJu7Tt97I30A_WxPqaMoIrw6lh61JTP765A/edit#gid=501147240' #測試資料

# Import worksheet
偏遠地區學校名錄 = gc.open_by_url(偏遠地區學校名錄URL)
偏遠地區學校名錄 = 偏遠地區學校名錄.worksheet("國民中小學(本校)")
原住民地區國小名錄 = gc.open_by_url(原住民地區國小名錄URL)
原住民地區國小名錄 = 原住民地區國小名錄.worksheet("109名錄")
各級學校分布位置_國小 = gc.open_by_url(各級學校分布位置_國小URL)
各級學校分布位置_國小 = 各級學校分布位置_國小.worksheet("109名錄")

測試資料 = gc.open_by_url(測試資料URL)
實際資料 = 測試資料.worksheet("實際舉辦場次") 
報名資料 = 測試資料.worksheet("場次報名資料") 

# Transform to dataframe
偏遠地區學校名錄 = 偏遠地區學校名錄.get_all_values()
偏遠地區學校名錄 = pd.DataFrame(偏遠地區學校名錄[4:], columns=偏遠地區學校名錄[3])
原住民地區國小名錄 = 原住民地區國小名錄.get_all_values()
原住民地區國小名錄 = pd.DataFrame(原住民地區國小名錄[3:], columns=原住民地區國小名錄[2])
各級學校分布位置_國小 = 各級學校分布位置_國小.get_all_values()
各級學校分布位置_國小 = pd.DataFrame(各級學校分布位置_國小[1:], columns=各級學校分布位置_國小[0])
實際資料 = 實際資料.get_all_values()
實際資料 = pd.DataFrame(實際資料[1:], columns=實際資料[0])
報名資料 = 報名資料.get_all_values()
報名資料 = pd.DataFrame(報名資料[1:], columns=報名資料[0])

## Data Join and cleaning
實際資料["偏鄉類型"] = pd.merge(實際資料, 偏遠地區學校名錄, how="left", on="學校代碼")["地區屬性"]
報名資料["偏鄉類型"] = pd.merge(報名資料, 偏遠地區學校名錄, how="left", on="學校代碼")["地區屬性"]
實際資料.loc[實際資料["學校代碼"].isin(原住民地區國小名錄["學校代碼"]), "原住民學校"] = 'Y'
實際資料["原住民學校"] = np.where(實際資料["原住民學校"] == 'Y', 'Y', 'N')
報名資料.loc[報名資料["學校代碼"].isin(原住民地區國小名錄["學校代碼"]), "原住民學校"] = 'Y'
報名資料["原住民學校"] = np.where(報名資料["原住民學校"] == 'Y', 'Y', 'N')

實際資料 = pd.merge(實際資料, 各級學校分布位置_國小[["代碼", "X 坐標", "Y 坐標"]], 
                how="left", left_on="學校代碼", right_on="代碼").drop("代碼", axis=1)
報名資料 = pd.merge(報名資料, 各級學校分布位置_國小[["代碼", "X 坐標", "Y 坐標"]], 
                how="left", left_on="學校代碼", right_on="代碼").drop("代碼", axis=1)
# Transform to gpd
gdf_實際資料 = gpd.GeoDataFrame(實際資料, geometry=gpd.points_from_xy(實際資料["X 坐標"], 實際資料["Y 坐標"]))
gdf_報名資料 = gpd.GeoDataFrame(報名資料, geometry=gpd.points_from_xy(報名資料["X 坐標"], 報名資料["Y 坐標"]))
# Transform TWD97(epsg=3826) to WGS84(epsg=4326)
gdf_實際資料.crs = {'init' :'epsg:3826'}
gdf_實際資料 = gdf_實際資料.to_crs(epsg=4326)
gdf_報名資料.crs = {'init' :'epsg:3826'}
gdf_報名資料 = gdf_報名資料.to_crs(epsg=4326)

# Seperate geometry column to latitude/longitude column
def point_to_x(row):
  if row.is_empty:
    return np.nan
  else:
    return row.x

def point_to_y(row):
  if row.is_empty:
    return np.nan
  else:
    return row.y

gdf_實際資料["X 坐標"] = gdf_實際資料["geometry"].apply(point_to_x)
gdf_實際資料["Y 坐標"] = gdf_實際資料["geometry"].apply(point_to_y)

gdf_報名資料["X 坐標"] = gdf_報名資料["geometry"].apply(point_to_x)
gdf_報名資料["Y 坐標"] = gdf_報名資料["geometry"].apply(point_to_y)

# Transform back to pandas dataframe
實際資料 = pd.DataFrame(gdf_實際資料)
報名資料 = pd.DataFrame(gdf_報名資料)
# Drop geometry column
實際資料.drop("geometry", axis=1, inplace=True)
報名資料.drop("geometry", axis=1, inplace=True)


## Data Storage
實際資料 = 實際資料.fillna("NA")
報名資料 = 報名資料.fillna("NA")

## Data Output
# 重新確認新資料放置位置
儲存位置URL = 'https://docs.google.com/spreadsheets/d/13CF09APbFZJS8s6IUzRORhbOEFgq7IKnEBFS9UIFJNA/edit#gid=564538404'
儲存位置 = gc.open_by_url(儲存位置URL)
# 將整理好的資料匯入Google表單
新增頁籤名稱 = "實際舉辦場次(From Colab)"

# Creat a new tab if it is not exist already
if not any([i.title == 新增頁籤名稱 for i in 儲存位置.worksheets()]):
    儲存位置.add_worksheet(title=新增頁籤名稱, rows=實際資料.shape[0], cols=實際資料.shape[1])

# 注意匯入之DataFrame不能存在空值
儲存位置.worksheet(新增頁籤名稱).update([實際資料.columns.values.tolist()] + 實際資料.values.tolist())
# 將整理好的資料匯入Google表單
新增頁籤名稱 = "場次報名資料(From Colab)"

# Creat a new tab if it is not exist already
if not any([i.title == 新增頁籤名稱 for i in 儲存位置.worksheets()]):
    儲存位置.add_worksheet(title=新增頁籤名稱, rows=報名資料.shape[0], cols=報名資料.shape[1])

# 注意匯入之DataFrame不能存在空值
儲存位置.worksheet(新增頁籤名稱).update([報名資料.columns.values.tolist()] + 報名資料.values.tolist())


