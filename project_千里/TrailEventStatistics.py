import gspread
import pandas as pd
import numpy as np
import warnings
import string
import copy
import sys
import re
import attr

from pandas.core.common import SettingWithCopyWarning
from datetime import datetime, timedelta
from oauth2client.client import GoogleCredentials
from google.colab import auth
from google.auth import default
from IPython.display import Markdown as md
from pandas.core.indexes.base import default_index

auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)

warnings.simplefilter(action="ignore", category=SettingWithCopyWarning)

@attr.s
class YearlyTrailEvent():
  input_url: str = attr.ib()
  output_url: str = attr.ib()
  year: str = attr.ib()

  def read_sheet(input_url, year):
    sheet = gc.open_by_url(input_url).worksheet(year).get_all_values()
    df = pd.DataFrame(sheet[1:], columns=sheet[0])
    col_names = ['日期', '活動名稱', '辦理情形', '天數', '活動類型', '手作步道名稱', '縣市', '鄉鎮區',
                 '社區', '負責人', '志工人數', '需求人力', '可登錄時數', '講師', '服勤區',
                 '實習步道師以上登記區', '種子師資實習登記區\n(會依該場活動狀況確認合適的夥伴；實習資格請以通知為準)']
    if not all(df.columns.isin(col_names)):
      print('欄位名稱不正確，請修改後再重跑一次')
      return
    
    df = df[(df["辦理情形"]=="正常") & (df["活動類型"]!="場勘")]
    return df
  
  def clean_date(date_str, year):
    try:
        date_str = re.split("[-(（]", date_str)[0]
        pattern = re.compile("(^[1-9]|^1[0-2])/([1-9]$|[12][0-9]$|3[01]$)")
        match = pattern.search(date_str)
        date_str = match.group(1) + "/" + match.group(2)
        date_str = date_str.rstrip().replace("\n", "").split('-')[0]
        date = year + "/" + date_str
        date = pd.to_datetime(date, format='%Y/%m/%d').strftime('%Y/%m/%d')
        return(date)
    except AttributeError:
        ErrorMsg = f"現有日期格式無法辨認：{date_str}，請改為xx/xx"
        return(ErrorMsg)

  @classmethod
  def iterate_clean_date(cls, df, year, date_col):
    df[date_col] = df[date_col].apply(lambda date_row: cls.clean_date(date_row, year)).astype(str)
    return df
  
  def add_new_col(df, col_names):
    df[col_names] = len(col_names) * [np.nan]
    return df
  
  def rename_cols(df, rename_dict):
    """
    rename_dict 輸入規則 -> {'原欄位1':'新欄位1', '原欄位2':'新欄位2', ...}
    """
    df.rename(columns = rename_dict, inplace = True)
    return df

  def segment_col(df, col_name):
    row_name_list = df[col_name].str.replace("\s+", "", regex=True).str.split("、")
    row_name_cnt = [len(set(name)) for name in row_name_list]
    if any(df[col_name]==""):
        row_name_cnt_zero = list(np.where(df[col_name]=="")[0])
        for value, idx in enumerate(row_name_cnt_zero):
            row_name_cnt[idx] = 0
    return np.array(row_name_cnt)
  
  @classmethod
  def aggregate_col(cls, df, col_names, output_col):
    row_name_cnt_list = [cls.segment_col(df, col_name) for col_name in col_names]
    df[output_col] = sum(row_name_cnt_list)
    return df
  
  def reorder_cols(df, ordered_col_names):
    return df[ordered_col_names]
  
  def export_sheet(df, year, output_url):
    workbook = gc.open_by_url(output_url)
    if not any([ws.title == year for ws in workbook.worksheets()]):
      print(f'-----  新增頁面 {year}')
      workbook.add_worksheet(title=year, rows=df.shape[0], cols=df.shape[1])
    else:
      print(f'-----  更新已有頁面 {year}')
    output = workbook.worksheet(year).update([df.columns.values.tolist()] + df.values.tolist(), value_input_option='USER_ENTERED')

  @classmethod
  def generate_statistics(cls, demand_url, yearly_trail_url, year):
    try:
      print("step1 讀取來源資料")
      df = cls.read_sheet(demand_url, year)
      print("step2 整合日期格式")
      df = cls.iterate_clean_date(df, year, '日期')
      print('step3 統計講師助教人數')
      df = cls.aggregate_col(df, ['講師','服勤區'], '講師助教')
      # df = cls.rename_cols(df,{'步道名稱':'手作步道名稱'})
      print('step4 新增空白欄位完畢')
      df = cls.add_new_col(df, ['合作對象','類型','備註'])
      df.fillna('', inplace=True)
      print('step5 更新欄位順序')
      df = cls.reorder_cols(df, ["活動名稱", "日期", "天數", "合作對象", "類型", "志工人數", "手作步道名稱", "講師助教", "備註"])
      print('step6 輸出檔案至歷屆手作步道統計')
      cls.export_sheet(df, year, yearly_trail_url)
      print(f"歷屆手作步道活動場次整理完成，請前往以下網址查看：{yearly_trail_url}")
      # return df # test 結果用
    except Exception as err:
      print(f'發生問題 --> {err}') 
  
@attr.s
class TrailsInfo():

  def read_sheet(input_url, sheet_name):
    sheet = gc.open_by_url(input_url).worksheet(sheet_name).get_all_values()
    df = pd.DataFrame(sheet[1:], columns=sheet[0])
    return df
  
  def format_trail(df):
    try:
      df = df.astype({'場次':int, '參與人次':int})
      return df
    except:
      sys.exit('場次、參與人數等欄位資料須為數字，請檢查並調整資料後重新執行。')
  
  def format_demand(df):
    df = df[(df['辦理情形'] == '正常') & (df['活動類型'] == '手作')]
    df.rename(columns={'天數':'場次'}, inplace = True)
    df['社區'] = np.where(df['社區']=='無','',df['社區'])  
    try:
      df = df.astype({'場次':int,'志工人數':int,'可登錄時數':int})
      return df
    except:
      sys.exit('場次、志工人數、可登錄時數等欄位資料須為數字，請檢查並調整資料後重新執行。')


  def sum_trail_stat(df):
    df = df.groupby(['手作步道名稱','縣市','鄉鎮區','社區']).agg(場次=('活動名稱','count'),志工人數=('志工人數','sum')).reset_index()
    df['參與人次'] = df['場次'] * df['志工人數']
    return df.drop(columns=['志工人數'])
  
  def get_yearly_stat(df, df_yr_trail, year):
    dct = {
      '縣市': lambda col: '、'.join(set(list(col))),
      '鄉鎮區': lambda col: '、'.join(set(list(col))),
      '社區': lambda col: '、'.join(set(list(col))),
      '場次':'sum',
      '參與人次': 'sum'
    }
    df = df.groupby('手作步道名稱').agg(**{k: (k, v) for k, v in dct.items()}).reset_index()
    df['場次年份'] = year
    df = pd.concat([df_yr_trail[df_yr_trail['場次年份'] != year], df])
    df.sort_values('場次年份', ascending=False, inplace=True)
    df.fillna('',inplace=True)
    for col in ['縣市','鄉鎮區','社區']:
      df[col] = np.where(df[col].str.startswith('、'),df[col].str.slice(start=1),df[col])
    return df

  def get_overall_stat(df_yr_trail):
    dct = {
      '縣市': lambda col: '、'.join(set(list(col))),
      '鄉鎮區': lambda col: '、'.join(set(list(col))),
      '社區': lambda col: '、'.join(set(list(col))),
      '場次':'sum',
      '參與人次': 'sum'
    }
    df = df_yr_trail.groupby('手作步道名稱').agg(**{k: (k, v) for k, v in dct.items()}).reset_index()
    for col in ['縣市','鄉鎮區','社區']:
      df[col] = np.where(df[col].str.startswith('、'),df[col].str.slice(start=1),df[col])
    return df

  def export_sheet(df_stat, trail_url, sheet_name):
    workbook = gc.open_by_url(trail_url)
    if not any([ws.title == sheet_name for ws in workbook.worksheets()]):
      print(f'-----  新增頁面 {sheet_name}')
      workbook.add_worksheet(title=sheet_name, rows=df_stat.shape[0], cols=df_stat.shape[1])
    else:
      print(f'-----  更新已有頁面 {sheet_name}')
    output = workbook.worksheet(sheet_name).update([df_stat.columns.values.tolist()] + df_stat.values.tolist())

  @classmethod
  def generate_statistics(cls, demand_url, demand_year, trail_url, trail_sheet_name):
    try:
      print('step1 讀取助教人力需求表並處理格式')
      df_demand = cls.read_sheet(demand_url, demand_year)
      df_demand = cls.format_demand(df_demand)
      print('step2 讀取手作步道場次並處理格式')
      df_trail = cls.read_sheet(trail_url, trail_sheet_name)
      df_trail = cls.format_trail(df_trail)
      print('step3 讀取每年手作步道紀錄並處理格式')
      df_yr_trail = cls.read_sheet(trail_url, '每年步道場次紀錄')
      df_yr_trail = cls.format_trail(df_yr_trail)
      print('step4 計算年度場次及參與人數')
      df = cls.sum_trail_stat(df_demand)
      print('step5 整合每年步道場次紀錄')
      df = cls.get_yearly_stat(df, df_yr_trail, demand_year)
      print('step6 更新<每年步道場次紀錄>')
      cls.export_sheet(df, trail_url, sheet_name='每年步道場次紀錄')
      print('step7 合併回原有手作步道場次')
      df = cls.get_overall_stat(df)
      print('step8 更新手作步道場次')
      cls.export_sheet(df, trail_url, sheet_name=trail_sheet_name)
      print(f"{demand_year} 手作步道場次整理完成，請前往以下網址查看：{trail_url}")
    except Exception as err:
      print(f'發生問題 --> {err}') 


@attr.s
class VolunteerHour():
  
  @classmethod
  def read_sheet(cls,input_url, sheet_name):
    sheet = gc.open_by_url(input_url).worksheet(sheet_name).get_all_values()
    df = pd.DataFrame(sheet[1:], columns=sheet[0])
#    if sheet_name == '每年志工時數登錄表':
#      df['登錄時數_助教人力需求表'] =  df['登錄時數_助教人力需求表'].astype('Int64')
    if sheet_name == '步道師實習表單':
      df['志工姓名'] = df['志工姓名'].apply(cls.format_name)
      df['參與時數'] = df['參與時數'].apply(cls.format_hr)
      df.iloc[:,1:].drop_duplicates(inplace=True)
      df['服務年份'] = df['開始日期'].str.slice(stop=4)
    return df

  def format_name(name):
    name = name.replace(' ', '')
    name = ''.join(i for i in name if i not in string.punctuation)
    name = ''.join(i for i in name if not i.isdigit())
    return name

  def format_hr(hours):
    # Only remain numbers
    hours = ''.join(i for i in hours if i.isdigit())
    if hours.isdigit():
      return int(hours)
    else:
      # if no numbers in the cell then return 0 hrs
      return '0'

  def date_add_year(df, year):
    df['日期'] = year + '年 ' + df['日期']
    return df

  def string_to_list(df, col_name, sep):
    df[col_name] = df[col_name].str.split(sep)
    return df

  def check_service_time(df):
    df['可登錄時數'] = np.where(df['綽號'].str.contains(r'\['),df['綽號'].str.extract(r"\[(\w+)\]")[0],df['可登錄時數'])
    df['綽號'] = df['綽號'].str.replace(r"\(.*\)","",regex=True)
    df['綽號'] = df['綽號'].str.replace(r"\[.*\]","",regex=True)
    return df

  def map_full_name(df, df_namemap):
    df = df.merge(df_namemap, on='綽號', how='left')
    df.rename(columns={'全名':'志工姓名'}, inplace=True)
    return df[['志工姓名','日期', '活動名稱', '手作步道名稱', '可登錄時數']]

  def get_yearly_hr(df, df_yearly_hr, year):
    df['可登錄時數'] = df['可登錄時數'].astype(int)
    df = df.groupby(['志工姓名']).agg(登錄時數_助教人力需求表=('可登錄時數','sum')).reset_index()
    df['服務年份'] = year
    df['登錄時數_步道師實習表單'] = 0
    df = pd.concat([df_yearly_hr[df_yearly_hr['服務年份'] != year], df[df_yearly_hr.columns]])
    df.sort_values('服務年份', ascending=False, inplace=True)
    df.fillna('',inplace=True)
    df = df.astype({'登錄時數_助教人力需求表':'int'},errors = 'ignore')
    return df
  
  def get_overall_hr(df):
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    df = df.groupby(['志工姓名']).agg(登錄時數_助教人力需求表=('登錄時數_助教人力需求表','sum'),登錄時數_步道師實習表單=('登錄時數_步道師實習表單','sum')).reset_index()
    return df.sort_values('登錄時數_助教人力需求表', ascending=False)
    
  def get_gs_group_hr(df,col_list,df_demand_agg):
    df = df.groupby(col_list).agg(登錄時數_步道師實習表單=('參與時數','sum')).reset_index()
    df = df.merge(df_demand_agg.drop(columns=['登錄時數_步道師實習表單']),on=col_list,how='outer')
    df.sort_values('服務年份', ascending=False, inplace=True)
    df.fillna(0,inplace=True)
    return df[['志工姓名','服務年份','登錄時數_助教人力需求表','登錄時數_步道師實習表單']]

  def export_sheet(df, output_url, sheet_name):
    workbook = gc.open_by_url(output_url)
    if not any([ws.title == sheet_name for ws in workbook.worksheets()]):
      print(f'-----  新增頁面 {sheet_name}')
      workbook.add_worksheet(title=sheet_name, rows=df.shape[0], cols=df.shape[1])
    else:
      print(f'-----  更新已有頁面 {sheet_name}')
    output = workbook.worksheet(sheet_name).update([df.columns.values.tolist()] + df.values.tolist())

  @classmethod
  def get_service_hour(cls, hour_info_url, demand_url, demand_year, namemap_url):
    try:
      print('step1 讀取資料')
      df_info = cls.read_sheet(hour_info_url,'步道志工資料庫')
      print('-----  步道師實習表單')
      df_gs_hr = cls.read_sheet(hour_info_url,'步道師實習表單')
      print('-----  每年志工時數登錄表')
      df_yearly_hr = cls.read_sheet(hour_info_url,'每年志工時數登錄表')
      print('-----  志工總時數登錄表')
      df_overall_hr = cls.read_sheet(hour_info_url,'志工總時數登錄表')
      print('-----  助教人力需求表')
      df_demand = cls.read_sheet(demand_url, demand_year)
      print('-----  志工本名綽號對照表')
      df_namemap = cls.read_sheet(namemap_url, 'Sheet1')
      print('step2 日期欄位補上年份')
      df = cls.date_add_year(df_demand, demand_year)
      print('step3 服勤區提取志工名字')
      df = cls.string_to_list(df, col_name='服勤區', sep='、')
      df = df.explode('服勤區')
      df.rename(columns = {'服勤區':'綽號'}, inplace = True)
      print('step4 計算各活動登錄時數')
      df = cls.check_service_time(df)
      print('step5 轉換綽號全名')
      df = cls.map_full_name(df, df_namemap)
      df = df[~df['志工姓名'].isnull()]
      print('step6 計算人力需求表當年度總時數')
      df_agg = cls.get_yearly_hr(df, df_yearly_hr, year=demand_year)
      df_agg = cls.get_gs_group_hr(df_gs_hr,['志工姓名','服務年份'],df_agg)
      print('step7 更新<每年志工時數登錄表>')
      cls.export_sheet(df_agg, hour_info_url, sheet_name='每年志工時數登錄表')
      print('step8 計算志工總時數')
      df_agg = cls.get_overall_hr(df_agg)
      print('step9 更新<志工總時數登錄表>')
      cls.export_sheet(df_agg, hour_info_url, sheet_name='志工總時數登錄表')
      print(f"志工時數登錄整理完成，請前往以下網址查看：{hour_info_url}")
    except Exception as err:
      print(f'發生問題 --> {err}') 
