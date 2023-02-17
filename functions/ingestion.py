import gspread
import numpy as np
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

def authorize(credential):
    scope_app = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    cred = ServiceAccountCredentials.from_json_keyfile_name(credential, scope_app)
    return gspread.authorize(cred)

def extract_from_spreadsheet(link, sheetname):
    """
    Read Spreadsheet, then return to pandas dataframe

    Parameters:
    link (str): Link to the spreadsheet to be inputted
    sheetname (str): Selected specific sheet name. For example"

    Returns:
    cleaned_data (pandas.core.frame.DataFrame): The cleaned pandas dataframe
    """
    client = authorize('sm-cred.json')
    sheet = client.open_by_url(link)
    sheet_instance = sheet.worksheet(sheetname)
    data = sheet_instance.get_all_records()
    df = pd.DataFrame.from_dict(data)
    if 'SLOC' in df.columns:
        cleaning_isi_fuel(df)
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date'], format='%d/%m/%Y')
    if 'Hour' in df.columns:
        df['Hour'] = pd.to_datetime(df['Hour'], format='%H:%M:%S')
    df.drop('Timestamp', axis=1, inplace=True)
    return df

def cleaning_isi_fuel(df):
    df['Liter'] = df['Liter'].replace('Kosong', np.nan).astype(float).astype("Int32")
    df.replace('eror', np.nan)
    anomali = ['Kosong', '0.511111111', '0.506944444', '0.522222222', '0.517361111']
    df['Hour'] = df['Hour'].where(~df['Hour'].isin(anomali), np.nan)

    if df['SLOC'].isnull().values.any():
        df["SLOC"].fillna(df["SLOC"].mode()[0], inplace=True)
    if df['Liter'].isnull().values.any():
        df["Liter"].fillna(df["Liter"].median(), inplace=True)
    if df['Hm'].isnull().values.any():
        df['Hm'] = df['Hm'].fillna(method='ffill')
    if df['Hour'].isnull().values.any():
        df.at[2592, 'Hour'] = '12:16:00'
        df.at[2593, 'Hour'] = '12:10:00'
        df.at[2594, 'Hour'] = '12:32:00'
        df.at[2595, 'Hour'] = '12:25:00'

def transform(pengisian_fuel, produksi, ritase):
    pd.options.mode.use_inf_as_na = True
    
    pengisian_fuel['Date'] = pd.to_datetime(pengisian_fuel['Date'])
    produksi['Date'] = pd.to_datetime(produksi['Date'])
    ritase['Date'] = pd.to_datetime(ritase['Date'])

    pengisian_fuel_dict = {}
    dates = pd.date_range(start='2022-01-01', end='2022-12-31', freq='D')
    df_dates = pd.DataFrame(data=dates, columns=['Date'])
    code_units = sorted(pengisian_fuel['Code Unit'].unique())
    for code_unit in code_units:
        pengisian_fuel_dict[code_unit] = pengisian_fuel[pengisian_fuel['Code Unit'] == code_unit][['Date','Liter']].groupby('Date').sum().reset_index()
        pengisian_fuel_dict[code_unit] = df_dates.join(pengisian_fuel_dict[code_unit].set_index('Date'), on='Date')
        pengisian_fuel_dict[code_unit]['Liter'].fillna(0, inplace=True)
        pengisian_fuel_dict[code_unit]['Code Unit'] = code_unit

    produksi_dict = {}
    # produksi['Ritase'] = (produksi['Production OB'] / 43)
    eq_numbs = sorted(produksi['Eq. Numb'].unique())
    for eq_numb in eq_numbs:
        produksi_dict[eq_numb] = produksi[produksi['Eq. Numb'] == eq_numb][['Date', 'Production OB']].groupby('Date').sum().reset_index()
        produksi_dict[eq_numb]['Date'] = pd.to_datetime(produksi_dict[eq_numb]['Date'])
        produksi_dict[eq_numb]['Loader'] = eq_numb

    ritase_dict = {}
    ritase['Production OB'] = ritase['Ritase'] * 43
    loaders = sorted(ritase['Loader'].unique())
    for loader in loaders:
        ritase_dict[loader] = ritase[ritase['Loader'] == loader][['Date', 'Production OB']].groupby('Date').sum().reset_index()
        ritase_dict[loader]['Date'] = pd.to_datetime(ritase_dict[loader]['Date'])
        ritase_dict[loader]['Loader'] = loader

    ritase_produksi_dict = produksi_dict | ritase_dict

    df_list = [*range(4)]
    for i, loader in enumerate(sorted(eq_numbs + loaders)):
        df_list[i] = pengisian_fuel_dict[loader].join(ritase_produksi_dict[loader].set_index('Date'), on='Date')
        df_list[i].drop(['Loader'], axis = 'columns', inplace = True)

    new_data = pd.concat(df_list, axis=0)

    new_data['Fuel Ratio'] = new_data['Liter']/new_data['Production OB']
    if new_data['Production OB'].isnull().values.any():
        new_data['Production OB'].fillna(0, inplace=True)
    new_data['Production OB'] = new_data['Production OB'].astype(int)
    new_data.rename(columns={'Liter': 'Fuel Consumption'}, inplace=True)
    new_data = new_data[['Date', 'Fuel Consumption', 'Fuel Ratio', 'Production OB', 'Code Unit']]
    new_data['Fuel Ratio'] = np.where(new_data['Production OB'] == 0, 0, new_data['Fuel Ratio'])
    new_data = new_data.sort_values(by=['Date', 'Code Unit']).reset_index(drop=True)
    # new_data[['Fuel_Consumption', 'Fuel_Ratio', 'Ritase', 'Production_OB']] = new_data[['Fuel_Consumption', 'Fuel_Ratio', 'Ritase', 'Production_OB']].astype(float).round(1)
    return new_data

def load_to_spreadsheet(df, link, sheetname):
    client = authorize('sm-cred.json')
    sheet = client.open_by_url(link)
    sheet_instance = sheet.worksheet(sheetname)
    df['Date'] = df['Date'].astype(str)
    sheet_instance.update([df.columns.values.tolist()] + df.values.tolist())