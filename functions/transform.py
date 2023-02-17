import pandas as pd

def get_ritase(path):
    """Get more organized and clean data from RITASE Raw Data

    Parameters:
    path (str): Location of RITASE Raw Data

    Returns:
    cleaned_data (dataframe): The cleaned RITASE pandas dataframe
    """
    raw_data = pd.read_excel(path, sheet_name=list(range(1, 13)), index_col=0, header=None, skiprows=1)
    cleaned_data = pd.DataFrame(columns=['LOADER', 'JAM', 1, 0])
    for i in range(1, 13):
        temp_data = pd.DataFrame(raw_data.get(i).T[1:].values, columns=raw_data.get(i).T.iloc[0])
        temp_data = temp_data.fillna(method="ffill").set_index(['LOADER', 'JAM']).stack().reset_index()
        cleaned_data = pd.concat([cleaned_data, temp_data], ignore_index=True)
    cleaned_data.columns = ['Loader', 'Hour', 'Date', 'Ritase']
    cleaned_data[['Hour', 'Ritase']] = cleaned_data[['Hour', 'Ritase']].astype(int)
    cleaned_data.sort_values(['Date','Hour'], ignore_index=True, inplace=True)
    cleaned_data.dropna(subset=['Date'], inplace=True)
    return cleaned_data

def get_fuel_unit(path):
    data = pd.ExcelFile(path)

    # Membuat dataframe baru untuk data bersih
    temp = pd.DataFrame(columns=['CodeUnit', 'EGI', 'SLOC', 'Jam', 'Liter', 'Hm', 'Date', 'Shift'])

    for x in range(12):
      tabnames = data.sheet_names
      df = data.parse(tabnames[x], skiprows=3)
      idx = 0
      unit = ['EX41', 'EX42', 'EX43', 'EX44']
      for d in df.columns:
        if df[d][1] in unit:
          break
        else:
          idx = idx + 1
      df.drop(df.iloc[:, 0:idx], inplace=True, axis=1)

      df2 = df.copy()
      df2.rename(columns={df2.columns[0]: 'CodeUnit'}, inplace=True)
      df2.rename(columns={df2.columns[1]: 'EGI'}, inplace=True)

      df2 = df2[df2.columns.drop(list(df2.filter(regex='Unnamed:')))]
      df2 = df2[df2.columns.drop(list(df2.filter(regex='(Hm/Km)')))]
      df2 = df2.drop(df2.index[0])
      df2 = df2.iloc[:8]
      df2 = df2.reset_index()
      df2 = df2.drop(['index'], axis=1)

      temp_date = data.parse(tabnames[x], skiprows=0)
      temp_date = temp_date.iloc[1:2, :len(df2.columns)].reset_index().drop(['index'], axis=1)
      temp_date = temp_date.apply(pd.to_datetime, errors='coerce').dropna(axis=1, how='all')

      df3 = df2.copy()

      for i in range(32):
        for k in range(2):
          shift = 'Day' if k == 0 else 'Night'
          for j in range(8):
            try:
              new_row = {'CodeUnit': df3.loc[j].values[0], 
                        'EGI': df3.loc[j].values[1], 
                        'SLOC': df3.loc[j].values[2], 
                        'Jam': df3.loc[j].values[3],
                        'Liter': df3.loc[j].values[4],
                        'Hm': df3.loc[j].values[5],
                        'Date': temp_date.loc[0].values[i],
                        'Shift': shift}
              temp = temp.append(new_row, ignore_index=True)
            except Exception:
              break
          df3 = df3.drop(df3.iloc[:, 2:6], axis = 1)

    temp = temp.dropna(how='any',axis=0)
    temp = temp.reset_index()
    temp = temp.drop(['index'], axis=1)

    for c in range(len(temp)):
      stemp = type(temp['Jam'][c]) == pd._libs.tslibs.timestamps.Timestamp
      if stemp:
          temp['Jam'][c] = temp['Jam'][c].strftime('%H:%M:%S')
    return temp