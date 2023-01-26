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
    cleaned_data.columns = ['loader', 'hour', 'date', 'ritase']
    cleaned_data[['hour', 'ritase']] = cleaned_data[['hour', 'ritase']].astype(int)
    cleaned_data['date'] = cleaned_data['date'] + pd.to_timedelta(cleaned_data['hour'], unit='h')
    cleaned_data.sort_values('date', ignore_index=True, inplace=True)
    cleaned_data.drop(columns='hour', inplace=True)
    cleaned_data.dropna(subset=['date'], inplace=True)
    return cleaned_data