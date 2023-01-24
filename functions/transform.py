import pandas as pd

def get_ritase(path, unit):
    """Get more organized and clean data from RITASE Raw Data

    Parameters:
    path (str): Location of RITASE Raw Data
    unit (str): Unit Number. Either ex42 or ex44

    Returns:
    cleaned_data (dataframe): The cleaned RITASE pandas dataframe
    """
    usecols = 'B:Z' if unit == 'ex42' else 'B, AA:AX'
    raw_data = pd.read_excel(path, sheet_name=list(range(1, 13)), usecols=usecols, index_col=0, skiprows=2)
    cleaned_data = pd.DataFrame(columns=['JAM', 'level_1', 0])
    for i in range(1, 13):
        cleaned_data = cleaned_data.append(raw_data.get(i).stack().reset_index(), ignore_index=True)
    cleaned_data.rename(columns={'JAM': 'date', 'level_1': 'hour', 0: f'ritase_{unit}'}, inplace=True)
    cleaned_data[f'ritase_{unit}'] = cleaned_data[f'ritase_{unit}'].astype('int')
    cleaned_data['hour'] = cleaned_data['hour'].astype('float').astype('int')
    cleaned_data['date'] = cleaned_data['date'] + pd.to_timedelta(cleaned_data['hour'], unit='h')
    cleaned_data.drop(columns='hour', inplace=True)
    if cleaned_data.isnull().sum().any():
        cleaned_data.dropna(subset=['date'], inplace=True)
    return cleaned_data