import pandas as pd
import matplotlib.pyplot as plt

def timeseries_viz(data, unit, date_range):
    """Get timeseries from RITASE DataFrame

    Parameters:
    data (dataframe): Variable of RITASE Dataframe
    unit (str): Unit Number. Either ex42 or ex44
    date_range (str): Date Range that will be grouped by. Either day or month

    Returns:
    RITASE Timeseries (visualization): The visualizaation of RITASE in day or month
    """
    data = data.reset_index().groupby(pd.Grouper(key='date', axis=0, freq=date_range)).sum()
    fig, ax = plt.subplots(figsize=(18, 8))
    plt.title(f'Ritase {unit.upper()} Timeseries', fontsize=20, y=1)
    plt.xlabel('Dates', fontsize=14)
    plt.ylabel(f'Ritase {unit.upper()}', fontsize=14)
    ax.fill_between(data.index, data[f'ritase_{unit}'], color='#96b9d0', linewidth=1.1, alpha=0.1)
    ax.plot(data.index, data[f'ritase_{unit}'], color='#fc8d62', linewidth=1.1)