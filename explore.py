import pandas as pd

def warstatistics(csvfile, csvvariable):

    vets = pd.read_csv(csvfile, dtype={'PUMA10': object})
    vets = vets.fillna(value=0)
    totalcount = vets.groupby('PUMA10').agg({csvvariable: 'count'})
    vetcount = vets[vets[csvvariable] == 1].groupby('PUMA10').agg({csvvariable: 'count'})
    average = vets.groupby('PUMA10').agg({csvvariable: 'mean'})
    average[csvvariable] = average[csvvariable].divide(average[csvvariable].mean())
    print average.describe()

warstatistics('5yearnamvets.csv', 'MLPE')
