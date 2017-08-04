import pandas as pd

vets = pd.read_csv('iraqvets.csv', dtype={'PUMA': object})
vets = vets.fillna(value=0)
totalcount = vets.groupby('PUMA').agg({'MLPA': 'count'})
vetcount = vets[vets['MLPA'] == 1].groupby('PUMA').agg({'MLPA': 'count'})
average = vets.groupby('PUMA').agg({'MLPA': 'mean'})
average['MLPA'] = average['MLPA'].divide(average['MLPA'].mean())
print average.describe()
