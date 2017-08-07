import pandas as pd
pd.set_option('display.max_columns', None)
import shapefile
import os

def prep_data(csvfilelist, csvvariables):
    datalist = []
    for csvfile in csvfilelist:
        chunks = pd.read_csv(csvfile, chunksize=1000000, usecols=csvvariables, dtype={'PUMA10': object})
        data = pd.concat(chunk[csvvariables] for chunk in chunks)
        print 'finished narrowing ' + csvfile
        datalist.append(data)
        print 'finished appending ' + csvfile
    output = pd.concat(datalist)
    return output

def get_war_data(csvfile, csvvariable):
    data = pd.read_csv(csvfile, dtype={'PUMA10': object})
    #N/A is "no active duty", 0 is "did not serve this period" --> 0 is global "did not serve"
    data = data.fillna(value=0)
    average = data.groupby('PUMA10').agg({csvvariable: 'mean'})
    average[csvvariable] = average[csvvariable].divide(average[csvvariable].mean())
    value_dict = dict(zip(average.index, average[csvvariable]))
    return value_dict

def prep_shapefiles(value_dict, save_loc):
    fields = [('DeletionFlag', 'C', 1, 0), ['STATEFP10', 'C', 2, 0], ['PUMACE10', 'C', 5, 0], ['VET_PROPORTION', 'N', 16, 4]]
    w = shapefile.Writer(shapeType=5)
    w.fields = fields

    for i in range(67):
        i = "%02d" % (i,)
        foldername = 'tl_2016_' + i + '_puma10'
        try:
            sf = shapefile.Reader('state_shapefiles/original/' + foldername + '/' + foldername)
        except:
            continue
        count = 0
        print 'working on: ' + str(i)

        for shape in sf.iterShapeRecords():
            newrecord = shape.record[:2]
            newrecord.append(value_dict[shape.record[1]])
            w.records.append(newrecord)
            w._shapes.append(shape.shape)
    w.save('maps/' + save_loc)

if __name__ == '__main__':
    output = prep_data(['data/ss15pusa.csv', 'data/ss15pusb.csv', 'data/ss15pusc.csv', 'data/ss15pusd.csv'], ['PUMA10', 'MLPE'])
    output.to_csv('namvets.csv')
    print 'Starting value_dict'
    value_dict = get_war_data('namvets.csv', 'MLPE')
    print 'Starting shapefile construction'
    prep_shapefiles(value_dict, 'namvets/nam')
