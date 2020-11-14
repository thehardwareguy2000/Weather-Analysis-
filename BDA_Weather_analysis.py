from google.colab import drive
drive.mount('/content/drive')
from pyspark import SparkContext
from google.colab import drive
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as sqlf

# Create a Spark Context
sc = SparkContext.getOrCreate()

# Create a sql context
sqlc = SQLContext(sc)

# Analysis on the latest 19 years
years = range(2015, 2020)

# Yearly stats
for year in years:
    # get data as raw text
    txtfile = sc.textFile('/content/drive/My Drive/data/%s.csv' % year)
    # split attribute values using commas
    data = txtfile.map(lambda x: x.split(','))
    # create table
    table = data.map(lambda r: Row(station=r[0], date=r[1], ele=r[2], val=int(r[3]), m_flag=r[4], q_flag=r[5], s_flag=r[6], obs_time=r[7]))
    # create dataframe
    df = sqlc.createDataFrame(table)

    # Handle abnomalities and missing data
    clean_df = df.filter(df['q_flag'] == '')

    print("\nYear %s Stats:\n" % year)
    # 1. Average min
    res = clean_df.filter(clean_df['ele'] == 'TMIN').groupby().avg('val').first()
    print('Avg. Min Temp = %.2f degrees Celsius' % (res['avg(val)'] / 10.0))

    # 1. Average max
    res = clean_df.filter(clean_df['ele'] == 'TMAX').groupby().avg('val').first()
    print('Avg. Max Temp = %.2f degrees Celsius' % (res['avg(val)'] / 10.0))

    # 2. Max TMAX
    res = clean_df.filter(clean_df['ele'] == 'TMAX').groupby().max('val').first()
    print('Max TMAX value = %.2f degrees Celsius' % (res['max(val)'] / 10.0))

    # 2. Min TMIN
    res = clean_df.filter(clean_df['ele'] == 'TMIN').groupby().min('val').first()
    print('Min TMIN value = %.2f degrees Celsius' % (res['min(val)'] / 10.0))

    # 3. Five distinct hottest weather stations
    res = clean_df.filter(clean_df['ele'] == 'TMAX').sort(sqlf.desc('val')).groupBy(clean_df['station']).agg(sqlf.max('val')).sort(sqlf.desc('max(val)')).limit(5).collect()
    print("Top 5 distinct hottest stations")
    for i in res:
        print('Station:%s\tTemperature:%.2f degrees Celsius' % (i.station, float(i['max(val)']) / 10.0))

    # 3. Five hottest weather stations only by temperature
    res = clean_df.filter(clean_df['ele'] == 'TMAX').sort(sqlf.desc('val')).limit(5).collect()
    print("Top 5 hottest weather stations only by temperature")
    for i in res:
        print('Station:%s\tTemperature:%.2f degrees Celsius' % (i.station, float(i['val']) / 10.0))

    # 3. Five distinct coldest weather stations
    res = clean_df.filter(clean_df['ele'] == 'TMIN').sort(sqlf.asc('val')).groupBy(clean_df['station']).agg(sqlf.min('val')).sort(sqlf.asc('min(val)')).limit(5).collect()
    print("Top 5 distinct coldest stations")
    for i in res:
        print('Station:%s\tTemperature:%.2f degrees Celsius' % (i.station, float(i['min(val)']) /10.0))

    # 3. Five coldest weather stations only by temperature
    res = clean_df.filter(clean_df['ele'] == 'TMIN').sort(sqlf.asc('val')).limit(5).collect()
    print("Top 5 coldest stations only by temperature")
    for i in res:
        print('Station:%s\tTemperature:%.2f degrees Celsius' % (i.station, float(i['val']) / 10.0))

# Aggregate statistics
# 4. Hottest and coldest weather stations on entire data
txtfile1 = sc.textFile('../data/20??.csv')
data = txtfile1.map(lambda x: x.split(','))
table = data.map(lambda r: Row(station=r[0], date=r[1], ele=r[2], val=int(r[3]), m_flag=r[4], q_flag=r[5], s_flag=r[6], obs_time=r[7]))
df = sqlc.createDataFrame(table)
clean_df = df.filter(df['q_flag'] == '')

# hottest day and weather station
res = clean_df.filter(clean_df['ele'] == 'TMAX').sort(sqlf.desc('val')).first()
print("Hottest station: %s on %s with temperature:%.2f degrees Celsius" % (res.station, res.date, float(res['val']) / 10.0))

# coldest day and weather station
res = clean_df.filter(clean_df['ele'] == 'TMIN').sort(sqlf.asc('val')).first()
print("Coldest Station: %s on %s with temperature:%.2f degrees Celsius" % (res.station, res.date, float(res['val']) / 10.0))