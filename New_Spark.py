from pyspark import SparkContext
from datetime import datetime

def requiredData(record):
    try:
        #get each line from the dataset put the values in the key value pair.
        parts = record.split(",")
        video_id = parts[0]
        trending_date = parts[1]
        category = parts[3]
        likes = parts[6]
        dislikes = parts[7]
        country = parts[11]

        if(trending_date != 'trending_date'):
            #with the guidence of internet and friend changed the format of the date to YYY-MM-DD.
            trending_date = datetime.strftime(datetime.strptime(trending_date, '%y.%d.%m'),'%Y-%m-%d')
        return ((video_id,country), (video_id,country,trending_date,category,likes,dislikes))
    except:
        return ()

def topVideos(rec):
    return (str(rec[1][0]),rec[1][1],str(rec[1][2]),str(rec[1][3]))

def calculation(rec):
    topValue = (int(rec[1][5]) - int(rec[0][5])) - (int(rec[1][4]) - int(rec[0][4]))
    return ((topValue),(rec[0][0],topValue,rec[0][3],rec[0][1]))


sc = SparkContext(appName='Trending Vidoes')#creating the sparkcontext variable
inputfile = sc.textFile("AllVideos_short.csv") #setting the textfile to the spark context

Data = inputfile.map(requiredData) # Data will get the key value pair by using the map function.
groupedData = Data.groupByKey() # on the Data we will combine keys based on the videoID and country.
filterData =groupedData.filter(lambda something: len(something[1]) >= 2) # cutting short the data which has less length less than 2. 
#print(filterData.take[10])

sortAndSliceData = filterData.map(lambda rec: sorted(list(rec[1]), key=lambda keys: keys[2])[0:2])
#this is complex one here we will sort the date with the value and slicing the data set by first two row.
calData = sortAndSliceData.map(calculation)
#cal the differnce in the dislike to like and store the value in the cal data.
descendingData = calData.sortByKey(ascending= False)
#sort the data according to the dislike number
trendRate = descendingData.map(topVideos)
#from the key value we are getting the required data in the format
sc.parallelize(trendRate.take(10)).saveAsTextFile('output')
#here, we are printing the data to the outputfile
print(descendingData.take(10))


