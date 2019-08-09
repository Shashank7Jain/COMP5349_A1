from pyspark import SparkContext
from datetime import datetime

def extractData(record):
    #try:
    #    video_id,trending_date,category_id,category,publish_time,views,likes,dislikes,comment_count,ratings_disabled,video_error_or_removed,country = record.split(",")
    ##        trending_date = datetime.strftime(datetime.strptime(trending_date, '%y.%d.%m'),'%Y-%m-%d')
    #    return ((video_id,country), (video_id,country,trending_date,category,likes,dislikes))
    #except:
    #    return ()
     try:
        #print("hello")
        #userID, movieID, rating, timestamp = record.split(",")
        #rating = float(rating)
        #return (movieID, rating)
        #for line in videoData:

        parts = record.strip().split(",")

        if parts[0] == "video_id":
            continue
        videoID = parts[0]
        date = parts[1]
        #dateparts = date.split(".")
        trending_date = datetime.strftime(datetime.strptime(date,'%y.%d.%m'),'%Y-%m-%d')
        category = parts[3]
        likes = parts[6]
        dislikes = parts[7]
        country =parts[11]

        return ((videoID,country),( videoID,country,trending_date,category,likes,dislikes))


    except:
        return ()

def calcDislikeGrowth(records):

    growthValue = (int(records[1][5]) - int(records[0][5])) - (int(records[1][4]) - int(records[0][4]))
    return ((growthValue),(records[0][0],growthValue,records[0][3],records[0][1]))


allvideos = sc.textFile("AllVideos_short.csv")

groupedData = allvideos.map(extractData).groupByKey().filter(lambda x: len(x[1]) >= 2)


groupedDataTwoRecords = groupedData.map(lambda rec: sorted(list(rec[1]), key=lambda k: k[2])[0:2])


withGrowth = groupedDataTwoRecords.map(calcDislikeGrowth).sortByKey(ascending= False)


for i in withGrowth.take(10):
    print("{},{},{},{}".format(i[1][0],i[1][1],i[1][2],i[1][3]))
