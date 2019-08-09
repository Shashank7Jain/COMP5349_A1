#!/usr/bin/python3

import sys

def read_map_output(file):
    """ return an key value pair extracted from the file system
        input format category \t vidoeid \t country
        output format (category average)
        """
    for line in file:
        yield line.strip().split("\t")

def reducer_video_function():
    current_cat  = ""
    count_countries = 0
    video_dict = dict()

    #data = open("mapper_video_output.txt","r")
    data = read_map_output(sys.stdin)
    for category, video_id, country in data:
        
        if current_cat != category:
        #until there is a change in the category the values are added to the dict and if the key already exist the country is added to the value list and the count_country is increased for every new country it parses.
            if current_cat =="":
                #first line and first category
                current_cat = category
                video_dict[video_id] = [country]
                count_countries +=1

            else:
            #change of category what to do print the average on the screen
                length_of_dict = len(video_dict)
            #divide by no of countries with no of unique videoID
                average = round(count_countries/float(length_of_dict),2)
                
                print("{}\t{}".format(current_cat,average))

                current_cat = category

                count_countries =0
                video_dict = dict()

                video_dict[video_id] = [country]
                count_countries +=1

        else:
            #check if the key exists and if not add it to the dict. 
            if video_id in video_dict.keys():
                countries  = video_dict[video_id]
            #get the values and check the if present country is present in the list if not increase the count_countries.
                if country not in countries:
                    count_countries +=1

                video_dict[video_id].append(country)

            else:
                video_dict[video_id] = [country]
                count_countries +=1
    #print("\n{}\n".format(video_dict))

if __name__ == '__main__':
    reducer_video_function()
