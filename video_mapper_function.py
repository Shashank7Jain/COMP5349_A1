#!/usr/bin/python3

import sys

def video_mapper_function():

  for line in sys.stdin:
#"""Reads every line from the data set and gives the output in the key value format"""
      parts = line.strip().split(",")

      if parts[0] == "video_id":
          continue
      #if mapperlist.length ==0:
      video_id = parts[0]
      category = parts[3]
      country = parts[11]

      print("{}\t{}\t{}".format(category,video_id,country))

if __name__ == '__main__':
    video_mapper_function()
