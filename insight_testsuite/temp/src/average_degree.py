"""
============================================
Insight Data Science Challenge 
============================================
author: @kevin_alcedo

input: input file name and output file name 

output: output text file with average degree

summary: calculation of average degree of
hashtags updated each time a new tweet is 
processed, regardless of if it falls outside 
the 60 second window. 

"""

# print(__doc__)
import sys
import json
from datetime import datetime
from heapq import *
from operator import itemgetter

class AvgDegreeGraph(object):
	def __init__(self):

		# graph containing edge list
		self.graph = {}
		# history of current tweets in the time window
		self.tweet_history = []
		# newest and oldest tweets in order to find the bounds of the current time window
		self.newest_tweet = None
		self.oldest_tweet = None
		self.time_window = 60.0

	def calculate_average_degree(self):
		# go through all values in the hashtag dictionary and measure the degree, and total average degree
		average = (sum( len(hashtags) for hashtags in (self.graph).values()) / (len((self.graph).keys())*1.0))
		# return value truncated to the second decimal point
		return format((int(average*100)/float(100)), '.2f')

	def new_tweet(self,new_tweet):
		# collect hastags from incoming tweet
		hashtags = new_tweet[0]
		time = new_tweet[1]

		# check if within time window
		if self.check_if_valid(time):
			# if there are two or more hashtags create 
			# if len(hashtags) >= 2:
			# add new tweet to history of tweets
			heappush(self.tweet_history,(time,hashtags))

			# update the newest and oldest tweets
			self.newest_tweet = nlargest(1,self.tweet_history)
			self.oldest_tweet = nsmallest(1,self.tweet_history)
			
			# check if oldests are out of time window and remove edges
			self.remove_out_of_window()

			# add new edges
			self.add_edges()
				
	def add_edges(self):

		# go through each hashtag in current window and add edges
		for hashtags in self.tweet_history:
			for hashtag in hashtags[1]:
				# if this is a new hashtag then add connections
				if hashtag not in (self.graph).keys():
					self.graph[hashtag] = hashtags[1]-set([hashtag])
				else: 
					# create an union of the new hashtags except itself
					self.graph[hashtag] = (self.graph[hashtag])|(hashtags[1]-set([hashtag]))

	def remove_edges(self,hashtags):
		# remove edges 
		for hashtag in hashtags:
			self.graph[str(hashtag)] = self.graph[str(hashtag)]-(hashtags-set([hashtag]))
			if len(self.graph[str(hashtag)]) == 0:
				(self.graph).pop(str(hashtag), None)
		
	def remove_out_of_window(self):
		# remove every old tweet thats outside the window
		for tweet in self.tweet_history:
			if (abs(tweet[0]-self.newest_tweet[0][0]).seconds)>self.time_window:
				self.remove_edges(tweet[1])
				heappop(self.tweet_history)
				# update newest and oldest tweet
				self.newest_tweet = nlargest(1,self.tweet_history)
				self.oldest_tweet = nsmallest(1,self.tweet_history)
			else: break


	def check_if_valid(self,time):
		# if the tweet history is empty then return true
		if len(self.tweet_history) <= 1: 
			return True
		# else if the time is within the window return true
		elif time>(self.oldest_tweet[0][0]) and time<(self.newest_tweet[0][0]): 
			return True
		# else if the time is outside the window, outside of the oldest tweet return false
		elif time<(self.oldest_tweet[0][0]): 
			return False
		# else if the time is past the window but newer then return true
		elif time>(self.newest_tweet[0][0]) and abs(time-self.oldest_tweet[0][0]).seconds>self.time_window:
			return True
		# otherwise return true
		else: return True

def load_hastags(new_tweet):
	# returns a list of hastags from an incoming tweet as a set to keep unique hashtags only 
	return set([ (unicode(hashtag['text'])).encode('utf-8') for hashtag in new_tweet["entities"]["hashtags"] ])

def load_dates(new_tweet):
	# split the strings within "created_at" 
	time_values = [str(item) for item in new_tweet["created_at"].split()]
	# returns a list with time from an incoming tweet
	return datetime.strptime((' ').join(time_values[0:4]+[time_values[5]]),"%a %b %d %H:%M:%S %Y")

if __name__ == '__main__':
	# check for correct input 
	if len(sys.argv) != 3:
		print ('function must be called with: python ./src/average_degree.py ./tweet_input/tweets.txt ./tweet_output/output.txt')
		sys.exit(1)

	input_file = sys.argv[1]
	output_file = sys.argv[2]

	# create graph object
	avg_graph = AvgDegreeGraph()

	# parse through json object on each line of the input text file
	with open(input_file,'r') as in_f, open(output_file,'w+') as out_f:
		# calculate 
		for new_line in in_f:
			line = json.loads(new_line)

			# perform calculations if tweet contains the necessary fields
			# also prevents the rate limiting messages from causing errors
			try:
				# add new tweet to graph
				tweet = [load_hastags(line)]+[load_dates(line)]
				avg_graph.new_tweet(tweet)
				# if the new tweet satisfies time window the add average to output file
				avg = avg_graph.calculate_average_degree()
				out_f.write(str(avg)+'\n')
			except:
				continue


