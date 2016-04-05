from average_degree import *
import unittest
from datetime import datetime
from heapq import *
from operator import itemgetter
import cProfile

# Self notes:
# -- make sure to check for different days, and possibly year, cant just use hours [FIXED]
# -- check the limit thing
# -- re-read everything


def dates(date):
	time_values = [str(item) for item in date.split()]
	return datetime.strptime((' ').join(time_values[0:4]+[time_values[5]]),"%a %b %d %H:%M:%S %Y")

class average_degree_TestCase(unittest.TestCase):
		
	def test_degree_calc(self):
		# tweet_1 = [set(['Spark' , 'Apache']) 			, dates("Thu Mar 24 17:51:10 +0000 2016")]
		# tweet_2 = [set(['Apache' , 'Hadoop', 'Storm']) 	, dates("Thu Mar 24 17:51:15 +0000 2016")]
		# tweet_3 = [set(['Apache']) 						, dates("Thu Mar 24 17:51:30 +0000 2016")]
		# tweet_4 = [set(['Flink' , 'Spark'])				, dates("Thu Mar 24 17:51:55 +0000 2016")]
		# tweet_5 = [set(['Spark' , 'HBase'])				, dates("Thu Mar 24 17:51:58 +0000 2016")]
		# tweet_6 = [set(["Hadoop", "Apache"])			, dates("Thu Mar 24 17:52:12 +0000 2016")]
		# tweet_7 = [set(['Flink' , 'HBase']) 	, dates("Thu Mar 24 17:52:10 +0000 2016")]
		# tweet_8 = [set(['Cassandra' , 'NoSQL']) 	,dates("Thu Mar 24 17:51:10 +0000 2016")]
		# tweet_9 = [set(['Kafka' , 'Apache']) ,dates("Thu Mar 24 17:52:20 +0000 2016")]
	
		# avg_graph = AvgDegreeGraph()
		# # first tweet
		# # print tweet_1
		# print 
		# # print avg_graph.tweet_history
		# avg_graph.new_tweet(tweet_1)
		# # print avg_graph.tweet_history
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()

		# # second tweet
		# # print tweet_2[0]
		# avg_graph.new_tweet(tweet_2)
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()

		# # third tweet
		# # print tweet_3[0]
		# avg_graph.new_tweet(tweet_3)
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()

		# # fourth tweet
		# # print tweet_4[0]
		# avg_graph.new_tweet(tweet_4)
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()

		# # 5 tweet
		# # print tweet_5[0]
		# avg_graph.new_tweet(tweet_5)
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()	

		# tweet_6 = [set(["Hadoop", "Apache"])				,dates("Thu Mar 24 17:52:12 +0000 2016")]
		# # 6 tweet
		# # print tweet_6[0]
		# avg_graph.new_tweet(tweet_6)
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()
		# avg_graph.new_tweet(tweet_7)
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()
		# avg_graph.new_tweet(tweet_8)
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()
		# avg_graph.new_tweet(tweet_9)
		# # print avg_graph.graph
		# print avg_graph.calculate_average_degree()


		# tweet_1 = [set(['Pisteare', 'elsientometro']), datetime(2015, 11, 5, 5, 5, 39)]
		# tweet_2 = [set(['whitepinkacrylic', 'ArceliasNails', 'mobilehomebasednailservice', 'fullset']), datetime(2015, 11, 5, 5, 5, 39)]
		# avg_graph = AvgDegreeGraph()
		# # print avg_graph.tweet_history
		# avg_graph.new_tweet(tweet_1)
		# # print avg_graph.tweet_history
		# # print avg_graph.graph
		# avg = avg_graph.calculate_average_degree()
		# print avg
		# output_to_file(avg)

		# # second tweet
		# # print tweet_2[0]
		# avg_graph.new_tweet(tweet_2)
		# # print avg_graph.graph
		# avg = avg_graph.calculate_average_degree()
		# print avg
		# output_to_file(avg)
		pass
	def test_timewindow(self):
		# tweet_0 = [set(['Spark' , 'Apache']) 			, dates("Thu Mar 24 17:51:10 +0000 2016")]
		# tweet_1 = [set(['Apache' , 'Hadoop', 'Storm']) 	, dates("Thu Mar 24 17:51:15 +0000 2016")]
		# tweet_2 = [set(['Apache']) 						, dates("Thu Mar 24 17:51:30 +0000 2016")]
		# tweet_3 = [set(['Flink' , 'Spark'])				, dates("Thu Mar 24 17:51:55 +0000 2016")]
		# tweet_4 = [set(['Spark' , 'HBase'])				, dates("Thu Mar 24 17:51:58 +0000 2016")]
		# tweet_5 = [set(["Hadoop", "Apache"])			, dates("Thu Mar 24 17:52:12 +0000 2016")]
		
		# new_1 = [set(['Flink' , 'HBase']) 	, dates("Thu Mar 24 17:52:10 +0000 2016")]
		# new_2 = [set(['Cassandra' , 'NoSQL']) 	,dates("Thu Mar 24 17:51:10 +0000 2016")]
		# new_3 = [set(['Kafka' , 'Apache']) ,dates("Thu Mar 24 17:52:20 +0000 2016")]
		# new_4 = [set(['L' , 'P']) ,dates("Thu Mar 24 17:58:21 +0000 2016")]

		# avg_graph = AvgDegreeGraph()
		
		# # first tweet
		# print 
		# avg_graph.new_tweet(tweet_0)
		# print pretty_print_graph(avg_graph.graph)
		# print 
		# avg_graph.new_tweet(tweet_1)
		# print pretty_print_graph(avg_graph.graph)
		# avg_graph.new_tweet(tweet_2)
		# print pretty_print_graph(avg_graph.graph)
		# avg_graph.new_tweet(tweet_3)
		# print pretty_print_graph(avg_graph.graph)
		# avg_graph.new_tweet(tweet_4)
		# print pretty_print_graph(avg_graph.graph)
		# avg_graph.new_tweet(tweet_5)
		# print pretty_print_graph(avg_graph.graph)
		# # print avg_graph.graph
		# print
		# avg_graph.new_tweet(new_1)
		# print pretty_print_graph(avg_graph.graph)
		# avg_graph.new_tweet(new_2)
		# print
		# print pretty_print_graph(avg_graph.graph)
		# # print pretty_print(avg_graph.tweet_history)
		# avg_graph.new_tweet(new_3)
		# print pretty_print_graph(avg_graph.graph)
		# print avg_graph.calculate_average_degree()
		# avg_graph.new_tweet(new_4)
		# print
		# # print pretty_print(avg_graph.tweet_history)
		# avg_graph.new_tweet(new_4)
		# print
		# print pretty_print(avg_graph.tweet_history)
		# print abs(new_4[1]-tweet_2[1]).seconds
		pass
	def test_timedef(self):
		# tweet_1 = (set(['Spark' , 'Apache']) 			, datetime.strptime("17:51:10","%H:%M:%S"))
		# tweet_2 = (set(['Apache' , 'Hadoop', 'Storm']) 	, datetime.strptime("17:51:15","%H:%M:%S"))
		# tweet_3 = (set(['Apache']) 						, datetime.strptime("17:51:30","%H:%M:%S"))
		# tweet_4 = (set(['Flink' , 'Spark'])				, datetime.strptime("17:51:55","%H:%M:%S"))
		# tweet_5 = (set(['Spark' , 'HBase'])				, datetime.strptime("17:51:58","%H:%M:%S"))
		# tweets = [tweet_5, tweet_2, tweet_3, tweet_4 ,tweet_1]
		# times = [tweet_5[1], tweet_2[1], tweet_3[1], tweet_4[1], tweet_1[1]]
		# # print times
		# times.sort()
		# print times
		# print
		# heap = []
		# for tweet in tweets:
		# 	heappush(heap, tweet)
		# sortedx = []
		# while heap:
		#     sortedx.append(heappop(heap))
		# print sortedx
		# print
		# tweets.sort(key=itemgetter(1), reverse=False)
		# sortedx.sort(key=itemgetter(1), reverse=False)
		# print tweets
		# print
		# print sortedx
		# print tweets == sortedx
		pass
	def test_output(self):
		tweet_1 = [set(['Spark' , 'Apache']) 			, dates("Thu Mar 24 17:51:10 +0000 2016")]
		tweet_2 = [set(['Apache' , 'Hadoop', 'Storm']) 	, dates("Thu Mar 24 17:51:15 +0000 2016")]
		tweet_3 = [set(['Apache']) 						, dates("Thu Mar 24 17:51:30 +0000 2016")]
		tweet_4 = [set(['Flink' , 'Spark'])				, dates("Thu Mar 24 17:51:55 +0000 2016")]
		tweet_5 = [set(['Spark' , 'HBase'])				, dates("Thu Mar 24 17:51:58 +0000 2016")]
		tweet_6 = [set(["Hadoop", "Apache"])			, dates("Thu Mar 24 17:52:12 +0000 2016")]
		tweet_7 = [set(['Flink' , 'HBase']) 	, dates("Thu Mar 24 17:52:10 +0000 2016")]
		tweet_8 = [set(['Cassandra' , 'NoSQL']) 	,dates("Thu Mar 24 17:51:10 +0000 2016")]
		tweet_9 = [set(['Kafka' , 'Apache']) ,dates("Thu Mar 24 17:52:20 +0000 2016")]

		tweets = [tweet_1]+[tweet_2]+[tweet_3]+[tweet_4]+[tweet_5]+[tweet_6]+[tweet_7]+[tweet_8]+[tweet_9]
		avg_graph = AvgDegreeGraph()
		with open('test.txt','w') as out_f:
			for tweet in tweets:
				try:
				# check if new tweet containts more 2 or more hastags
					# if len(load_hastags(line))!=2:
					# add new tweet
					# print line
					avg_graph.new_tweet(tweet)
					print avg_graph.check_if_valid(tweet[1])
					avg = avg_graph.calculate_average_degree()
					print avg
					if avg_graph.check_if_valid(tweet[1]):
						out_f.write(str(avg)+'\n')
				except:
					continue

def pretty_print(graph):
	for time,hashtags in graph:
		pretty_hashes = []
		for hashtag in hashtags:
			pretty_hashes.append(hashtag) 
		print pretty_hashes, time

def pretty_print_graph(graph):
	for key,hashtags in graph.items():
		pretty_hashes = []
		for hashtag in hashtags:
			pretty_hashes.append(hashtag) 
		print key, pretty_hashes

if __name__ == '__main__':

	unittest.main()




	