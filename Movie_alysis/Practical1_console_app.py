#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 19 10:41:01 2022

@author: najmi
"""
import pandas as pd
from pyspark.ml.feature import VectorAssembler
import sys
from py4j.java_gateway import JavaGateway
import pyspark
from pyspark import SparkConf
from pyspark.sql import DataFrame,group
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from tkinter import *

from tkinter import *
import autovizwidget
from tkinter.ttk import *



df = pd.read_csv("/Users/najmi/Desktop/Semester 2/Data Intensive systems/ml-latest-small/movies.csv")
df['Year']=df.title.str.extract('\((.*?)\)')
spark = SparkSession.Builder().master("local[*]")\
.appName("Movie_alysis").config("spark.some.config.option","some-value")\
.getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")


df


def main():
    comp_join= []
    KMeans_transform = []
    Joined_data = setting_data()
    h=[]
    while(1):
        print("Search by user ID ---> 0\nSearch by Title---> 1\
              \nearch by Genre ---> 2\
              \nSearch by Year ---> 3\
              \nSearch by Top Movies ---> 4\nSearch by Movie ID ---> 5\nCompare 2 Users ---> 6\
              \nClustered users by move taste ---> 7")
        scanner = spark.sparkContext._gateway.jvm.java.util.Scanner
        sys_in = getattr(spark.sparkContext._gateway.jvm.java.lang.System, 'in')
        result = scanner(sys_in).nextLine()
        v = int(result) 
        o=1
        n="Horror"
        genre_data= Joined_data.filter(Joined_data.genres=="Horror")
        case = {0:"Enter User ID",
                1: "Enter Movie name",
                2: "Enter Genre",
                3: "Enter Year",
                4: "Top Movies",
                5:"Enter Movie ID"
            }
        
        if v==0:
            print("0 ---> All data of a user\n1 ---> movie list by user\
                   \n2---> Total movies watched by a User\
                   \n3---> Get Total genres watched by a user\
                   \n4---> User's top rated genre\
                   \n5---> Favourite genre by most viewed")
            a= int(ask_input())
            print(case[v])
            o = ask_input()
        elif v==1:
            print("0---> Ratings of a movie by ID\n1--> Get Total watches")
            a= int(ask_input())
            print(case[v])
            n=str(ask_input())
            n=n.upper()
        
        elif v==2:
           print(case[v])
           n=str(ask_input())
           n=n.upper()
           a=0
        elif v==3:
            print(case[v])
            o=ask_input()
            a=0
        elif v==4:
            print(case[v])
            print("0---> Top Ratings \n1--> Top watched")
            a=ask_input()
            a=0
        
        elif v==5:
            print("0---> Ratings of a movie by ID\n1--> Get Total watches")
            a= int(ask_input())
            print(case[v])
            o=ask_input()
        elif v==6:
            
            print("Enter First User ID")
            User1=ask_input()
            print("Enter Second User ID")
            User2=ask_input()
            
            Joined_data.groupBy("userId","genres").agg({"rating":"mean"}).withColumnRenamed("avg(rating)","avg_rating").sort(desc("avg_rating")).show()
            user_data1 = Joined_data.filter(Joined_data.userId==User1)
            us_1=user_data1.groupBy("userId","genres").agg({"rating":"mean"}).withColumnRenamed("avg(rating)","avg_rating").sort(desc("avg_rating"))
            user_data2 = Joined_data.filter(Joined_data.userId==User2)
            us_2=user_data2.groupBy("userId","genres").agg({"rating":"mean"}).withColumnRenamed("avg(rating)","avg_rating").sort(desc("avg_rating"))
            jo_comp = us_1.join(us_2,on="genres",how="outer")
            us_3=user_data1.groupBy("userId","genres").agg({"title":"count"}).withColumnRenamed("count(title)","most").sort(desc("most"))
            us_3=us_3.withColumnRenamed("userId", "Id")
            us_4=user_data2.groupBy("userId","genres").agg({"title":"count"}).withColumnRenamed("count(title)","most").sort(desc("most"))
            us_4=us_4.withColumnRenamed("userId", "Id")
            most_comp= us_3.join(us_4,on="genres",how="outer")
           
            out_join1=us_1.join(us_3,on=us_1.genres==us_3.genres,how="inner").drop(us_3.genres)
            out_join1=out_join1.drop("Id")
            out_join2=us_2.join(us_4,on=us_2.genres==us_4.genres,how="inner").drop(us_2.genres)
            out_join2=out_join2.drop("Id")
            
            comp_join=out_join1.join(out_join2,on="genres",how="outer")
            
            a=0
        elif v==7:
            print("0---> See user ID view of with all genres and their cluster\n1--> Cluster view")
            a= int(ask_input())
            df_2=Joined_data.groupBy("userId","genres").agg({"title":"count"}).withColumnRenamed("count(title)","most").sort(asc("userId"))
            ao=df_2.groupBy("userId").pivot("genres").sum("most").drop("(no genres listed)","IMAX","Film-Noir","Documentary")
            ao=ao.na.fill(value=0)
            assemble=VectorAssembler(inputCols=ao.columns[1:],outputCol = 'features')
            
            assembled_data=assemble.transform(ao)
            KMeans_=KMeans(featuresCol="features",predictionCol='prediction',k=8,maxIter=10,distanceMeasure='euclidean')
            KMeans_fit=KMeans_.fit(assembled_data)
            KMeans_transform=KMeans_fit.transform(assembled_data)
            h=KMeans_transform.groupBy("userId").pivot('prediction').sum("userId").drop("userId")
            h=h.na.fill(value=0)
            
   
        else:
                print("Error message: wrong usage")
                print("0:Enter User ID\n1:Enter Movie ID")
                v=ask_input()
        
       
        
        genre_data= Joined_data.filter(col("genres").startswith(n))
        user_data = Joined_data.filter(Joined_data.userId==o)
        movie_data= Joined_data.filter(Joined_data.movieId==o)
        year_data= Joined_data.filter(Joined_data.year==o)
        title_data= Joined_data.filter(col("title").startswith(n))
        
       #Search movie by id/title, show the average rating, the number of users that have 
       #watched the movie
        options = {0:{0:user_data,
                      1:user_data.select("title"),
                      2:user_data.select(countDistinct(user_data.movieId)),
                      3:user_data.select(countDistinct(user_data.genres)),
                      4:user_data.groupBy("userId","genres").agg({"rating":"mean"}).withColumnRenamed("avg(rating)","avg_rating").sort(desc("avg_rating")),
                      5:user_data.groupBy("userId","genres").agg({"title":"count"}).withColumnRenamed("count(title)","Times_Watched").sort(desc("Times_Watched"))
                      },
                   1:{0:get_Rating(title_data),
                      1:get_total_movie_watched(title_data)
                       },
                   2:{0:genre_data.select("title")
                      },
                   3:{0:year_data.select("title")
                       },
                   4:{0:Joined_data.groupBy("movieId","title")\
                      .agg({"rating":"mean"})\
                      .withColumnRenamed("avg(rating)", "avg_rating").sort(desc("avg_rating")).show,
                      1:Joined_data.groupBy("movieId","title").agg({"userId":"count"}).withColumnRenamed("count(userId)", "count").sort(desc("count"))
                       },
                   5:{0:get_Rating(movie_data),
                      1:get_total_movie_watched(movie_data)
                       },
                   6:{0:comp_join
                       },
                   7:{0:KMeans_transform,
                      1:h
                       }
                   }
        if v==0:
            if a>3:
                options[v][a].show(1)
            else:
                print("How many entries you want to see?")
                rows = int(ask_input())
                options[v][a].distinct().show(rows)
                
        else:
            print("How many entries you want to see?")
            rows = int(ask_input())
            options[v][a].distinct().show(rows)
            
        
   #user_data.groupBy("userId","genres","title").agg({"rating":"mean"}).withColumnRenamed("avg(rating)","avg_rating").sort(desc("avg_rating")).show()
        
def ask_input():
    scanner = spark.sparkContext._gateway.jvm.java.util.Scanner  
    sys_in = getattr(spark.sparkContext._gateway.jvm.java.lang.System, 'in')  
    return scanner(sys_in).nextLine()
   
       

def setting_data():
    
    
   

    Movie_data = spark.read.option("delimiter", ",").option("header", "true").option("inferSchema", "true").csv('/Users/najmi/Desktop/Semester 2/Data Intensive systems/ml-latest-small/movies.csv')

    Ratings_data = spark.read.option("delimiter", ",").option("header","true").csv('/Users/najmi/Desktop/Semester 2/Data Intensive systems/ml-latest-small/ratings.csv')

    Movie_data = Movie_data.withColumn("genres", explode(split("genres", "[|]")))
    Movie_data = Movie_data.withColumn("genres", upper(Movie_data.genres))
    Movie_data = Movie_data.withColumn("title", upper(Movie_data.title))
    

    Ratings_data=Ratings_data.withColumn("userId",col("userId").cast("int"))

    Ratings_data=Ratings_data.withColumn("movieId",col("movieId").cast("int"))

    Movie_data=Movie_data.withColumn("movieId",col("movieId").cast("int"))

    Joined_data = Ratings_data.join(Movie_data,Ratings_data.movieId==Movie_data.movieId).drop(Movie_data.movieId)
    
    movies_with_year_df = Movie_data.select('movieId','title',regexp_extract('title',r'\((\d+)\)',1).alias('year')).distinct()
    
    year = movies_with_year_df.select("year","movieId")
    
    Joined_data= Joined_data.join(year,Joined_data.movieId==year.movieId).drop(year.movieId)
    
    Joined_data=Joined_data.repartition(8)
    return Joined_data
        


def get_Rating(a):
    
    return a.agg({"rating":"mean"}).withColumnRenamed("avg(rating)", "Avg Rating")
   
def get_total_movie_watched(a):
    
    return a.select(count(a.movieId))



main()
    




