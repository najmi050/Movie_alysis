#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 23 15:20:59 2022

@author: najmi
"""
import pandas as pd
from pyspark.ml.feature import VectorAssembler
import sys
from py4j.java_gateway import JavaGateway
import pyspark.sql
from pyspark import SparkConf
from pyspark.sql import DataFrame,group
from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import *
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from tkinter import *
from tkinter.ttk import *
spark = SparkSession.Builder().master("local[*]")\
.appName("Movie_alysis").config("spark.some.config.option","some-value")\
.getOrCreate()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
from tkinter import *
from pandastable import Table, TableModel
import tabloo
import tkinter as tk
import tkinter.scrolledtext as st
from pandastable import Table,TableModel

  
# Creating tkinter window

  
# Title Label

master = Tk()
master.title('Movie_alysis')                        
master.geometry("1200x900")
master.configure(bg='black')
frame = Frame(master=master)
frame.pack(side='left', fill='both', expand=True)
text_area = st.ScrolledText(frame,wrap = tk.WORD,
                            font = ("Times New Roman",19))


def setting_data():

    Movie_data = spark.read.option("delimiter", ",").option("header", "true").option("inferSchema", "true").csv('/Users/najmi/Desktop/Semester 2/Data Intensive systems/CS5052_practical1/ml-latest-small/movies.csv')

    Ratings_data = spark.read.option("delimiter", ",").option("header","true").csv('/Users/najmi/Desktop/Semester 2/Data Intensive systems/CS5052_practical1/ml-latest-small/ratings.csv')

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


def get_user_movies():
   
    
    
   
    Joined_data = setting_data()
    
    user_data = Joined_data.filter(Joined_data.userId==e.get()).select("title").distinct()
    user_data=user_data.toPandas()
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None) 
    pd.set_option('display.width', 1000) 
    pd.set_option('display.colheader_justify', 'center') 
    pd.set_option('display.precision', 3)
   
    
    #text_area.insert(END,user_data)
    
    text_area.configure(state ='normal')
    
    pt = Table(frame,dataframe=user_data)
    pt.show()
    
   

    
   
    
    
def get_user_movie_count():
   
    
    
    Joined_data = setting_data()
    user_data = Joined_data.filter(Joined_data.userId==e.get()).select(countDistinct("movieId")).withColumnRenamed("count(DISTINCT movieId)", "Total movies watched by user")
    user_data=user_data.toPandas()
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
    
   
    #text_area.insert(END,user_data)
    
    pt = Table(frame,dataframe=user_data)
    pt.show()
    

def get_user_genre_count():
  
    
    Joined_data = setting_data()

    user_data = Joined_data.filter(Joined_data.userId==e.get()).select(countDistinct("genres")).withColumnRenamed("count(DISTINCT genres)", "Total genres watched by user")
    user_data=user_data.toPandas()
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None) 
    
   
    #text_area.insert(END,user_data)
    
    pt = Table(frame,dataframe=user_data)
    pt.show()
    

def get_genre_rating():
    
    Joined_data = setting_data()

    user_data = Joined_data.filter(Joined_data.userId==e.get()).groupBy("userId","genres").agg({"rating":"mean"}).withColumnRenamed("avg(rating)","avg_rating").sort(desc("avg_rating"))
    user_data=user_data.toPandas()
    
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None) 
    
   
    #text_area.insert(END,user_data)
    pt = Table(frame,dataframe=user_data)
    pt.show()
   
    
    
def get_fav_genre_watched():
    
    Joined_data = setting_data()
    
    user_data = Joined_data.filter(Joined_data.userId==e.get()).groupBy("userId","genres").agg({"title":"count"}).withColumnRenamed("count(title)","Times_Watched").sort(desc("Times_Watched"))
     
    user_data=user_data.toPandas()
    
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
   
    fav=user_data.head(1)
    pt = Table(frame,dataframe=fav)
    pt.show()
    
   
def get_fav_genre_rating():
   
    Joined_data = setting_data()

    user_data = Joined_data.filter(Joined_data.userId==e.get()).groupBy("userId","genres").agg({"rating":"mean"}).withColumnRenamed("avg(rating)","Favourite_Rated_Genre").sort(desc("Favourite_Rated_Genre"))
    user_data=user_data.toPandas()
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
    fav=user_data.head(1)
   
    pt = Table(frame,dataframe=fav)
    pt.show()
    
    
    
def get_genre_watched():
   
    
    
    
    Joined_data = setting_data()

    
    user_data = Joined_data.filter(Joined_data.userId==e.get()).groupBy("userId","genres").agg({"title":"count"}).withColumnRenamed("count(title)","Favourite_MostWatched_Genre").sort(desc("Favourite_MostWatched_Genre")) 
    user_data=user_data.toPandas()
    
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
   
    #text_area.insert(END,user_data)
    
    pt = Table(frame,dataframe=user_data)
    pt.show()
    
    
def get_Rating_byID():
    #text_area = st.ScrolledText(frame,wrap = tk.WORD,height=30,width=60,font = ("Times New Roman",15))
    
   
    Joined_data = setting_data()

    movie_data= Joined_data.filter(Joined_data.movieId==e2.get())
    title_data= movie_data.agg({"rating":"mean"}).withColumnRenamed("avg(rating)", "Avg Rating")
   
    title_data=title_data.toPandas()
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
   
    #text_area.insert(END,title_data)
    
    pt = Table(frame,dataframe=title_data)
    pt.show()
    
   
def get_Movie_user_watched_byID():
    
    #text_area = st.ScrolledText(frame,wrap = tk.WORD,height=30,width=60, font = ("Times New Roman",15))
    
    
    Joined_data = setting_data()

    movie_data= Joined_data.filter(Joined_data.movieId==e2.get())
    title_data= movie_data.select(count("movieId"))
   
    title_data=title_data.toPandas()
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
   
    #text_area.insert(END,title_data)
    
    pt = Table(frame,dataframe=title_data)
    pt.show()
    
    
    
def get_Rating():
    #text_area = st.ScrolledText(frame,wrap = tk.WORD,height=30,width=60,font = ("Times New Roman",15))
    
    text_area.delete(1.0,END)
    Joined_data = setting_data()
    name_movie= e3.get()
    name_movie = name_movie.upper()

    movie_data= Joined_data.filter(col("title").startswith(name_movie))
    title_data= movie_data.groupby("title").agg({"rating":"mean"}).withColumnRenamed("avg(rating)", "Avg Rating")
   
    title_data=title_data.toPandas()
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
   
   

    #text_area.insert(END,title_data)
    pt = Table(frame,dataframe=title_data)
    pt.show()
    
    
   
def get_Movie_user_watched():
    
  #  text_area = st.ScrolledText(frame,wrap = tk.WORD,height=30,width=60,  font = ("Times New Roman",15))
    name_movie= e3.get()
    
    name_movie = name_movie.upper()
    Joined_data = setting_data()
    

    movie_data= Joined_data.filter(col("title").startswith(name_movie))
    
                                   
    title_data = movie_data.groupby("title").agg({"movieId":"count"})
    
   
    title_data=title_data.toPandas()
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)

    #text_area.insert(END,title_data)
    pt = Table(frame,dataframe=title_data)
    pt.show()
    

   
def get_Movie_byGenre():
    
   # text_area = st.ScrolledText(frame,wrap = tk.WORD,height=30,width=60,font = ("Times New Roman",15))
    name_genre= e4.get()
    name_genre = name_genre.upper()
    Joined_data = setting_data()

    movie_data= Joined_data.filter(col("genres").startswith(name_genre))
                                   
    
    title_data=movie_data.select(col("title"),col("genres")).distinct()
   
    title_data=title_data.toPandas()
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)

    
    #text_area.insert(END,title_data)
    pt = Table(frame,dataframe=title_data)
    pt.show()
    
def get_Movie_byYear():
    
    #text_area = st.ScrolledText(frame,wrap = tk.WORD,height=30,width=60,    font = ("Times New Roman",15))
    year= e5.get()
    Joined_data = setting_data()

    year_data= Joined_data.filter(Joined_data.year==year)
                                   
    
    title_data=year_data.select(col("title"),col("genres")).distinct()
   
    title_data=title_data.select("title")
    title_data=title_data.toPandas()
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)

    
    #text_area.insert(END,title_data)
    pt = Table(frame,dataframe=title_data)
    pt.show()
    

def get_TopMovies():
    
    #text_area = st.ScrolledText(frame,wrap = tk.WORD,height=30,width=60,    font = ("Times New Roman",15))
    n= int(e5.get())
    Joined_data = setting_data()

   
   
    title_data=Joined_data.groupBy("movieId","title")\
       .agg({"rating":"mean"})\
       .withColumnRenamed("avg(rating)", "avg_rating").sort(desc("avg_rating"))
    title_data=title_data.toPandas()
   
    
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
    title_data=title_data.head(n)

    
    pt = Table(frame,dataframe=title_data)
    pt.show()
    

def get_MostWatchedMovies():
    
    n= int(e5.get())
    Joined_data = setting_data()

   
   
    title_data=Joined_data.groupBy("movieId","title").agg({"userId":"count"})\
    .withColumnRenamed("count(userId)", "count").sort(desc("count"))
    title_data=title_data.toPandas()
   
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)

    
    title_data=title_data.head(n)

    
    #text_area.insert(END,title_data)
    pt = Table(frame,dataframe=title_data)
    pt.show()
    
    
def get_UserComparison():
    
    #text_area = st.ScrolledText(frame,wrap = tk.WORD,height=30,width=60,    font = ("Times New Roman",15))
    User1= int(e7.get())
    User2= int(e8.get())
    
    Joined_data = setting_data()
    
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None) 

   
   
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
    out_join2=out_join2.drop("userId").withColumnRenamed("avg_rating","avg_rating1")
    out_join2=out_join2.withColumnRenamed("most","most1")
    comp_join=out_join1.join(out_join2,on="genres",how="outer")
    comp_join=comp_join.na.fill(value=0)
    comp_join= comp_join.toPandas()
    comp_join['user_1']= comp_join['userId']
    comp_join['Most_Watched_1']= comp_join['most']
    comp_join['AVG_Rating_1']= comp_join['avg_rating']
    comp_join['user_2']= comp_join['Id']
    
    comp_join['Most_Watched_2']= comp_join['most1']
    
    comp_join['AVG_Rating_2']= comp_join['avg_rating1']
    
    comp_join['Genres']= comp_join['genres']
    comp_join=comp_join.drop(["userId","Id","most","most1","avg_rating","avg_rating1","genres"],axis=1)
    
   
    
    #text_area.insert(END,comp_join)
    pt = Table(frame,dataframe=comp_join)
    pt.show()
    
def get_Cluster():
    
    Joined_data = setting_data()

   
    df_2=Joined_data.groupBy("userId","genres").agg({"title":"count"}).withColumnRenamed("count(title)","most").sort(asc("userId"))
    ao=df_2.groupBy("userId").pivot("genres").sum("most").drop("(no genres listed)","IMAX","Film-Noir","Documentary")
    ao=ao.na.fill(value=0)
    assemble=VectorAssembler(inputCols=ao.columns[1:],outputCol = 'features')
    
    assembled_data=assemble.transform(ao)
    KMeans_=KMeans(featuresCol="features",predictionCol='prediction',k=8,maxIter=10,distanceMeasure='euclidean')
    KMeans_fit=KMeans_.fit(assembled_data)
    KMeans_transform=KMeans_fit.transform(assembled_data)
    from pyspark.ml.feature import PCA as PCAml 
    pca = PCAml(k=2, inputCol="features", outputCol="pca") 
    pca_model = pca.fit(assembled_data) 
    pca_transformed = pca_model.transform(assembled_data)
    import numpy as np 
    x_pca = np.array(pca_transformed.rdd.map(lambda row: row.pca).collect()) 
    cluster_assignment = np.array(KMeans_transform.rdd.map(lambda row: row.prediction).collect()).reshape(-1,1)
    import seaborn as sns 
    import matplotlib.pyplot as plt
    pca_data = np.hstack((x_pca,cluster_assignment))

    pca_df = pd.DataFrame(data=pca_data, columns=("1st_principal", "2nd_principal","cluster_assignment"))
    sns.FacetGrid(pca_df,hue="cluster_assignment", height=6).map(plt.scatter, '1st_principal', '2nd_principal' ).add_legend()
    
    h=KMeans_transform.groupBy("userId").pivot('prediction').sum("userId").drop("userId")
    h=h.na.fill(value=0)
    
    h=h.toPandas()
    
    for col in h: 
        h[col] = h[col].sort_values(ignore_index=True,ascending=False) 
    
    
    
    
    
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)

    
    
    #text_area.insert(END,title_data)
    pt = Table(frame,dataframe=h)
    pt.show()
    plt.show()    

def get_UserIDwise_Cluster():
    
    Joined_data = setting_data()

   
    df_2=Joined_data.groupBy("userId","genres").agg({"title":"count"}).withColumnRenamed("count(title)","most").sort(asc("userId"))
    ao=df_2.groupBy("userId").pivot("genres").sum("most").drop("(no genres listed)","IMAX","Film-Noir","Documentary")
    ao=ao.na.fill(value=0)
    assemble=VectorAssembler(inputCols=ao.columns[1:],outputCol = 'features')
    
    assembled_data=assemble.transform(ao)
    KMeans_=KMeans(featuresCol="features",predictionCol='prediction',k=8,maxIter=10,distanceMeasure='euclidean')
    KMeans_fit=KMeans_.fit(assembled_data)
    
    KMeans_transform=KMeans_fit.transform(assembled_data)
    
    
   
    pd.set_option('display.max_rows', None) 
    pd.set_option('display.max_columns', None)
    
    from pyspark.ml.feature import PCA as PCAml 
    pca = PCAml(k=2, inputCol="features", outputCol="pca") 
    pca_model = pca.fit(assembled_data) 
    pca_transformed = pca_model.transform(assembled_data)
    import numpy as np 
    x_pca = np.array(pca_transformed.rdd.map(lambda row: row.pca).collect()) 
    cluster_assignment = np.array(KMeans_transform.rdd.map(lambda row: row.prediction).collect()).reshape(-1,1)
    import seaborn as sns 
    import matplotlib.pyplot as plt
    pca_data = np.hstack((x_pca,cluster_assignment))

    pca_df = pd.DataFrame(data=pca_data, columns=("1st_principal", "2nd_principal","cluster_assignment"))
    sns.FacetGrid(pca_df,hue="cluster_assignment", height=6).map(plt.scatter, '1st_principal', '2nd_principal' ).add_legend()

     
    KMeans_transform=KMeans_transform.select('userId','features','prediction')
    KMeans_transform = KMeans_transform.toPandas()



    
    
    #text_area.insert(END,title_data)
    pt = Table(frame,dataframe=KMeans_transform)
    pt.show()
    plt.show()    

def movieRecommendation():
    Joined_data=setting_data()
    ratings_big = spark.read.option("delimiter", ",").option("header", "true").option("inferSchema", "true").csv('/Users/najmi/Desktop/Semester 2/Data Intensive systems/ml-latest-small/ratings.csv')            
    from pyspark.ml.recommendation import ALS 
    als = ALS(maxIter=10, regParam=0.3, userCol="userId", 
                      itemCol = "movieId", ratingCol = "rating", coldStartStrategy = "drop") 
    
    alsModel = als.fit(ratings_big) 
    prediction = alsModel.transform(ratings_big)
    recommended_movie_df = alsModel.recommendForAllUsers(10) 
    
    
    
    recommended_movie_df =  recommended_movie_df.filter(recommended_movie_df.userId==e.get())
    df = recommended_movie_df.toPandas()
    df = df.explode('recommendations')
    df = pd.concat([df.drop(['recommendations'],axis=1),df['recommendations'].apply(pd.Series)],axis=1)
    df= df.drop('userId',axis=1)
    df=df.rename(columns={0:'movieId',1:'Predicted_Rating'})
    
    
    Joined_data=Joined_data.toPandas()
    titles=[]
    for i in df['movieId']:
        title=Joined_data[Joined_data['movieId']==i]['title'].iloc[0]
        titles.append(title)
   
   
    df['Top 10 titles for '+str(e.get())]=titles
    
    #text_area.insert(END,title_data)
    pt = Table(frame,dataframe=df)
    pt.show()   
    
   
    
                
frame2 = Frame(master=master)
frame2.pack(side='right', fill='both', expand=True)  
             

# object of tkinter
# and background set for white


master.resizable(True,True)
master.configure(bg='black')
 
# Variable Classes in tkinter

# Creating label for each information
# name using widget Label
frame3=  Frame(master=master)
frame3.pack(side='left', fill='both', expand=True)
Label(frame3, text="Enter User ID:",
      bg="white").pack()
e = Entry(frame3,)
e.pack(fill=tk.X)

b = tk.Button(frame3, text="Show titles", command=get_user_movies,highlightbackground='grey')
b.pack(fill=tk.X)
b2 = Button(frame3, text="Movies watched", command=get_user_movie_count,highlightbackground='grey')
b2.pack(fill=tk.X)

b3 = Button(frame3, text="Genres watched", command=get_user_genre_count,highlightbackground='grey')
b3.pack(fill=tk.X)
b4 = Button(frame3, text="Genre ratings", command=get_genre_rating,highlightbackground='grey')
b4.pack(fill=tk.X)
b5 = Button(frame3, text="Genre watch count", command=get_genre_watched,highlightbackground='grey')
b5.pack(fill=tk.X)
b6 = Button(frame3, text="Favourite genre based on rating", command=get_fav_genre_rating,highlightbackground='grey')
b6.pack(fill=tk.X)
b7 = Button(frame3, text="Favourite genre based most viewed", command=get_fav_genre_watched,highlightbackground='grey')
b7.pack(fill=tk.X)
b8 = Button(frame3, text="Movie recommendation ", command=movieRecommendation,highlightbackground='grey')
b8.pack(fill=tk.X)
Label(frame2, text="Enter Movie ID:",
      bg="white").pack()
e2 = Entry(frame2)
e2.pack(fill=tk.X)
b9 = Button(frame2, text="Watch count", command=get_Movie_user_watched_byID,highlightbackground='grey')
b9.pack(fill=tk.X)
b10= Button(frame2, text="Rating", command=get_Rating_byID,highlightbackground='grey')
b10.pack(fill=tk.X)

Label(frame2, text="Enter Movie's Name:",
      bg="white").pack()
e3 = Entry(frame2)
e3.pack(fill=tk.X)
b11 = Button(frame2, text="Watch count", command=get_Movie_user_watched,highlightbackground='grey')
b11.pack(fill=tk.X)
b12= Button(frame2, text="Rating", command=get_Rating,highlightbackground='grey')
b12.pack(fill=tk.X)

Label(frame2, text="Enter Genre Name:",
      bg="white").pack()
e4 = Entry(frame2)
e4.pack(fill=tk.X)
b13 = Button(frame2, text="Movie list", command=get_Movie_byGenre,highlightbackground='grey')
b13.pack(fill=tk.X)
Label(frame2, text="Enter Year or n:",
      bg="white").pack()

e5 = Entry(frame2)
e5.pack(fill=tk.X)
b14 = Button(frame2, text="Year Movie list", command=get_Movie_byYear,highlightbackground='grey')
b14.pack(fill=tk.X)



b15 = Button(frame2, text="Top n Rated", command=get_TopMovies,highlightbackground='grey')
b15.pack(fill=tk.X)
b16 = Button(frame2, text="Top n Watched ", command=get_MostWatchedMovies,highlightbackground='grey')
b16.pack(fill=tk.X)

Label(frame3, text="Enter User1:",
      bg="white").pack()
e7 = Entry(frame3)
e7.pack(fill=tk.X)

Label(frame3, text="Enter User2:",
      bg="white").pack()
e8 = Entry(frame3)
e8.pack(fill=tk.X)
b17 = Button(frame3, text="Compare", command=get_UserComparison,highlightbackground='grey')
b17.pack(fill=tk.X)
b18 = Button(frame3, text="Clustered Users according to taste (cluster baskets)", command=get_Cluster,highlightbackground='grey')
b18.pack(fill=tk.X)

b19 = Button(frame3, text="Clustered Users according to taste (UserID wise) ", command=get_UserIDwise_Cluster,highlightbackground='grey')
b19.pack(fill=tk.X)
# Creating label for class variable
# name using widget Entry
frame.configure(bg='grey')
frame2.configure(bg='grey')
frame3.configure(bg='grey')
frame.pack(expand=True)



master.mainloop()