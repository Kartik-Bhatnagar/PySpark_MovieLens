# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ## Overview
# MAGIC 
# MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
# MAGIC 
# MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/users-1.dat"
file_type = "dat"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

sc

# COMMAND ----------

import pandas as pd

# COMMAND ----------

user_rdd = sc.textFile("/FileStore/tables/users-1.dat")
print(user_rdd.take(2))

# COMMAND ----------

movie_rdd = sc.textFile("/FileStore/tables/movies.dat")
print(movie_rdd.take(2))

# COMMAND ----------

rat_rdd = sc.textFile("/FileStore/shared_uploads/kartikbhatnagar15@gmail.com/rating_-1.dat")
print("userid::movieId::Rating::Time")
print(rat_rdd.take(2)) 

# COMMAND ----------

# MAGIC %sh git clone https://github.com/srjsunny/Movie_Lens_Data-Analysis/tree/master/dataSet --depth 1 --branch=master /dbfs/FileStore/tables/

# COMMAND ----------

# DBTITLE 1,KPI 1 [Top 10 most watched movie]
map1 =movie_rdd.map(lambda x : x.split("::")).map(lambda x : (x[0],x[1]))
map1.takeOrdered(6)

# COMMAND ----------

#(movie_id , 1)
map2 = rat_rdd.map(lambda x:(x.split("::")[1],1)).reduceByKey(lambda x,y : x+y)
print("MovieId , Totalcount")
map2.take(6)

# COMMAND ----------


join1 = map2.leftOuterJoin(map1)
print("[movieId,(Total_Count,movie_name)]")
(join1.take(6))


# COMMAND ----------

# DBTITLE 1,KPI1:Output [Top 10 most watched movie]
#now we have both rdd of rating and movie joined we will make (movieName,1) tuple
reducer1  = join1.map(lambda x : (x[1][1],x[1][0]) )
top10Reducer = reducer1.takeOrdered(10,lambda y:-y[1])
top10Reducer
#Note the orignal data contained 10 Lakh ratings,here due to storage constraint only 2 lakhs ratings are taken

# COMMAND ----------

top10Movies = pd.DataFrame(top10Reducer,columns=["Movie Name","No. of Ratings"])
top10Movies.index =(top10Movies.index)+1
top10Movies

# COMMAND ----------

# DBTITLE 1,KPI 2 : Top 20 rated movies provided it's atleast watched/rated by 40 users
#from kpi 1 in map2 we have count of  ratings for each movie , now we want sum of ratings and then we will join it together to findthe average
map3 = rat_rdd.map(lambda x:(x.split("::"))).map(lambda y:( y[1],int(y[2]))).reduceByKey(lambda x,y : x+y)
print("MovieId , RatingCount")
map3.take(6)

# COMMAND ----------

join2 = map3.leftOuterJoin(map2)
print("movieId , (Rating Sum ,RatingCount)")
join2.take(6)


# COMMAND ----------

#now we need to take only those   movies for which atleat 40 ratings are done
atleat40 = join2.filter(lambda y : (y[1][1] > 39))
atleat40.take(6)

# COMMAND ----------

#making (movieId, avg.Rating) tuple
avg_rating = atleat40.map(lambda row : (row[0],round(float(row[1][0]/row[1][1]) ,5)))#reducer 3 in
avg_rating.take(6)

# COMMAND ----------

# DBTITLE 1,KPI 2 Output [Top 20 Movies by ratings (at-least rated by 40 people)] 
movie_Avg_rat = avg_rating.leftOuterJoin(map1).map(lambda y:(y[1][1],y[1][0]))
print("MovieId,(Avg. rating,Movie Name)")
top20=movie_Avg_rat.takeOrdered(20 , lambda y: -y[1])
top20

# COMMAND ----------

#For better Visualisation lets view it in Pandas data frame
kpi2 = pd.DataFrame(top20, columns=["Movie","Average Ratings"])
kpi2.index = (kpi2.index)+1
kpi2

# COMMAND ----------

# DBTITLE 1,KPI 3  genres ranked by Average Rating, for each profession and age group. The age groups to be considered are: 18-35, 36-50 and 50+.
map4 =movie_rdd.map(lambda x : x.split("::")).map(lambda x : (x[0],x[2]))
print("(movieId , Genere)")
map4.take(6) 

# COMMAND ----------

map5 =user_rdd.map(lambda x : x.split("::")).map(lambda x : (x[0],x[2],x[3]))
print("(userId , Age, Occupation)")
map5.take(6) 

# COMMAND ----------

map6 =rat_rdd.map(lambda x : x.split("::")).map(lambda x : (x[0],x[1]+":"+x[2]))
print("(UserId,movieId : Rating)") #instaed of 3 tuple element we kept 2 bcoz the problem was coming during joins
map6.take(6) 

# COMMAND ----------

def age_group(row):
  
  age= row[1]
  if( int(age) > 50):
    tup = (row[0],"50+ :"+row[2])
    return(tup)
  elif(int(age) <= 50 and int(age) > 35):
    tup = (row[0],"36 to 50 :"+row[2])
    return(tup)
  else:
    tup = (row[0],"18 to 35 :"+row[2])
    return(tup)
  
  
  
  

# COMMAND ----------

map7 =map5.map(age_group)
print("(UserId,AgeGroup,Occupation)")
map7.take(6)

# COMMAND ----------

kp3_join1 = map7.leftOuterJoin(map6)
print("userId,(AgeGroup : Occupation , movieId:Rating)")
kp3_join1.take(6)
#join1 is done for just experimental sake ,itis not used further down

# COMMAND ----------

map8 =rat_rdd.map(lambda x : x.split("::")).map(lambda x : (x[1],x[0]+":"+x[2]))
print("(movieId ,UserId : Rating)") #instaed of 3 tuple element we kept 2 bcoz the problem was coming during joins
map8.take(6) 

# COMMAND ----------

usr_gnr = map8.leftOuterJoin(map4).map(lambda y:((y[1][0]).split(":")[0],((y[1][1]+":"+(y[1][0]).split(":")[1]))))
print("userId,(Genre : Rating")
usr_gnr.take(6)

# COMMAND ----------

kpi3_join2 = usr_gnr.leftOuterJoin(map7)
print("UserId,(Genre:Rating  ,  AgeGroup:Occupation)")
kpi3_join2.takeSample(6,10)


# COMMAND ----------

#now we just want a tuple like (AgeGroup:Occpation:Genre  , Rating)
agor_sum = kpi3_join2.map(lambda y : ( y[1][1]+":"+((y[1][0]).split(":"))[0]   ,        float( (y[1][0].split(":"))[1] ) ))
print("(AgeGroup:Occpation:Genre  , Rating)")
agor_sum.take(6)

# COMMAND ----------

sum = agor.reduceByKey(lambda x,y : x+y)
sum.take(6)

# COMMAND ----------

agor_count = kpi3_join2.map(lambda y : ( y[1][1]+":"+((y[1][0]).split(":"))[0]   ,   1 ))
print("(AgeGroup:Occpation:Genre  , 1)")
agor_count.take(6)

# COMMAND ----------

count = agor_count.reduceByKey(lambda x,y:x+y)
count.take(6)

# COMMAND ----------

# DBTITLE 1,KPI3 Output [ genres ranked by Average Rating, for each profession and age group. The age groups to be considered are: 18-35, 36-50 and 50+.]
kpi3_join3 = count.leftOuterJoin(sum).map(lambda y :( y[0]  ,  y[1][1]/y[1][0]))
print("AgeGroup : Occupation : Genre  , Avg .rating")
kpi3_join3.take(20)

# COMMAND ----------

# Create a view or table
simple_kpi3 = kpi3_join3.map(lambda y :( ((y[0]).split(":"))[0]  ,  ((y[0]).split(":"))[1]  ,   ((y[0]).split(":"))[2]  ,  round(y[1],4) )  )
kpi3 = simple_kpi3.collect()
kpi3_df =pd.DataFrame(kpi3, columns=["AgeGroup","Occupation","Genre","Avg.Rating"])
kpi3_df.sample(25)
