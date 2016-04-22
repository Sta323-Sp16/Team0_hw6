Sys.setenv(HADOOP_CONF="/data/hadoop/etc/hadoop")
Sys.setenv(YARN_CONF="/data/hadoop/etc/hadoop")
Sys.setenv(SPARK_HOME="/data/hadoop/spark/")
Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.99-2.6.5.0.el7_2.x86_64/")

.libPaths(c(file.path(Sys.getenv("SPARK_HOME"), "R/lib"), .libPaths()))
library(SparkR)
library(magrittr)


## Starting

sc = sparkR.init("local[4]")
sqlContext = sparkRSQL.init(sc)


## Existing Data Frame

df = createDataFrame(sqlContext, iris)
summarize(df, mean(df$Sepal_Length)) %>% collect()

model = glm(Sepal_Length~Sepal_Width, data=df, family="gaussian")
summary(model)

## json 

j = read.json(sqlContext, "hdfs://localhost:8020/data/reddit/small.json")
j = read.df(sqlContext, "hdfs://localhost:8020/data/reddit/small.json", source="json")


res = group_by(j,"author") %>% count()
res = arrange(res, desc(res$count))
res = collect(res)

registerTempTable(j, "reddit")
res = sql(sqlContext, "SELECT author, COUNT(*) AS count FROM reddit GROUP BY author ORDER BY count DESC")
res = collect(res)


## Text file

t = read.text(sqlContext, "hdfs://localhost:8020/data/shakespeare/hamlet.txt")

res = filter(t, trim(t$value) != "")
res = mutate(res, clean = regexp_replace(res$value, "[0-9,\\-\"'`.?()’—!⌈]","") %>% lower())

res = selectExpr(res, "split(clean,' ') AS words")
res = select(res, explode(res$words))
res$col = trim(res$col)
res = group_by(res, "col") %>% count()
res = arrange(res, desc(res$count))



## Time Stamps

j = read.json(sqlContext, "hdfs://localhost:8020/data/reddit/large.json")

res = select(j, j$subreddit, j$created_utc) 
res = mutate(res, created = from_unixtime(res$created_utc)) %>%
      mutate(., month=month(.$created), wday=date_format(.$created,"E")) 
res = group_by(res, res$month, res$subreddit) %>% count()

res_jan = filter(res, res$month == 1) %>% arrange(., desc(.$count)) %>% head(n=25)
res_feb = filter(res, res$month == 2) %>% arrange(., desc(.$count)) %>% head(n=25)
res_mar = filter(res, res$month == 3) %>% arrange(., desc(.$count)) %>% head(n=25)

save(res_jan,res_feb,res_mar, file="task1.Rdata")



## Stopping

sparkR.stop() # Stop sparkR
