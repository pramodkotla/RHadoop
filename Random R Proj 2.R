rm(list=ls())

#Team: Random R
#Date: 11/28/2016
#Proj: R and Hadoop

############################ Environment Setup #########################################
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-mapreduce/hadoopstreaming-
2.5.0-cdh5.5.0.jar")
library(rhdfs)
hdfs.init()
############################ Basic Hadoop Operations####################################

# RHDFS
# Put File from local file system to hdfs
hdfs.put('/home/cloudera/R/randomr.txt','./')
hdfs.ls('./')
# Copying File within hdfs using R
hdfs.copy('randomr.txt','randomrcnt.txt')
hdfs.ls('./')
# Moving file within HDFS
hdfs.move('randomrcnt.txt','./data/randomrcnt.txt')
hdfs.ls('./data')
# Removing files and folders from HDFS
hdfs.rm('./data')
# Loading file from HDFS to Local file system
hdfs.get('randomr.txt','/home/cloudera/randomtest.txt')
# Renaming existing file in HDFS
hdfs.rename('randomr.txt','randomname.txt')
# Changing file permissions in HDFS to 777 i.e. all can read, write and execute the file
hdfs.chmod('randomname.txt',permissions='777')
# Checking info to verify above command 
hdfs.file.info('randomname.txt')

####################################### Data Read/Write ##############################

# creating a write type of object along with HDFS path of file
f = hdfs.file("women.txt","w")
data(women)
# This function will write women data set to file path specified in object f
hdfs.write(women,f)
# Closing f object
hdfs.close(f)

# creating read type of object along with hdfs path
f = hdfs.file("women.txt","r")
# Hdfs.read gives us serialized read object
dfserialized = hdfs.read(f)
# We can recover the data frame by unserializing the serialized df
df = unserialize(dfserialized)
df
# Closing the object
hdfs.close(f)

###################################### Data Manipulation ###############################

library(plyrmr)
library(car)
library(rmr2)

plyrmr.options(backend = "local")

# Performing manuipulation operations on Salaries data set
data(Salaries)
# Reading the data frame
Salaries = data.frame(Salaries)
# Example of normal filtering as we studied using dplyr
where(Salaries,salary>=100000)

# Performing data manipulation using map reduce
# Defining path in distributed file system for Salaries data frame
saldata = to.dfs(data.frame(Salaries),output= '/tmp/Salaries2')
saldata
# Performing operations on input file from hdfs
input(saldata) %|% where(salary>=100000)
input(saldata) %|% transmute(mean(salary))
input(saldata) %|% group(sex) %|% transmute(mean(salary))
# Sampling data to get top 10 rows
sample(input(saldata),n=10)

############################## Performance Test #############################################
# Calculating time to calculate cube of a series using normal function of R - sapply
a.time = proc.time()
small.ints2 = 1:100000
result.normal = sapply(small.ints2,function(x) x^3)
proc.time()- a.time

# Calculating time to perform same calculation using map reduce
b.time = proc.time()
small.ints = to.dfs(1:100000)
result = mapreduce(input=small.ints,map=function(k,v) cbind(v,v^3))
proc.time()-b.time


################################### Modeling ############################################
data(women)
X = matrix(women$height)
y = matrix(women$weight)
model = lm(y~X)
summary(model)

# Using map reduce
Sys.setenv(HADOOP_CMD="/usr/bin/hadoop")
Sys.setenv(HADOOP_STREAMING="/usr/lib/hadoop-mapreduce/hadoop->
streaming-2.5.0-cdh5.5.0.jar")
library(rmr2)

# Taking regressor and intercept in X for linear modelling coeff.
X = as.matrix(cbind((women$height),as.vector(rep(1,nrow(women)))))
# Adding index to identify row of X matrix 
X.index = to.dfs(cbind(1:nrow(X), X))
# Taking target variable in y matrix
y = as.matrix(women$weight)

# Defining reducer function that here will sum the key value pairs implemented by mapper function 

Reducer = function(., YY)
keyval(1, list(Reduce('+', YY)))

# Now we know beta matrix = XTX * XTY

# To compute XTX and XTY is a big data problem 
# Calculating beta is NOT!

# Calculating XTX 
XtX =
   values(
     from.dfs(
       mapreduce(
         input = X.index,
        map =
           function(., Xi) {
             Xi = Xi[,-1]
             keyval(1, list(t(Xi) %*% Xi))},
         reduce = Reducer,
         combine = TRUE)))[[1]]

# Calculating XTY
Xty =
   values(
     from.dfs(
       mapreduce(
       input = X.index,
         map = function(., Xi) {
           yi = y[Xi[,1],]
           Xi = Xi[,-1]
           keyval(1, list(t(Xi) %*% yi))},
         reduce = Reducer,
         combine = TRUE)))[[1]]

# Solving XtX and XtY in R using solve function inorder to obtain Beta matrix

solve(XtX,Xty)
################################### Modeling Multiple X #############################################
# Follow the same steps as shown in previous example to observe beta matrix for Boston data set.

library(MASS)
data(Boston)
View(Boston)
X = as.matrix(Boston[,-14])
y = matrix(Boston$medv)
model = lm(y~X)
summary(model)

X = as.matrix(cbind(Boston[,-14],as.vector(rep(1,nrow(Boston)))))
y = matrix(Boston$medv)
X.index = to.dfs(cbind(1:nrow(X), X))

Reducer = function(., YY)
  keyval(1, list(Reduce('+', YY)))

XtX =
  values(
    from.dfs(
      mapreduce(
        input = X.index,
        map =
          function(., Xi) {
            Xi = Xi[,-1]
            keyval(1, list(t(Xi) %*% Xi))},
        reduce = Reducer,
        combine = TRUE)))[[1]]

Xty =
  values(
    from.dfs(
      mapreduce(
        input = X.index,
        map = function(., Xi) {
          yi = y[Xi[,1],]
          Xi = Xi[,-1]
          keyval(1, list(t(Xi) %*% yi))},
        reduce = Reducer,
        combine = TRUE)))[[1]]

solve(XtX,Xty)
