This project is a big data application based on Apache Spark
and Livy, which is a REST interface. It applies various data
analytical methods to analyze and profile the input data.
Then it will clean the dataset.

The core of the application is data processing. We apply ALS,
which is a classic meachine learning method in MLlib library
to train a model based on the dataset. And add a new user with
some rating recordings. Fianlly, we will predict what movies
the new user may like using our trained model and recommend 
some movies based on the prediction. 

What's more, making our application integrated and a complete
pipeline, we build a web service on top of our data processing
phase based on Livy, which is a REST interface framework.

There are basically three parts of this application:

1. Data profiling, cleaning, and analyzing.
2. Use a smaller dataset to choose best parameter for our model
3. Start the server, train the model with large dataset and 
   recommend new user movies based on our web service.

Here are the steps to start a server and run the application.
============================================

Firstly, start a server on Dumbo login node
--------------------------------------------
# Copy the software
```
mkdir ~/livy-0.6.0
cp -r /share/apps/livy/0.6.0/* ~/livy-0.6.0
```

# Start the server
```
export SPARK_HOME=/share/apps/spark/spark-2.3.0-bin-hadoop2.6
export HADOOP_CONF_DIR=/etc/hadoop/conf.cloudera.yarn
cd ~/livy-0.6.0
./bin/livy-server  # start a server listening on the default port 8998
```


On my laptop
==========
Open a new terminal while within nyu, run this to enable ssh port forwarding:
```
$ ssh -L 2345:localhost:8998 <netid>@128.122.215.52
```
# IP for two login nodes
1. login-1-1:  128.122.215.51
2. login-2-1:  128.122.215.52

Before typing the application code, you need to install a python library,
which supports Livy:
```
pip install requests
```

Then in another terminal on the laptop, these running fine for me in a python session.
You need to input the command sentence by sentence in file "application_code.py".

The results which contain movies we recommend will save automatically as files
in HDFS.

You could check it using command:
```
hdfs dfs -cat <filename> on dumbo
```

Or you could move it to dumbo node and to your local storage:
```
hdfs dfs -get <from_path> <to_path>
scp [-r] <from_path> <to_path>
```
