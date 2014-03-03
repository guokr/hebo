hebo
====

A dataflow scheduler based on cascalog for hadoop tasks

## Core Concepts
Data here I use are continuous and vitally time-based,typically web logs.
This data can come in yearly,monthly or even daily and hourly depends on your requirements of your application,here I use granularity to distinguish them,and provide you a elegent way to write codes among dataflow upon complex variety of data granularity.
Suppose you have 3 task A,B,C,and they have fllowing restriction:
--------------------------------------------------------
taskname     relies     input-granu  output-granu  data-granu      
A             --         daily         daily        hourly
B             A          daily         monthly      daily
C             B          monthly       daily        daily
--------------------------------------------------------
Now I will show you changes of data through this workflow with a example having 2014-02-20T23:55:05+0800 properties.
1.A repies no tasks,so A can execute directly.the input-granu of A is daily,so it parameter may contains 2014 02 30,then data-granu of A is hourly,so 2014-02-20T23:55:05+0800 will be changed to 2014-02-20T23:00:00+0800,at last the output-granu is daily,so it will gather all data of 2014-02-20 into a file.So final file contains 2014-02-20T23:00:00+0800, 2014-02-20T22:00:00+0800, 2014-02-20T21:00:00+0800 and so on. 
2.B can't be executed until A is finished.the input-granu of B should be equal output-granu of A,because output-granu of B is monthly,it require the total days of the exact month,in another way,files such as 2014-02-20,2014-02-21,2014-02-20 will feed B,therefore the param of B may contain 2014 02,and 2014-02-20T23:00:00+0800 changed to 2014-02-20T00:00:00+0800 according to data-granu.
3.C can't be executed until B is finished.the input-granu of C should be equal output-granu of B,becasue output-granu of C is daily,is will divided monthly input file into multiple daily files, 2014-02-20T00:00:00+0800 remains the same.

## Getting started

### Requirements
  * add [Leiningen](http://leiningen.org/) to your path
  * add %HADOOP_HOME%/bin to your path
  * add %HEBO_HOME%/bin to your path

Here %HADOOP_HOME% %HEBO_HOME% refer the root folder of your hadoop and hebo   

### Demo code
  (deftask A
    :desc    "desc of A"
    :cron    "0 12/45 * * * ?"
    :param   [:year :month :day]
    :data    {:granularity :hourly}
    :output  { :fs :hdfs :base "/path/to/output/of/A" :granularity :daily :delimiter "\t"}
    :query   (fn [fn-fgranu fn-dgranu timestamp source]
                (<- 
                   [?hourly ?result]
                   (source ?datetime ?line)
                     (fn-dgranu ?datetime :> ?hourly)
                     (process ?line :> ?result))))

(deftask B
    :desc    "desc of B"
    :cron    "0 12/45 * * * ?"
    :param   [:year :month]
    :data    {:granularity :daily}
    :output  { :fs :hdfs :base "/path/to/output/of/B" :granularity :daily :delimiter "\t"}
    :query   (fn [fn-fgranu fn-dgranu timestamp source]
                (<- 
                   [?hourly ?result]
                   (source ?hourly ?line)
                     (fn-dgranu ?hourly :> ?daily)
                     (process ?line :> ?result))))
                     
above codes demostrate the skeleton frame of task. your business related code should be placed in :query entry

