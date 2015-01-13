hebo
=====

A framework to develop Hadoop data processing tasks and manage their dependency in runtime.

* A dataflow scheduler based on cascalog for Hadoop tasks dependent each other.
* A DSL with an elegant way to write a single Hadoop data processing task.

Design concepts
----------------

In practice, time series data can be managed in different time-granularities, such as yearly, monthly, daily or hourly depends on your requirements. If data are organized in some kind of file system, there exist 3 kind of granularities for a data processing task:

 * input granularity: the granularity level of the whole data inside the input file
 * output granularity: the granularity level of the whole data inside the output file
 * data granularity: the granularity level of one line of data inside the output file

Considering the runtime dependency between tasks, we really need a scheduler to handle all the tasks.

Hebo is just such a scheduler solution combined with a DSL to simplify the development.

Example
--------

Suppose we have 3 tasks - A,B,C, and they follows below relationship:

| taskname |   dependency | input-granu | output-granu | data-granu |      params    |
|:--------:|:------------:|:-----------:|:------------:|:----------:|:--------------:|
| A        |      null    | daily       | daily        | hourly     |[year month day]|
| B        |        A     | daily       | monthly      | daily      |[year month]    |
| C        |        B     | monthly     | daily        | daily      |[year month]    |

And suppose an example data thunk dated with 2014-02-20T23:55:05+0800 which are going throughout this dataflow.

1. A dependent no other tasks inside the system, so A can execute directly. The input granularity of A is daily, so its parameter may contains 2014 02 30; then the data granularity of A is hourly, so the data thunk of 2014-02-20T23:55:05+0800 will be truncated into 2014-02-20T23:00:00+0800; and the output granularity is daily, so finaly A will aggragate all the data thunk by hourly level inside 2014-02-20, and result in one output file marked with 2014-02-20. Inside this file, it contains items marked with 2014-02-20T23:00:00+0800, 2014-02-20T22:00:00+0800, 2014-02-20T21:00:00+0800 and so on. 

2. Tasks of B can't be executed until corresponding tasks of A is finished. Obviously, the input granularity of B should be equal with output granularity of A. Because output granularity of B is monthly, it require all the relied  tasks of A such as 2014-02-01 ... 2014-02-28 to be ready, and then task B will be invoked, and result in file 2014-02 with daily items.

3. C can't be executed until B is finished. This task will split the monthly data from B, and result in files 2014-02-01 ... 2014-02-28 with daily items.

## Getting started

### Requirements
  * add [Leiningen](http://leiningen.org/) to your path
  * add %HADOOP_HOME%/bin to your path
  * add %HEBO_HOME%/bin to your path

Here %HADOOP_HOME% %HEBO_HOME% refer the root folder of your hadoop and hebo   

### Demo code
```
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
```
```
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
                     
```
Codes above demonstrate the skeleton of a task. your business related code should be placed in :query entry.

