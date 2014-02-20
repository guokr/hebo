(defproject com.guokr/hebo "0.0.1"
  :description "hebo: a dataflow scheduler based on cascalog for hadoop tasks"

  :aot :all
  :main hebo.main

  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]

  :repositories {"conjars" "http://conjars.org/repo"}

  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-time "0.5.0"]
                 [org.clojure/data.xml "0.0.7"]
                 [log4j/log4j "1.2.17"]
                 [org.slf4j/slf4j-api "1.7.2"]
                 [org.slf4j/slf4j-log4j12 "1.7.2"]
                 [com.draines/postal "1.9.0"]
                 [org.clojure/tools.logging "0.2.3"]
                 [clojurewerkz/quartzite "1.1.0"]
                 [server-socket "1.0.0"]
                 [com.taoensso/carmine "2.4.4"]
                 [com.google.guava/guava-tests-jdk5 "16.0"]
                 [org.mapdb/mapdb "0.9.9"]
                 [cascalog "2.0.0"]
                 [cascalog/cascalog-more-taps "2.0.0"]
                 [cascading/cascading-core "2.2.0"]
                 [cascading/cascading-hadoop "2.2.0"]
                 [cascading/cascading-local "2.2.0"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.apache.hadoop/hadoop-core "1.1.2"]])
