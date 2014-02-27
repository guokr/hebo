(ns hebo.action
  (:use [clojure.tools.logging :only [info error]]
        [hebo.redis]
        [hebo.util :only [get-next-tasks parse-int]])
  (:use [clojure.string :only (join split)])
  (:require [taoensso.carmine :as car]))

(defn fire-next [taskname param]
  (info "fire the nextasks of" taskname param )  
  (let [next-tasks (get-next-tasks taskname)]
    (if (> (count next-tasks) 0)
      (let [task-param-len (redis (assemble-redis-cmd car/llen (map #(vector (str "task:" % ":param")) next-tasks)))
            task-len-map (if (= 1 (count next-tasks)) {(first next-tasks) task-param-len} (zipmap next-tasks task-param-len))
            joint-param (join "-" param)
            task-history (redis (car/smembers (str "task:" taskname ":history")))
            trunc-map {1 4, 2 7, 3 10, 4 13}] ;2014-01-22-12-00-00
        (if (> (count task-history) 0)
          (doseq [[task param-length] task-len-map]
            (let [fine-granu-history (set (map #(subs % 0 (get trunc-map (parse-int param-length))) task-history))
                  begin-items (filter #(or (.contains % joint-param) (.contains joint-param %)) fine-granu-history)]
              (if (> (count begin-items) 0) 
                (redis (assemble-redis-cmd car/sadd (map #(vector (str "begin:" task) %) begin-items) ) )))))))))

