(ns hebo.action
  (:use [hebo.redis]
        [hebo.util :only [get-next-tasks parseInt]])
  (:use [clojure.string :only (join split)])
  (:require [taoensso.carmine :as car]))

(defn fire-next [taskname param]
  (prn taskname param)
  (let [next-tasks (get-next-tasks taskname)]
    (if (> (count next-tasks) 0)
      (let [task-param-len (eval (cons 'hebo.redis/redis  (for [t next-tasks] `(car/llen (str "task:" ~t ":param")))))
            task-len-map (if (= 1 (count next-tasks)) {(first next-tasks) task-param-len} (zipmap next-tasks task-param-len))
            joint-param (join "-" param)
            job-running-statue (redis (car/smembers (str "job:" taskname ":" joint-param)))
            trunc-map {1 4, 2 7, 3 10, 4 13}]
        (prn task-len-map)   
        (doseq [[k v] task-len-map]
          (let [status (set (map #(subs % 0 (get trunc-map v)) job-running-statue))
                begin-items (filter #(or (.contains % joint-param) (.contains joint-param %)) status)]
            (if (> (count begin-items) 0) 
              (eval (cons 'hebo.redis/redis  (for [t begin-items] `(car/sadd (str "begin:" ~k) ~t)))))))))))

