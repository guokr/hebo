(ns hebo.util
  (:use [clojure.string :only [split join]]
        [clojure.tools.logging :only (info error)]
        [clj-time.format]
        [clj-time.coerce :only [to-long from-long]]
        [clj-time.core :exclude [second extend]]
        [cascalog.api]
        [hebo.redis]
        [hebo.xml])
    (:require [taoensso.carmine :as car]))
;------functions below used in macro deftask----------

(def dt-formatter (formatter "yyyy-MM-dd'T'HH:mm:ssZ" (default-time-zone)))

(defn trunc-datetime [datetime granu]
  (let [dt (parse dt-formatter datetime)]
    (case granu 
      :yearly   (unparse (formatter "YYYY" (default-time-zone)) dt)
      :monthly  (unparse (formatter "YYYY-MM" (default-time-zone)) dt)
      :daily    (unparse (formatter "YYYY-MM-dd" (default-time-zone)) dt)
      :hourly   (unparse (formatter "YYYY-MM-dd-HH" (default-time-zone)) dt)
      :minutely (unparse (formatter "YYYY-MM-dd-HH-mm" (default-time-zone)) dt))))

(def granu-level {:yearly 1 :monthly 2 :daily 3 :hourly 4 :minutely 5})

(defn granu-compare [granu1 granu2]
  " > return 1;  =  return 0 ; < return -1 "
  (info "granu-compare" granu1 granu2)
  (if (nil? granu2)
    0
  (- (granu1 granu-level) (granu2 granu-level))))

(defn get-fine-granu
  ([granu] granu)
  ([granu1 granu2] (if (<  (granu-compare granu1 granu2) 0) granu1 granu2))  ;granu1=daily granu2=hourly --> hourly
  ([granu1 granu2 & more] (reduce get-fine-granu (get-fine-granu granu1 granu2) more)))

(defn get-coarse-granu
  ([granu] granu)
  ([granu1 granu2] (if (>  (granu-compare granu1 granu2) 0) granu1 granu2))  ;granu1=daily granu2=hourly --> daily
  ([granu1 granu2 & more] (reduce get-coarse-granu (get-coarse-granu granu1 granu2) more)))

(defn get-all-tasks []
  ;get all tasks from redis based on task's output key
  (let [tasks (redis (car/keys "task:*:output"))]
    (if (> (count tasks) 0)
      (map #(subs % (inc (.indexOf % ":")) (.lastIndexOf % ":") ) tasks)
      [])))

(defn get-next-tasks [task]
  (let [all-task (get-all-tasks)]
    (if (> (count all-task) 0)
      (let [all-next-task (eval (cons 'hebo.redis/redis (for [t all-task] `(car/get (str "task:" ~t ":pretask")))))
            task-pretask (zipmap all-task all-next-task)
            right-next (keys (filter #(= task (val %)) task-pretask))]
        (if (nil? right-next)
          []
          right-next))
      [])))

(defn symbolize [keyword-or-str]
  (if (instance? clojure.lang.Keyword keyword-or-str)
    (symbol (name keyword-or-str))
    keyword-or-str))

(defn parameterize [keyword-or-str]
  (if (instance? clojure.lang.Keyword keyword-or-str)
    (symbol (str "?" (name keyword-or-str)))
    keyword-or-str))

(defn symbolize-hash [hash]
  (into {} (map #(vector (first %) (vec (map symbolize (second %)))) hash)))

(defn parse-int [str-or-nil]
  (if (nil? str-or-nil)
    0 (Integer. str-or-nil)))

(defn parse-datetime [sdt]
  (parse dt-formatter sdt))

(defn datetime-to-timestamp [dt]
  (/ 
    (to-long (from-time-zone dt (default-time-zone)))
    1000))

(defn str-to-timestamp [sdt]
  (let [dt (parse-datetime sdt)]
    (datetime-to-timestamp dt))) 

(defn str-to-minute-timestamp [sdt]
  (let [dt (parse-datetime sdt)
        dt-year (year dt)
        dt-month (month dt)
        dt-day (day dt)
        dt-hour (hour dt)
        dt-minute (minute dt)]
    (datetime-to-timestamp (from-time-zone (date-time dt-year dt-month dt-day dt-hour dt-minute) (default-time-zone)))))

(defn str-to-hour-timestamp [sdt]
  (let [dt (parse-datetime sdt)
        dt-year (year dt)
        dt-month (month dt)
        dt-day (day dt)
        dt-hour (hour dt)]
    (datetime-to-timestamp (from-time-zone (date-time dt-year dt-month dt-day dt-hour) (default-time-zone)))))

(defn str-to-day-timestamp [sdt]
  (let [dt (parse-datetime sdt)
        dt-year (year dt)
        dt-month (month dt)
        dt-day (day dt)]
    (datetime-to-timestamp (from-time-zone (date-time dt-year dt-month dt-day) (default-time-zone)))))

(defn str-to-month-timestamp [sdt]
  (let [dt (parse-datetime sdt)
        dt-year (year dt)
        dt-month (month dt)]
    (datetime-to-timestamp (from-time-zone (date-time dt-year dt-month) (default-time-zone)))))

(defn str-to-year-timestamp [sdt]
  (let [dt (parse-datetime sdt)
        dt-year (year dt)]
    (datetime-to-timestamp (from-time-zone (date-time dt-year) (default-time-zone)))))

(defn granularity-is [granu]
  (case granu
    :itemized str-to-timestamp
    :minutely str-to-minute-timestamp
    :hourly   str-to-hour-timestamp
    :daily    str-to-day-timestamp
    :monthly  str-to-month-timestamp
    :yearly   str-to-year-timestamp))
 
(defn addtask [taskname taskinfo]
  (redis
    (car/set (str "task:" taskname ":desc") (:desc taskinfo))
    (car/hset "cron" taskname (:cron taskinfo))
    (car/hset (str "task:" taskname ":output") "fs" (:fs (:output taskinfo)))
    (car/hset (str "task:" taskname ":output") "base" (:base (:output taskinfo)))
    (car/hset (str "task:" taskname ":output") "granularity" (:granularity (:output taskinfo)))
    (car/hset (str "task:" taskname ":output") "delimiter" (:delimiter (:output taskinfo)))
    (car/hset (str "task:" taskname ":data") "granularity" (:granularity (:data taskinfo)))
  
    (let [input (:input taskinfo)]
      (when-not (nil? input))
        (car/hset (str "task:" taskname ":input") "fs" (:fs input))
        (car/hset (str "task:" taskname ":input") "base" (:base input))
        (car/hset (str "task:" taskname ":input") "granularity" (:granularity input))
        (car/hset (str "task:" taskname ":input") "delimiter" (:delimiter input)))
    
    (let [pretask (:pretask taskinfo)]
      (if (not (nil? pretask))
        (redis (car/set (str "task:" taskname ":pretask") pretask))))
  
    (let [refs (:refs taskinfo)]
      (if (not (nil? refs))
        (assemble-redis-cmd 
          car/sadd (map #(vector (str "task:" taskname ":refs") %) refs))))
        
    (assemble-redis-cmd
      car/rpush (map #(vector (str "task:" taskname ":param") %) (:param taskinfo)))))

(def cli-opts
   [["-h" "--help"]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))
