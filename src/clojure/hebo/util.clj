(ns hebo.util
  (:use [clojure.string :only [split join]]
        [clj-time.format]
        [clj-time.coerce :only [to-long from-long]]
        [clj-time.core :exclude [second extend]]
        [cascalog.api]
        [hebo.redis])
    (:require [taoensso.carmine :as car]))

;------functions below used in macro deftask----------

(def dt-formatter (formatter "yyyy-MM-dd'T'HH:mm:ssZ" (default-time-zone)))

(defn trunc-datetime [datetime level]
  (let [dt (parse dt-formatter datetime)]
    (cond
      (= :yearly level) (unparse (formatter "YYYY" (default-time-zone)) dt)
      (= :monthly level) (unparse (formatter "YYYY-MM" (default-time-zone)) dt)
      (= :daily level) (unparse (formatter "YYYY-MM-dd" (default-time-zone)) dt)
      (= :hourly level) (unparse (formatter "YYYY-MM-dd-HH" (default-time-zone)) dt)
      (= :minutely level) (unparse (formatter "YYYY-MM-dd-HH-mm" (default-time-zone)) dt))))

(def granu-level {"yearly" 4 "monthly" 3 "daily" 2 "hourly" 1})

(defn granu-compare [granu1 granu2]
  " > return 1;  =  return 0 ; < return -1 "
  (if (empty? granu2)
    0
  (- (get granu-level granu1) (get granu-level granu2))))

(defn get-fine-granu
  ([granu] granu)
  ([granu1 granu2] (if (<  (granu-compare granu1 granu2) 0) granu1 granu2))  ;granu1=daily granu2=hourly --> hourly
  ([granu1 granu2 & more] (reduce get-fine-granu (get-fine-granu granu1 granu2) more)))

(defn get-coarse-granu
  ([granu] granu)
  ([granu1 granu2] (if (>  (granu-compare granu1 granu2) 0) granu1 granu2))  ;granu1=daily granu2=hourly --> daily
  ([granu1 granu2 & more] (reduce get-fine-granu (get-fine-granu granu1 granu2) more)))

(defn get-all-tasks []
  ;get all tasks from redis based on task's cron key
  (let [tasks (redis (car/keys "task:*:cron"))]
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

(defn parseInt [str-or-nil]
  (if (nil? str-or-nil)
    0 (Integer. str-or-nil)))

(defn str2datetime [sdt]
  (parse dt-formatter sdt))

(defn datetime2timestamp [dt]
  (/
    (to-long dt)
    1000))

(defn str2timestamp [sdt]
  (let [dt (str2datetime sdt)]
    (datetime2timestamp dt)))

(defn str2minutets [sdt]
  (let [dt (str2datetime sdt)
        dt-year (year dt)
        dt-month (month dt)
        dt-day (day dt)
        dt-hour (hour dt)
        dt-minute (minute dt)]
    (datetime2timestamp (from-time-zone (date-time dt-year dt-month dt-day dt-hour dt-minute) (default-time-zone)))))

(defn str2hourts [sdt]
  (let [dt (str2datetime sdt)
        dt-year (year dt)
        dt-month (month dt)
        dt-day (day dt)
        dt-hour (hour dt)]
    (datetime2timestamp (from-time-zone (date-time dt-year dt-month dt-day dt-hour) (default-time-zone)))))

(defn str2dayts [sdt]
  (let [dt (str2datetime sdt)
        dt-year (year dt)
        dt-month (month dt)
        dt-day (day dt)]
    (datetime2timestamp (from-time-zone (date-time dt-year dt-month dt-day) (default-time-zone)))))

(defn str2monthts [sdt]
  (let [dt (str2datetime sdt)
        dt-year (year dt)
        dt-month (month dt)]
    (datetime2timestamp (from-time-zone (date-time dt-year dt-month) (default-time-zone)))))

(defn str2yearts [sdt]
  (let [dt (str2datetime sdt)
        dt-year (year dt)]
        (datetime2timestamp (from-time-zone (date-time dt-year) (default-time-zone)))))

(defn granularity [granu]
  (cond
    (= granu :itemized)
      str2timestamp
    (= granu :minutely)
      str2minutets
    (= granu :hourly)
      str2hourts
    (= granu :daily)
      str2dayts
    (= granu :monthly)
      str2monthts
    (= granu :yearly)
      str2yearts
      ))

(defmacro push-list [task-name array]
  `(map #(list 'car/rpush ~task-name (name %) ) ~array))

(defmacro push-set [task-name array]
  `(map #(list 'car/sadd ~task-name (name %) ) ~array))

(defmacro push-hash [task-name hash]
  `(map #(list 'car/hset ~task-name (name (key %))  (name (val %)) ) ~hash))

(defn addtask [taskname taskinfo]
  (eval
    (cons 'hebo.redis/redis
      (into
        (into
          (push-list (str "task:" taskname ":param") (:param taskinfo))
          (push-set (str "task:" taskname ":refs") (:refs taskinfo)))
        (list
          `(car/set (str "task:" ~taskname ":desc") (:desc ~taskinfo))
          `(car/set (str "task:" ~taskname ":cron") (:cron ~taskinfo))
          `(car/set (str "task:" ~taskname ":pretask") (:pretask ~taskinfo))
          `(car/hset (str "task:" ~taskname ":output") "fs" (name (:fs (:output ~taskinfo))))
          `(car/hset (str "task:" ~taskname ":output") "base" (:base (:output ~taskinfo)))
          `(car/hset (str "task:" ~taskname ":output") "granularity" (name (:granularity (:output ~taskinfo))))
          `(car/hset (str "task:" ~taskname ":output") "delimiter" (name (:delimiter (:output ~taskinfo))))
          `(car/hset (str "task:" ~taskname ":data") "granularity" (name (:granularity (:data ~taskinfo))))
          )))))

(def cli-opts
  [["-c" "--conf path" "path of hebo.yaml"]
   ["-h" "--help"]])

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))
