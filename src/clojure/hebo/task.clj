(ns hebo.task
  (:use [clojure.java.io]
        [clojure.string :only (join split trim join)]
        [clojure.java.shell :only [sh]]
        [clojure.stacktrace :only [print-stack-trace root-cause]]
        [clojure.tools.logging :only (info error)]
        [cascalog.api]
        [cascalog.more-taps :only (hfs-delimited)]
        [clj-time.coerce :only [to-long from-long]]
        [clj-time.core :exclude [second extend]]
        [clj-time.format]
        [clojure.tools.cli :only [parse-opts]]
        [hebo.redis]
        [hebo.action]
        [hebo.util]
        [hebo.main :only [terminate initiate]])
  (:require [taoensso.carmine :as car]
            [cascalog.cascading [util :as u] [tap :as t]])
  (:import [com.guokr.hebo.tap Granularity HfsTemplateTap]
            [cascading.scheme.hadoop TextDelimited]))

(defn ok? [task params]
  (println "check running status of [" task params "]")
  (if (= (redis (car/sismember (str "run:" (name task)) (join "-" params)) 0))
    (do
      (redis (car/sadd (str "run:" (name task)) (join "-" params)))
      true)
    false))

(defn rm [task params]
  (redis (car/srem (str "run:" (name task)) (join "-" params))))

(defn purge [task params job-fname]
  (println "purge" job-fname)
  (redis (car/srem (str "run:" (name task)) (join "-" params)))
  (sh "hadoop" "fs" "-rmr" job-fname))

(defn stitch-path [fs fname]
  "make output filename based on filesystem and filename
  {:fs :hdfs :name [ pv :year :month :day :hour]}  ;hdfs format
  {:fs :local :name [ /data/logs/extracted :year :month :day]}  ;local format"
  (when-not (and (empty? fs) (empty? fname)) 
    (if (= "hdfs" fs)
      (str "/" (join "/" fname))  ;default fs is hdfs
      (str "file://" (join "/" fname)))))

(defn get-input-info [pretask]
  "return path of input file based on previous task "
  (let [task-name (str "task:" (name pretask) ":output")
        [fs fbase fgranu fdelimiter] (redis (car/hget task-name "fs")
                                            (car/hget task-name "base")
                                            (car/hget task-name "granularity")
                                            (car/hget task-name "delimiter"))]
    [fs fbase (keyword fgranu) fdelimiter]))


(defn check-refs [refs job-param]
  "check if the references have done"
  (if (empty? refs)
    true
    (let [joint-param (join "-" job-param)]
      (loop [refs refs]
        (let [f (first refs)]
          (if (nil? f)
            true
            (if (= 1 (redis (car/sismember (str "end:" (name  f)) joint-param)))
              (recur (rest refs))
              false))))))) 

(defn check-pretask [task pretask job-param]
  "check if the previous tasks have done"
  (if (nil? pretask) ;没有前置任务时直接返回true
    true
    (let [pretask (name pretask)
          [igranu ogranu pre-job-param-num] (redis (car/hget (str "task:" pretask ":output") "granularity") 
                                                   (car/hget (str "task:" task ":output") "granularity")
                                                   (car/llen (str "task:" pretask ":param")))
          redis-key (str "job:" pretask ":" (join "-" (take (parseInt pre-job-param-num) job-param)));demo job:count-uv:2013-11-18
          pre-job-status (set (redis (car/smembers redis-key)))]
      (prn "redis-key = " redis-key "pre-job-status = " pre-job-status "param = " (join "-" job-param) "io = " (granu-compare igranu ogranu))
      (if (> (count pre-job-status) 0)    
        (cond
          (= 0 (granu-compare igranu ogranu)) (contains? pre-job-status (join "-" job-param))
          (> 0 (granu-compare igranu ogranu)) ;igranu=hourly   ogranu=daily ;aid by daemon process 
            (let [converter {"yearly" years "monthly" months "daily" days "hourly" hours}
                  begin (apply date-time (map parseInt job-param))
                  end (plus begin ((converter ogranu) 1))
                  arr2datetime (fn [arr] (apply date-time arr))
                  timestamps (set (map to-long (map arr2datetime (map #(map parseInt (split % #"-")) pre-job-status))))]
              (prn timestamps)    
              (loop [dt begin]
                (if (>= (to-long dt) (to-long end))
                  true
                  (if (contains? timestamps (to-long dt)) 
                    (recur (plus dt ((converter igranu) 1)))
                    (do 
                      (prn "not in = "dt)
                      false)))))
          (< 0 (granu-compare igranu ogranu)) ; igranu=daily ogranu=hourly
              (contains? pre-job-status (join "-" job-param)))
        false)
      )))


(defn retrieve-pathFields [igranu ogranu]
  "return path fields based on igranu and ogranu for t/hfs-tap to construct its :templatefields"
  (let [level {:year 4, :month 3, :day 2, :hour 1}
        mapping {:yearly :year :monthly :month :daily :day :hourly :hour}
        inverted (clojure.set/map-invert level)
        ilevel (level (igranu mapping))
        olevel (level (ogranu mapping))
        interval (- ilevel olevel)
        namize (fn [kw] (str "?" (name kw)))
        get-level (fn [idx] (namize (get inverted idx)))
        ogranulevel (namize (ogranu mapping))
        ]
    (cond 
      (= interval 1) (vector ogranulevel)   ; eg: igranu=:daily,ogranu=:hourly  output: ?hour
      (= interval 2) (vector (get-level (+ 1 olevel)) ogranulevel)
      (= interval 3) (vector (get-level (+ 2 olevel)) (get-level (+ 1 olevel)) ogranulevel)
      (= interval -1) "/*"
      (= interval -2) "/*/*"
      (= interval -3) "/*/*/*"
      ))) 

(defn construct-source [input igranu ogranu delimiter]
  (let [pattern (retrieve-pathFields igranu ogranu)]
    (if (string? pattern)
      (hfs-delimited input :delimiter delimiter :quote "" :source-pattern pattern)
      (hfs-delimited input :delimiter delimiter :quote ""))))

(defn construct-sink [igranu ogranu query output delimiter]
  (let [pattern (retrieve-pathFields igranu ogranu)]
    (if (vector? pattern)
      (let [number (count pattern)
            n-rest (apply comp (repeat number rest))
            n-template (join "/" (repeat number "%s"))
            scheme (t/text-line ["line"] (u/fields (n-rest (get-out-fields query))))
            sink (t/hfs-tap scheme output  :delimiter delimiter :sinkmode :keep :sink-template n-template :templatefields pattern)]
        sink)
    (hfs-delimited output :delimiter delimiter))))

(defn construct-sink2 [igranu ogranu query output delimiter]
  (let [granu-mapping {:yearly Granularity/YEARLY :monthly Granularity/MONTHLY :daily Granularity/DAILY :hourly Granularity/HOURLY :minutely Granularity/MINUTELY}]
    (if (= igranu ogranu)
      (hfs-delimited output :delimiter delimiter)
      (let [scheme (TextDelimited. (u/fields (get-out-fields query)) false delimiter)]
        (HfsTemplateTap. (t/hfs scheme output) (igranu granu-mapping) (ogranu granu-mapping)))
    )))

(def task-running-status (atom ""))

(defn dgranu-tracking [taskname job-param ogranu dgranu]
  (mapfn [datetime]
    (let [dt (trunc-datetime datetime ogranu)]
      (if (not (= @task-running-status dt))
        (let [redis-key (str "job:" taskname ":" (join "-" job-param))]
          (redis (car/sadd redis-key dt))
          (reset! task-running-status dt)
          (prn "task-running-status = " @task-running-status)))
      (unparse dt-formatter (from-long (* 1000 ((granularity dgranu) datetime)))))))

(defn usage [options-summary]
  (->> [""
        "Usage: taskname [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "name      retrieve taskname"        
        "cron      retrieve task's cron"
        "begin     add begin item in redis"
        "exec      exec the task"
        "purge     purge the task"
        "info      retrieve information of the task"        
        ""
        "Please refer to the manual page for more information."]
       (join \newline)))

(defmacro deftask [task-name & {:as args}]
  (let [task#          (symbol task-name)
        test-name#     (symbol (str "test-task-" task-name))
        test-all#      (symbol (str "test-all-task-" task-name))
        begin-name#    (symbol (str "begin-job-" task-name))
        exec-name#     (symbol (str "exec-job-" task-name))
        end-name#      (symbol (str "end-job-" task-name))
        after-name#    (symbol (str "after-job-" task-name))
        install-name#     (symbol (str "read-job-" task-name))
        purge-name#    (symbol (str "purge-job-" task-name))
        intern-main#   (symbol "-main")
        intern-exec#   (symbol "-exec")
        info#          (assoc args :name (name task-name))
        description#   (:desc args)
        cron#          (:cron args)
        parameters#    (vec (:param args))
        job-params#    (vec (map symbolize (:param args)))
        pretask#       (:pretask args)
        references#    (:refs args)
        ofs#           (name (:fs (:output args)))
        obase#         (:base (:output args))
        ogranu#        (:granularity (:output args))
        odelimiter#    (:delimiter (:output args))
        dgranu#        (:granularity (:data args))
        query#         (:query args)
        proc#          (or (:process args) (fn [ctx] ctx))]
    `(do
       (def ~task#
         (fn ~job-params# (initiate (str '~task-name) ~job-params#)))
       (def ~begin-name#
         (fn ~job-params# (initiate (str '~task-name) ~job-params#)))
       (def ~exec-name# (fn ~job-params#
         (if (check-pretask '~task-name ~pretask# ~job-params#)
           (if (check-refs ~references# ~job-params#)
             (let [utime# (datetime2timestamp (from-time-zone (apply date-time (map parseInt ~job-params#)) (default-time-zone)))
                   [ifs# ibase# igranu# idelimiter#] (get-input-info ~pretask#)
                   input# (stitch-path ifs# (cons ibase# ~job-params#)) 
                   source# (construct-source input# igranu# ~ogranu# idelimiter#)
                   qry# (~query# (granularity igranu#) (dgranu-tracking '~task-name ~job-params# ~ogranu# ~dgranu#) utime# source#)
                   output# (stitch-path ~ofs# (cons ~obase# ~job-params#))
                   sink# (construct-sink2 igranu# ~ogranu# qry# output# ~odelimiter#)]
                 (if (ok? (str '~task-name) ~job-params#)
                   (try
                     (do
                       (?- sink# qry#)
                       (~proc# {:input {:fs ifs# :base ibase# :granularity igranu# :delimiter idelimiter#} :output (:output ~args) :param ~job-params#})
                       (terminate (str '~task-name) ~job-params#)
                       (fire-next (name '~task-name) ~job-params#)
                       (rm (str '~task-name) ~job-params#))
                     (catch Exception err#
                       (do
                         (print-stack-trace err#)
                         (print-stack-trace (root-cause err#))
                         (purge (str '~task-name) ~job-params# output#)
                         )))))
             (prn "refs undone"))
           (prn "pretask undone!"))))
       
       (def ~purge-name# (fn ~job-params#
         (purge (str '~task-name) ~job-params# (stitch-path ~ofs# (cons ~obase# ~job-params#)))))
       
       (def ~install-name# (fn []
          (addtask (str '~task-name) (dissoc ~args :query :process))))
          ;(send-command 54319 "refresh"))
     
       (def ~intern-main# (fn [& commands#]
         (let [cmds# (parse-opts commands# ~cli-opts)
              options# (:options cmds#)
              arguments# (:arguments cmds#)
              errors# (:errors cmds#)
              summary# (:summary cmds#)]
          (cond
            (:help options#) (exit 0 (usage summary#))
            errors# (exit 1 (error-msg errors#)))
            (reset! hebo-conf (load-yaml (:conf options#)))
            (case (first arguments#)
              "name"  (println (str '~task-name))
              "cron"  (println ~cron#)
              "begin" (apply ~begin-name# (rest arguments#))
              "exec"  (apply ~exec-name# (rest arguments#))
              "purge" (apply ~purge-name# (rest arguments#))
              "install" (~install-name#)
              "info" 
              (let [output# (:output ~args)
                    output-base# (:base output#)
                    output-delimiter# (:delimiter output#)
                    new-output# (assoc output# :base (str "\"" output-base# "\"") :delimiter (str "\"" output-delimiter# "\""))]
                     (println (assoc (dissoc ~args :query :process) 
                         :output new-output# :desc (str "\"" ~description# "\"") :cron (str "\"" ~cron# "\"")))) 
              (prn "unknown task command!")) 
          ))))))


