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

(defn isrunning? [task params]
  (println "check running status of [" task params "]")
  (if (= (redis (car/sismember (str "run:" (name task)) (join "-" params)) "0"))
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

(defn get-input-info [curtask pretask]
  "return path of input file based on previous task "
  (let [input (if (nil? pretask)
                (str "task:" (name curtask) ":input")
                (str "task:" (name pretask) ":output"))
        [fs fbase fgranu fdelimiter] (redis (car/hget input "fs")
                                            (car/hget input "base")
                                            (car/hget input "granularity")
                                            (car/hget input "delimiter"))]
    (info fs fbase (keyword fgranu) fdelimiter)    
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
            (if (= "1" (redis (car/sismember (str "end:" (name  f)) joint-param)))
              (recur (rest refs))
              false))))))) 

(defn check-pretask [task pretask task-param]
  "check if the previous tasks have done"
  (if (nil? pretask) ;没有前置任务时直接返回true
    true
    (let [pretask (name pretask)
          [igranu ogranu] (redis (car/hget (str "task:" pretask ":output") "granularity") 
                                 (car/hget (str "task:" task ":output") "granularity"))
          igranu (keyword igranu)
          ogranu (keyword ogranu)
          pretask-history (set (redis (car/smembers (str "task:" pretask ":history"))))
          joint-param (join "-" task-param)]
      (info "pretask-history = " pretask-history "param = " joint-param "io = " (granu-compare igranu ogranu))    
      (if (> (count pretask-history) 0)    
        (cond
          (= 0 (granu-compare igranu ogranu)) (contains? pretask-history joint-param)
          (< 0 (granu-compare igranu ogranu)) ;igranu=hourly   ogranu=daily ;aid by daemon process 
            (let [converter {:yearly years :monthly months :daily days :hourly hours :minutely minutes}
                  begin (apply date-time (map parse-int task-param))
                  end (to-long (plus begin ((ogranu converter) 1)))
                  arr-to-datetime (fn [arr] (apply date-time arr))
                  timestamps (set (map to-long (map arr-to-datetime (map #(map parse-int (split % #"-")) pretask-history))))]
              (loop [current (to-long begin)]
                (if (>= current end)
                  true
                  (if (contains? timestamps current) 
                    (recur (to-long (plus (from-long current) ((converter igranu) 1))))
                    (do 
                      (info pretask (from-long current) "not finished!" task "can't start")
                      false)))))
          (> 0 (granu-compare igranu ogranu)) ; igranu=daily ogranu=hourly
              (contains? pretask-history (join "-" (take (igranu granu-level) task-param))))
        false)
      )))

(defn make-input-pattern [igranu ogranu]
  (case (granu-compare igranu ogranu)
    1 "/*"
    2 "/*/*"
    3 "/*/*/*"
    "default"))

(defn modify-input-param [igranu ogranu param]
  (let [gap-length (granu-compare igranu ogranu)]
    ; max(igranu,ogranu)=job-granu  ?? 
    ;(if (> gap-length 0)
    ; (subvec param 0 (- (count param) gap-length))
      param))

(defn construct-source [fs base param igranu ogranu delimiter]
  (let [fine-param (modify-input-param igranu ogranu param)
        pattern (make-input-pattern igranu ogranu)
        input (stitch-path fs (cons base fine-param))]
    (info "input " input)    
    (if (not= "default" pattern)
      (hfs-delimited input :delimiter delimiter :quote "" :source-pattern pattern)
      (hfs-delimited input :delimiter delimiter :quote ""))))

(defn construct-sink [igranu ogranu query output delimiter]
  (let [granu-mapping {:yearly Granularity/YEARLY :monthly Granularity/MONTHLY :daily Granularity/DAILY :hourly Granularity/HOURLY :minutely Granularity/MINUTELY}]
    (if (> (granu-compare igranu ogranu) 0)
      (hfs-delimited output :delimiter delimiter)
      (let [scheme (TextDelimited. (u/fields (get-out-fields query)) false delimiter)]
        (HfsTemplateTap. (t/hfs scheme output) (igranu granu-mapping) (ogranu granu-mapping))))))

(def task-running-status (atom ""))

(defn dgranu-tracking [taskname ogranu dgranu]
  (mapfn [datetime]
    (let [dt (trunc-datetime datetime ogranu)]
      (when-not (= @task-running-status dt)
        (redis (car/sadd (str "task:" taskname ":history") dt))
        (reset! task-running-status dt))
      (unparse dt-formatter (from-long (* 1000 ((granularity-is dgranu) datetime)))))))

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
        begin-name#    (symbol (str "begin-job-" task-name))
        exec-name#     (symbol (str "exec-job-" task-name))
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
        prepare#       (or (:prepare args) (fn [ctx] ctx))
        final#         (or (:final args) (fn [ctx] ctx))]
    `(do
       (def ~task#
         (fn ~job-params# (initiate (str '~task-name) ~job-params#)))
       (def ~begin-name#
         (fn ~job-params# (initiate (str '~task-name) ~job-params#)))
       (def ~exec-name# (fn ~job-params#
         (if (check-pretask '~task-name ~pretask# ~job-params#)
           (if (check-refs ~references# ~job-params#)
             (let [utime# (datetime-to-timestamp (from-time-zone (apply date-time (map parse-int ~job-params#)) (default-time-zone)))
                   [ifs# ibase# igranu# idelimiter#] (get-input-info '~task-name ~pretask#)
                   source# (construct-source ifs# ibase# ~job-params# igranu# ~ogranu# idelimiter#)
                   qry# (~query# (granularity-is igranu#) (dgranu-tracking '~task-name ~ogranu# ~dgranu#) utime# source#)
                   output# (stitch-path ~ofs# (cons ~obase# ~job-params#))
                   sink# (construct-sink igranu# ~ogranu# qry# output# ~odelimiter#)
                   ctx# {:input {:fs ifs# :base ibase# :granularity igranu# :delimiter idelimiter#} :output (:output ~args) :param ~job-params#}]
                 (if (isrunning? (str '~task-name) ~job-params#)
                   (try
                     (do
                       (info "running task" '~task-name ~job-params#)
                       (~prepare# ctx#)
                       (?- sink# qry#)
                       (~final# ctx#)
                       (terminate (str '~task-name) ~job-params#)
                       (fire-next (str '~task-name) ~job-params#)
                       (rm (str '~task-name) ~job-params#))
                     (catch Exception err#
                       (do
                         (print-stack-trace err#)
                         (print-stack-trace (root-cause err#))
                         (purge (str '~task-name) ~job-params# output#))))))
             (do
               (error '~task-name ~job-params# "refs undone")
               (System/exit 1)))
           (do
             (error '~task-name ~job-params# "pretask undone!")
             (System/exit 1)))))
       
       (def ~purge-name# (fn ~job-params#
         (purge (str '~task-name) ~job-params# (stitch-path ~ofs# (cons ~obase# ~job-params#)))))
     
       (def ~intern-main# (fn [& commands#]
         (let [cmds# (parse-opts commands# ~cli-opts)
              options# (:options cmds#)
              arguments# (:arguments cmds#)
              errors# (:errors cmds#)
              summary# (:summary cmds#)]
          (cond
            (:help options#) (exit 0 (usage summary#))
            errors# (exit 1 (error-msg errors#)))
          (case (first arguments#)
            "name"  (println (str '~task-name))
            "cron"  (println ~cron#)
            "begin" (apply ~begin-name# (rest arguments#))
            "exec"  (apply ~exec-name# (rest arguments#))
            "purge" (apply ~purge-name# (rest arguments#))
            "info" 
              (let [output# (:output ~args)
                    output-base# (:base output#)
                    output-delimiter# (:delimiter output#)
                    new-output# (assoc output# :base (str "\"" output-base# "\"") :delimiter (str "\"" output-delimiter# "\""))]
                (if (not (nil? (:input ~args)))
                  (let [input# (:input ~args)
                        input-base# (:base input#)
                        input-delimiter# (:delimiter input#)
                        new-input# (assoc input# :base (str "\"" input-base# "\"") :delimiter (str "\"" input-delimiter# "\""))]
                    (println (assoc (dissoc ~args :query :prepare :final)
                             :output new-output# :input new-input# :desc (str "\"" ~description# "\"") :cron (str "\"" ~cron# "\""))))   
                  (println (assoc (dissoc ~args :query :prepare :final)
                           :output new-output# :desc (str "\"" ~description# "\"") :cron (str "\"" ~cron# "\""))))) 
            (prn "unknown task command!")) 
          ))))))
