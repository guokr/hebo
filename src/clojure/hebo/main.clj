(ns hebo.main
  (:use [clojure.java.shell :only [sh]]
        [clojure.tools.cli :only [parse-opts]]
        [clojure.stacktrace :only [print-stack-trace root-cause]]
        [clojure.string :only (join split trim split-lines)]
        [clojurewerkz.quartzite.jobs :only [defjob]]
        [clojurewerkz.quartzite.schedule.cron :only [schedule cron-schedule]]
        [clojure.tools.logging :only (info error)]
        [hebo.redis]
        [hebo.util]
        [hebo.server :only [start-server send-command]])
  (:require [taoensso.carmine :as car]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs :as j]
            [clojurewerkz.quartzite.conversion :as qc])
  (:gen-class))

(defn initiate [taskname params]
  (redis 
    (car/sadd (str "begin:" taskname) (join "-" params)))
  (println taskname params))

(defn reinitiate [taskname params]
  (apply sh (flatten [taskname "purge" params]))
  (redis
    (car/srem (str "run:" taskname) (join "-" params))
    (car/srem (str "begin:" taskname) (join "-" params))
    (car/srem (str "end:" taskname)   (join "-" params))
    (car/sadd (str "begin:" taskname) (join "-" params)))
  (println taskname params))

(defn execute [taskname]
  (redis (car/sdiffstore (str "cur:" taskname) (str "begin:" taskname) (str "end:" taskname)))
  (loop [job (redis (car/spop (str "cur:" taskname)))]
    (when (not (nil? job))
      (println (flatten [taskname "exec" (split job #"-")]))
      (if (and
            (= (redis (car/sismember (str "end:" taskname) job)) 0)
            (= (redis (car/sismember (str "run:" taskname) job)) 0)) 
        (apply sh (flatten [taskname "-c" @hebo-conf  "exec" (split job #"-")])))
      (redis (car/sdiffstore (str "cur:" taskname) (str "begin:" taskname) (str "end:" taskname)))
      (recur (redis (car/spop (str "cur:" taskname)))))))

(defn terminate [taskname params]
  (redis (car/sadd (str "end:" taskname) (join "-" params))))

(defn start []
  (if-let [results (redis (car/hkeys "cron"))]
    (loop [tasknames results]
      (when (not (empty? tasknames))
        (let [^String taskname (str (first tasknames))]
            (loop [job (redis (car/spop (str "run:" taskname)))]
              (when (not (nil? job))
                (println (flatten [taskname "purge" (split job #"-")]))
                (apply sh (flatten [taskname "purge" (split job #"-")]))
                (recur (redis (car/spop (str "run:" taskname)))))))
        (recur (rest tasknames)))))
  (start-server 54319)
  (qs/initialize)
  (qs/start)
  (send-command 54319 "refresh"))

(defn stop []
  (send-command 54319 "exit"))

(defn task-remove [arguments]
  (if arguments
    (doseq [tasknames arguments]
      (let [^String taskname (str tasknames)
            task-items (redis (car/keys (str "task:" taskname ":*")))]
        (doseq [item task-items]
          (redis (car/del item)))))))

(defn task-install [arguments]
  (if arguments
    (loop [tasknames arguments]
      (when (not (empty? tasknames))
        (let [taskname (str (first tasknames))
              cmd (trim (:out (sh taskname "info")))
              taskinfo (try 
                          (read-string cmd) 
                          (catch Exception e 
                            (do
                              (prn taskname)
                              (prn cmd)
                              (print-stack-trace e)
                              (print-stack-trace (root-cause e)))))]
          (prn taskinfo)    
          (addtask taskname taskinfo))
        (recur (rest tasknames))))
    ;(send-command 54319 "refresh")
    ))

(defn usage [options-summary]
  (->> [""
        "Usage: hebo [options] action"
        ""
        "Options:"
        options-summary
        ""
        "Actions:"
        "start     start hebo system to cron your tasks"
        "stop      stop hebo system"
        "exec      execute a task"
        "remove    remove your task from hebo system"
        "list        list all tasks in current hebo system"
        "status    check the status of your task"
        ""
        "Please refer to the manual page for more information."]
       (join \newline)))

(defn -main [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-opts)]
    (cond
      (:help options) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors)))
      (reset! hebo-conf (load-yaml (:conf options)))
    (case (first arguments)
      "start"  (start)
      "stop"   (stop)
      "exec"   (execute (second arguments))
      "init"   (initiate (second arguments) (nnext arguments))
      "reinit" (reinitiate (second arguments) (nnext arguments))
      "term"   (terminate (second arguments) (nnext arguments))
      "remove" (task-remove (next arguments))
      "install" (task-install (next arguments))
      (exit 1  (usage summary)))))
