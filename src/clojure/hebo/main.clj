(ns hebo.main
  (:use [clojure.java.shell :only [sh]]
        [clojure.tools.cli :only [parse-opts]]
        [clojure.stacktrace :only [print-stack-trace root-cause]]
        [clojure.string :only (join split trim split-lines)]
        [clojure.tools.logging :only [info warn error]]
        [clojurewerkz.quartzite.jobs :only [defjob]]
        [clojurewerkz.quartzite.schedule.cron :only [schedule cron-schedule]]
        [clojure.tools.logging :only (info error)]
        [clojure.java.io :only [resource]]
        [hebo.redis]
        [hebo.util]
        [hebo.xml]
        [hebo.log]
        [hebo.server :only [start-server send-command]])
  (:require [taoensso.carmine :as car]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs :as j]
            [clojurewerkz.quartzite.conversion :as qc])
  (:import [com.guokr.hebo Hebo])
  (:gen-class))


(defn initiate [taskname params]
  (redis (car/sadd (str "begin:" taskname) (join "-" params)))
  (println taskname params))

(defn reinitiate [taskname params]
  (apply sh (flatten [taskname "purge" params]))
  (redis
    (car/srem (str "run:" taskname) (join "-" params))
    (car/srem (str "begin:" taskname) (join "-" params))
    (car/srem (str "end:" taskname)   (join "-" params))
    (car/sadd (str "begin:" taskname) (join "-" params)))
  (println taskname params))

(defn task-install [arguments]
  (doseq [taskname arguments]
    (when-not (empty? taskname)
      (let [cmd (trim (:out (sh taskname "info")))
            taskinfo (try (read-string cmd)
                      (catch Exception err
                        (do
                          (print-stack-trace err)
                          (error "task" taskname "add error"))))]
        (info taskinfo)
        (addtask taskname taskinfo))))
    (send-command 54319 "refresh"))

(defn task-remove [arguments]
  (doseq [taskname arguments]
    (when-not (empty? taskname)
      (if (= "all" taskname)
        (let [task-items (redis (car/keys "task:*"))
              task-meta (filter #(not (.endsWith % "history")) task-items)]
          (redis
            (assemble-redis-cmd car/del (map #(vector %) task-meta))
            (car/del "cron")))
        (let [task-items (redis (car/keys (str "task:" taskname ":*")))
              task-meta (filter #(not (.endsWith % "history")) task-items)]
          (redis
            (assemble-redis-cmd car/del (map #(vector %) task-meta))
            (car/hdel "cron" taskname)))))))

(defn task [subcommand arguments]
  (case subcommand
    "install" (task-install arguments)
    "remove"  (task-remove arguments)
    (warn "subcommand task only has two options: install remove")))

(defn proc-install [arguments]
  (doseq [procname arguments]
    (when-not (empty? procname)
      (let [cmd (trim (:out (sh procname "info")))
            procinfo (try (read-string cmd)
                      (catch Exception err
                        (do
                          (print-stack-trace err)
                          (error "procname" procname "add error"))))]
        (redis (car/hset "cron" procname procinfo))
        (info procname procinfo))))
    (send-command 54319 "refresh"))

(defn proc-remove [arguments]
  (doseq [procname arguments]
    (when-not (empty? procname)
      (redis (car/hdel "cron" procname)))))

(defn proc [subcommand arguments]
  (case subcommand
    "install" (proc-install arguments)
    "remove"  (proc-remove arguments)
    (warn "subcommand proc only has two options: install remove")))

(defn execute [taskname]
  (redis (car/sdiffstore (str "cur:" taskname) (str "begin:" taskname) (str "end:" taskname)))
  (loop [job (redis (car/spop (str "cur:" taskname)))]
    (info "cur:" taskname ":" (redis (car/smembers (str "cur:" taskname))))
    (when-not (nil? job)
      (println (flatten [taskname "exec" (split job #"-")]))
      (let [[end? run?] (map parse-int (redis (car/sismember (str "end:" taskname) job)
                                              (car/sismember (str "run:" taskname) job)))]
        (if (and (= end? 0) (= run? 0))
          (let [err-msg (:err (apply sh (flatten [taskname "exec" (split job #"-")])))]
            (when-not (nil? err-msg)
              (error taskname job " exit with " err-msg)
              (System/exit 2))))
        (redis (car/sdiffstore (str "cur:" taskname) (str "begin:" taskname) (str "end:" taskname)))
        (recur (redis (car/spop (str "cur:" taskname))))))))

(defn terminate [taskname params]
  (redis (car/sadd (str "end:" taskname) (join "-" params))))

(defn start []
  ;(try
  ;  (doto
  ;    (Hebo.)
  ;    (.run))
  ;  (catch Exception err
  ;    (do
  ;      (error err)
  ;      (print-stack-trace err)
  ;      (print-stack-trace (root-cause err)))))
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
        "init      initialize the status of a task"
        "reinit    reinitialize the status of a task"
        "term      terminate a  task"
        "task      has install/install subcommand"
        "proc      has install/install subcommand"
        "list      list all tasks in current hebo system"
        ""
        "Please refer to the manual page for more information."]
       (join \newline)))

(defn -main [& args]
  (java.util.Locale/setDefault java.util.Locale/ENGLISH)
  (init-logging)
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-opts)]
    (cond
      (:help options) (exit 0 (usage summary))
      errors (exit 1 (error-msg errors)))
    (case (first arguments)
      "start"  (start)
      "stop"   (stop)
      "exec"   (execute (second arguments))
      "init"   (initiate (second arguments) (nnext arguments))
      "reinit" (reinitiate (second arguments) (nnext arguments))
      "term"   (terminate (second arguments) (nnext arguments))
      "task"   (task (second arguments) (nnext arguments))
      "proc"   (proc (second arguments) (nnext arguments))
      "list"   (prn (get-all-tasks))
      (exit 1  (usage summary)))))
