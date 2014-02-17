(ns hebo.proc
  (:use [clojure.string :only (join split trim join)]
        [clojure.stacktrace :only [print-stack-trace root-cause]]
        [clj-time.core :exclude [second extend]]
        [clojure.tools.logging :only [info warn error]]
        [hebo.redis]
        [hebo.util])
  (:require [taoensso.carmine :as car]))

(defn terminate [procname joint-param]
  (redis (car/sadd (str "proc:end:" procname) joint-param)))

(defmacro defproc [proc-name & {:as args}]
  (let [parameters#    (vec (:param args))
        job-params#    (vec (map symbolize (:param args)))
        intern-main#   (symbol "-main")
        intern-exec#   (symbol "-exec")
        proc#         (:process args)]
    `(do
       (def ~intern-exec# (fn ~job-params#
         (let [timestamp# (datetime-to-timestamp (from-time-zone (apply date-time (map parse-int ~job-params#)) (default-time-zone)))]
           (try
             (info "running proc" '~proc-name ~job-params#)
             (~proc# timestamp# (assoc (dissoc ~args :process) :param ~job-params#))
             (terminate '~proc-name (join "-" ~job-params#))
           (catch Exception err#
             (do
               (print-stack-trace err#)
               (print-stack-trace (root-cause err#))))))))
       
       (def ~intern-main# (fn [& arguments#]
         (case (first arguments#) 
           "exec" (apply ~intern-exec# (next arguments#))
           "info" (prn (:cron ~args))
           (prn "unknown hebo proc command!")))))))
