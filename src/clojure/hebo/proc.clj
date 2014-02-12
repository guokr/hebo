(ns hebo.proc
  (:use [clojure.string :only (join split trim join)]
        [clojure.stacktrace :only [print-stack-trace root-cause]]
        [clj-time.core :exclude [second extend]]
        [hebo.util]))

(defmacro defproc [proc-name & {:as args}]
  (let [parameters#    (vec (:param args))
        job-params#    (vec (map symbolize (:param args)))
        intern-main#   (symbol "-main")
        proc#         (:process args)]
    `(def ~intern-main# 
       (fn ~job-params#
         (let [timestamp# (datetime2timestamp (from-time-zone (apply date-time (map parseInt ~job-params#)) (default-time-zone)))]
           (try
             (~proc# timestamp# (assoc (dissoc ~args :process) :param ~job-params#))
           (catch Exception err#
             (do
               (print-stack-trace err#)
               (print-stack-trace (root-cause err#))))))))))


