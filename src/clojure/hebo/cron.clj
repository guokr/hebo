(ns hebo.cron
  (:use [clj-time.core :exclude [second extend]]
        [clj-time.coerce :only [to-long]]
        [clojure.string :only [split]]
        [clojure.java.shell :only [sh]]
        [clojurewerkz.quartzite.jobs :only [defjob]]
        [clojurewerkz.quartzite.schedule.cron :only [schedule cron-schedule]]
        [clojure.tools.logging :only (info error)]
        [hebo.redis]
        [hebo.util])
  (:require [taoensso.carmine :as car]
            [clojurewerkz.quartzite.scheduler :as qs]
            [clojurewerkz.quartzite.triggers :as t]
            [clojurewerkz.quartzite.jobs :as j]
            [clojurewerkz.quartzite.conversion :as qc]))


(defjob ShellJob [ctx]
  (let [m (qc/from-job-data ctx)
        taskname (str (m "task"))
        pretask (redis (car/get (str "task:" taskname ":pretask")))]
    (info "exec ShellJob" taskname "pretask is " pretask)
    (if (nil? pretask)
      (sh taskname "exec")
      (sh "hebo" "exec" taskname))))

(defn addcron [taskname cron]
  (info "add-cron" taskname cron)
  (let [job     (j/build
                  (j/of-type ShellJob)
                  (j/using-job-data {:task taskname})
                  (j/with-identity (j/key taskname)))
        trigger (t/build
                  (t/with-identity (t/key taskname))
                  (t/start-now)
                  (t/with-schedule (schedule (cron-schedule cron))))]
    (qs/schedule job trigger)))

(defjob HeboDaemon [ctx]
  (info "exec HeboDaemon")
  (let [today00 (from-time-zone (today-at 0 0) (default-time-zone))
        all-status (redis (car/keys "job*"))
        all-task (get-all-tasks)
        status-len (count all-status)
        task-len (count all-task)
        arr2datetime (fn [arr] (from-time-zone (apply date-time arr) (default-time-zone)))]
    (if (> status-len 0)
      (let [a-week-ago (minus today00 (days 7))
            params (map #(subs % (inc (.lastIndexOf % ":"))) all-status)
            datetimes (map arr2datetime (map #(map parse-int (split % #"-")) params))
            status-dt-mapping (zipmap all-status datetimes)
            to-be-deleted (keys (filter #(before? (second %) a-week-ago) status-dt-mapping))]
        (if (not (nil? to-be-deleted))
          (do
            (eval (cons 'guokr.redis/redis  (for [t to-be-deleted] `(car/del ~t))))
            (info "redis task job data = " to-be-deleted))
          )))
    (if (> task-len 0)
      (doseq [taskname all-task]
        (let [begin-list (str "begin:" taskname)
              end-list   (str "end:" taskname)
              remains (redis (car/sdiff begin-list end-list))
              two-days-ago (minus today00 (days 2))]
          (if (> (count remains) 0)
            (let [datetimes (map arr2datetime (map #(map parse-int (split % #"-")) remains))
                  undone-task-date (filter #(before? % two-days-ago) datetimes)]
              (if (> (count undone-task-date) 0)
                (error "task remains for 2 more days = " taskname undone-task-date)))))))))

(defn initdaemon []
  (let [job     (j/build
                  (j/of-type HeboDaemon)
                  (j/with-identity (j/key "job.hebodaemon")))
          trigger (t/build
                  (t/with-identity (t/key "trigger.hebodaemon"))
                  (t/start-now)
                  (t/with-schedule (schedule (cron-schedule "0 0 0/4 * * ?"))))]
      (info "add-cron hebodaemon 0 0 0/4 * * ?")    
      (qs/schedule job trigger)))

(defn refresh-cron []
  (qs/clear!)
  (let [inputs (redis (car/hkeys "cron"))]
    (doseq [[x y] (zipmap inputs
        (loop [tasks inputs crons []]
          (if (empty? tasks) crons
            (recur (rest tasks) (conj crons (redis (car/hget "cron" (first tasks))))))))]
      (addcron x y)))
  ;(initdaemon)
  )
