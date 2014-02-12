(ns hebo.cron
  (:use [clj-time.core :only [today-at date-time minus days before?]]
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
  (let [m (qc/from-job-data ctx)]
    (info "exec-cron" (m "task"))
    (sh "hebo" "exec" (str (m "task")))))

(defjob JobDaemon [ctx]
  (let [today00 (today-at 0 0) 
        all-status (redis (car/keys "job*"))
        all-task (get-all-tasks)
        status-len (count all-status)
        task-len (count all-task)]
    (if (> status-len 0)
      (let [a-week-ago (minus today00 (days 7))
            params (map #(subs % (inc (.lastIndexOf % ":"))) all-status)
            arr2datetime (fn [arr] (apply date-time arr))
            datetimes (map arr2datetime (map #(map parseInt (split % #"-")) params))
            status-dt-mapping (zipmap all-status datetimes)
            to-be-deleted (keys (filter #(before? (second %) a-week-ago) status-dt-mapping))]
        (if (not (nil? to-be-deleted))     
          ;(eval (cons 'hebo.redis/redis  (for [t to-be-deleted] `(car/del ~t))))
          (prn to-be-deleted)
          )))
    (if (> task-len 0)
      (doseq [taskname all-task]
        (let [begin-list (str "begin:" taskname)
              end-list   (str "end:" taskname)
              remains (redis (car/sdiff begin-list end-list))
              two-days-ago (minus today00 (days 2))]
          (if (> (count remains) 0)
            (let [arr2datetime (fn [arr] (apply date-time arr))
                  datetimes (map arr2datetime (map #(map parseInt (split % #"-")) remains))
                  undone-task-date (filter #(before? % two-days-ago) datetimes)]
              (if (> (count undone-task-date) 0)
                (prn "remains = " taskname remains)))))))))

(defn addcron [taskname cron]
  (let [job     (j/build
                  (j/of-type ShellJob)
                  (j/using-job-data {:task taskname})
                  (j/with-identity (j/key taskname)))
        trigger (t/build
                  (t/with-identity (t/key taskname))
                  (t/start-now)
                  (t/with-schedule (schedule (cron-schedule cron))))]
    (qs/schedule job trigger))
  (info "add-cron" taskname cron))

(defn refresh-cron []
    (qs/clear!)
    (let [inputs (redis (car/hkeys "cron"))]
      (doseq [[x y] (zipmap inputs
          (loop [tasks inputs crons []]
            (if (empty? tasks) crons
              (recur (rest tasks) (conj crons (redis (car/hget "cron" (first tasks))))))))]
        (addcron x y))))
