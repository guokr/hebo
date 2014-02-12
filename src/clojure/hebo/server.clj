(ns hebo.server
  (:use [clojure.string :only [trim join]]
        [clojure.java.io :only [reader writer]]
        [hebo.cron :only [refresh-cron]]
        [server.socket :only [create-server close-server]]
        [clojure.tools.logging :only (info error)])
  (:require [clojurewerkz.quartzite.scheduler :as qs])
  (:import (java.net Socket SocketException)
           (java.io OutputStream PrintWriter)))

(def ^:dynamic *server* (atom nil))

(defn -handle-client [in out]
  (binding [*in* (reader in)
            *out* (writer out)
            *err* (writer System/err)]
    (info "Welcome, hebo is running...")
    (flush)
    (loop [input (trim (read-line))]
      (when-not (= "exit" input)
        (do
          (if (= "refresh" input)
            (refresh-cron))
          (flush)
          (Thread/sleep 300)
          (recur (read-line)))))
    (info "hebo is stopping...")
    (qs/shutdown)
    (close-server @*server*)
    (System/exit 0)))

(defn start-server [port]
  (if (nil? @*server*)
    (do
      (reset! *server* (create-server (Integer. port) -handle-client))
      true)
    false))

(defn send-command [port command]
  (doto (PrintWriter. (.getOutputStream (Socket. "localhost" port)) true)
    (.println command)
    (.flush)
    (.close)))

