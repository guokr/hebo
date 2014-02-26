(ns hebo.redis
  (:use [clojure.java.io]
        [clojure.tools.logging :only [info warn error]]
        [clojure.string :only (join split)]
        [hebo.xml])
  (:require [taoensso.carmine :as car]))
 
(defn get-conn []
  (let [ip (parse-default-name (resource "core-site.xml"))
        port  (Integer. (or (System/getenv "HEBO_PORT") 9876))]
    {:pool {} :spec {:host ip :port port}}))

(defmacro redis [& body] `(car/wcar (get-conn) ~@body))

(defn assemble-redis-cmd [cmd params]
  (doseq [p params]
    (apply cmd p)))
 
 