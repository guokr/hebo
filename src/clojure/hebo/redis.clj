(ns hebo.redis
  (:use [clojure.java.io]
        [clojure.tools.logging :only [info warn error]]
        [clojure.string :only (join split)]
        [hebo.xml])
  (:require [taoensso.carmine :as car]))
 
(defn get-conn []
  (let [ip (parse-default-name (resource "core-site.xml"))]
    {:pool {} :spec {:uri ip :db 7}}))

(defmacro redis [& body] `(car/wcar (get-conn) ~@body))
 
 