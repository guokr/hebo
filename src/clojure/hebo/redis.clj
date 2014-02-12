(ns hebo.redis
  (:use [clojure.java.io]
        [clojure.string :only (join split)])
  (:require [clj-yaml.core :as yaml]
             [taoensso.carmine :as car]))

(def hebo-conf (atom ""))

(defn load-yaml [path]
  (yaml/parse-string (slurp (str path "/hebo.yaml"))))
 
(defn get-conn [conf]
  (let [redis-conf (split (:redis conf) #"#")]
    {:pool {} :spec {:uri (first redis-conf) :db (Integer. (second redis-conf))}}))

(defmacro redis [& body] `(car/wcar (get-conn (deref hebo-conf)) ~@body))
 
 