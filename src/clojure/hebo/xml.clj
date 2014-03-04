(ns hebo.xml
  (:use [clojure.data.xml :only [parse-str]]
         [clojure.string :only [split]]))

(defn parse-default-name [filename]
  (let [core-file (slurp filename)
        props (map  #(:content %) (:content (parse-str core-file)))
        is-fsname (fn [contents]
                    (loop [contents contents]
                      (let [con (first contents)]
                      (when-not (empty? con)
                        (if (and (= (:tag con) :name) (= (first (:content con)) "fs.default.name")) 
                          true
                          (recur (next contents))
                          )))))
        fs-kv (first (filter is-fsname props))
        default-name (loop [fs-kv fs-kv]
                      (let [fs (first fs-kv)]
                        (when-not (empty? fs)
                          (if (= (:tag fs) :value) 
                            (first (:content fs))
                            (recur (next fs-kv))))))]
     default-name))