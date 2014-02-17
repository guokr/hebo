(ns hebo.log
  (:import [org.apache.log4j RollingFileAppender PatternLayout Logger Level]))
 
(defn initLogging[]
  (let [layout (PatternLayout. "%d %5p [%t] - %m%n")
        hebo-info (doto (new RollingFileAppender layout "logs/hebo.info.log") 
                        (.setMaxFileSize "20MB")
                        (.setThreshold Level/INFO))
        hebo-error (doto (new RollingFileAppender layout "logs/hebo.error.log") 
                          (.setMaxFileSize "20MB")
                          (.setThreshold Level/ERROR))]
    (doto  
      (Logger/getLogger "hebo")
      (.addAppender hebo-info)
      (.addAppender hebo-error))
  ))
