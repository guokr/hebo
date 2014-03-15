(ns hebo.util-test
  (:use [clojure.test]
        [clj-time.coerce :only [to-long from-long]]
        [clj-time.core :exclude [second extend]]
        [clj-time.format]
        [hebo.util]))
  
(deftest granu-test
  (testing "granu used for same-granu is error"
    (let [daily-granu (granularity-is :daily)
          data-timestamp (daily-granu "2013-01-01T23:01:25+0800")
          input-timestamp (datetime-to-timestamp (from-time-zone (date-time 2013 1 1) (default-time-zone)))]
       (is (= input-timestamp data-timestamp)))))


(deftest get-fine-granu-test
  (is (= :daily (get-fine-granu :daily :monthly)))
  (is (= :daily (get-fine-granu :daily :monthly :yearly)))
  (is (= :hourly (get-fine-granu :daily :monthly :yearly :hourly))))
