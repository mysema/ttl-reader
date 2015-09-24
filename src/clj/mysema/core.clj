(ns mysema.core
  (:import (java.util ArrayList)))


(defn lookback-map
  "Map trandsducer which calls f with previous value as second argument"
  [f]
  (fn [rf]
    (let [pv (volatile! nil)]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (let [v (f input @pv)]
           (vreset! pv v)
           (rf result v)))))))

;; Not so usefule
(defn lookback-map-with-rf
  "Map trandsducer which calls f with previous value as second argument and gives
  function access to reducing function"
  [f]
  (fn [rf]
    (let [pv (volatile! nil)]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
         (let [v (f input @pv rf result)]
           (vreset! pv v)
           (rf result v)))))))

(defn map-with-rf
  "Map trandsducer which calls f with previous value as second argument"
  [f]
  (fn [rf]
      (fn
        ([] (rf))
        ([result] (rf result))
        ([result input]
           (rf result (f input rf result))))))


;
;(extend-protocol clojure.core.protocols.CollReduce
;  java.io.BufferedReader
;  (coll-reduce
;    ([rdr f]
;     (let [line (.readLine rdr)]
;       (if line
;         (loop [ret line]
;           (if line
;             (let [ret (f)])))
;         (f)
;         )
;
;       )
;     (loop [line (.readLine rdr)]
;       (if line
;         (let [ret (f )]))))
;    ([rdr f val]
;    (loop []))
;

  ;
  ;  (let [s (.s str-seq)]
  ;    (loop [i (.i str-seq)
  ;           val val]
  ;      (if (< i (.length s))
  ;        (let [ret (f val (.charAt s i))]
  ;          (if (reduced? ret)
  ;            @ret
  ;            (recur (inc i) ret)))
  ;        val))))
  ;)

  ;(when-let [line (.readLine rdr)]
  ;  (cons line (lazy-seq (line-seq rdr)))))

(defn lookback-partition-by
  "Returns a stateful transducer which partitions the collection
  when predicate returns truthy value.
  Predicate function is called with two values, current item
  and previous item."
  [pred]
  (fn [rf]
    (let [a (ArrayList.)
          pi (volatile! nil)]
      (fn
        ([] (rf))
        ([result]
         (let [result (if (.isEmpty a)
                        result
                        (let [v (vec (.toArray a))]
                          (.clear a)
                          (unreduced (rf result v))))]
           (rf result)))
        ([result input]
         (let [pitem @pi]
           (vreset! pi input)
           (if-not (pred input pitem)
             (do
               (.add a input)
               result)
             (let [v (vec (.toArray a))]
               (.clear a)
               (let [ret (rf result v)]
                 (when-not (reduced? ret)
                   (.add a input))
                 ret)))))))))
