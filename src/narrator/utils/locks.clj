(ns narrator.utils.locks
  (:use
    [potemkin])
  (:import
    [java.util.concurrent.locks
     ReentrantLock
     ReentrantReadWriteLock]))

;;;

(definterface+ ILock
  (acquire [_])
  (acquire-exclusive [_])
  (release [_])
  (release-exclusive [_])
  (try-acquire [_])
  (try-acquire-exclusive [_]))

;;;

(deftype+ AsymmetricLock [^ReentrantReadWriteLock lock]
  ILock
  (acquire [this]
    (-> lock .readLock .lock))
  (release [this]
    (-> lock .readLock .unlock))
  (acquire-exclusive [this]
    (-> lock .writeLock .lock))
  (release-exclusive [this]
    (-> lock .writeLock .unlock))
  (try-acquire [_]
    (-> lock .readLock .tryLock))
  (try-acquire-exclusive [_]
    (-> lock .writeLock .tryLock)))

(defn asymmetric-lock []
  (AsymmetricLock. (ReentrantReadWriteLock. false)))

(deftype+ Lock [^ReentrantLock lock]
  ILock
  (acquire-exclusive [_] (.lock lock))
  (release-exclusive [_] (.unlock lock))
  (try-acquire-exclusive [_] (.tryLock lock)))

(defn lock []
  (Lock. (ReentrantLock. false)))

;;;

(defmacro with-lock [lock & body]
  `(let [lock# ~lock]
     (do
       (acquire lock#)
       (try
         ~@body
         (finally
           (release lock#))))))

(defmacro with-exclusive-lock [lock & body]
  `(let [lock# ~lock]
     (do
       (acquire-exclusive lock#)
       (try
         ~@body
         (finally
           (release-exclusive lock#))))))
