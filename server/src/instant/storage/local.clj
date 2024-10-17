(ns instant.storage.local
  (:require [clojure.java.io :as io]))

;; Directory where files will be stored locally.
(def storage-dir "/var/lib/instantdb/files")

;; Ensure the storage directory exists
(when-not (.exists (io/file storage-dir))
  (.mkdirs (io/file storage-dir)))

;; List all files in the storage directory
(defn list-files []
  (map #(.getName %) (.listFiles (io/file storage-dir))))

(defn format-object [file]
  {:key (.getName file)
   :name (.getName file)
   :size (.length file)
   :owner "local-user" ;; Placeholder, since local files might not have an owner.
   :etag nil ;; No etag available for local files.
   :last_modified (.lastModified file)})
   
;; Get a file's content as an input stream
(defn get-file [filename]
  (io/input-stream (str storage-dir "/" filename)))

;; Upload a file by writing content to it
(defn put-file [filename content]
  (let [filepath (str storage-dir "/" filename)]
    (with-open [w (io/output-stream filepath)]
      (.write w content))))

;; Delete a file
(defn delete-file [filename]
  (let [filepath (str storage-dir "/" filename)]
    (io/delete-file filepath)))