(ns pallet.crate.zookeeper
  "Crate to install and configure Apache zookeeper."
  (:require
   [pallet.action.directory :as directory]
   [pallet.action.file :as file]
   [pallet.action.remote-directory :as remote-directory]
   [pallet.action.remote-file :as remote-file]
   [pallet.action.service :as service]
   [pallet.action.user :as user]
   [pallet.argument :as argument]
   [pallet.compute :as compute]
   [pallet.session :as session]
   [pallet.stevedore :as stevedore]
   [clojure.string :as string])
  (:use
   [pallet.core :only [server-spec]]
   [pallet.parameter
    :only [assoc-target-settings get-node-settings get-target-settings]]
   [pallet.phase :only [phase-fn]]
   [pallet.crate-install :only [install install-strategy]]
   [pallet.version-dispatch
    :only [defmulti-version-crate multi-version-session-method]]
   pallet.thread-expr))

;;; # Settings
(def default-settings
  {:version "3.4.4"
   :install-path "/usr/local/zookeeper-%s"
   :log-path "/var/log/zookeeper"
   :data-path "/var/zookeeper"
   :user "zookeeper"
   :owner "zookeeper"
   :group "zookeeper"
   :config {:tickTime 2000
            :clientPort 2181
            :quorumPort 2888
            :electionPort 3888
            :initLimit 10
            :syncLimit 5}
   :dist-url "http://www.apache.org/dist"
   :download-path "%s/zookeeper/zookeeper-%2$s/zookeeper-%2$s.tar.gz"})

(defn download-urls
  "Returns a computed download url and md5."
  [{:keys [dist-url download-path version] :as settings}]
  (let [url (format download-path dist-url version)]
    [url (str url ".md5")]))

(defn computed-defaults
  [{:keys [data-path install-path log-path version] :as settings}]
  (let [[url md5] (download-urls settings)
        settings (merge {:tx-log-path (format "%s/txlog" log-path)
                         :home (format install-path version)
                         :download-url url
                         :download-md5 md5}
                        settings)
        settings (merge {:config-path (str (:home settings) "/conf")}
                        settings)]
    (update-in settings [:config]
               merge {:dataDir data-path
                      :dataLogDir (:tx-log-path settings)})))

(defmulti-version-crate install-settings [version session settings])

(multi-version-session-method
 install-settings {:os :linux}
 [os os-version version session settings]
 (let [{:keys [download-url download-md5] :as settings}
       (install-strategy settings)]
   (cond
    (:strategy settings) settings
    :else (assoc settings :install-strategy ::remote-directory
                 :remote-directory {:url download-url :md5-url download-md5}))))

(defn zookeeper-settings
  [session {:keys [instance-id] :as settings}]
  (let [settings (->> settings
                      (merge default-settings)
                      computed-defaults)]
    (assoc-target-settings
     session :zookeeper instance-id
     (install-settings session (:version settings) settings))))

;;; ## Install

;;; Install via download
(defmethod install ::remote-directory
  [session facility instance-id]
  (let [{:keys [remote-directory home user owner group log-path tx-log-path
                config-path data-path]}
        (get-target-settings session :zookeeper instance-id)]
    (->
     session
     (user/group group :system true)
     (user/user user :system true :group group)
     (apply-map->
      remote-directory/remote-directory
      home
      (merge
       {:unpack :tar :tar-options "xz"
        :owner user :group group}
       remote-directory))
     (directory/directory log-path :owner user :group group :mode "0755")
     (directory/directory tx-log-path :owner user :group group :mode "0755")
     (directory/directory config-path :owner user :group group :mode "0755")
     (directory/directory data-path :owner user :group group :mode "0755")
     (remote-file/remote-file
      (format "%s/log4j.properties" config-path)
      :remote-file (format "%s/conf/log4j.properties" home)
      :owner user :group group :mode "0644")
     (file/sed
      (format "%s/log4j.properties" config-path)
      {"log4j.rootLogger=INFO, CONSOLE"
       "log4j.rootLogger=INFO, ROLLINGFILE"
       "log4j.appender.ROLLINGFILE.File=zookeeper.log"
       (format "log4j.appender.ROLLINGFILE.File=%s/zookeeper.log" log-path)}
      :seperator "|"))))

(defn install-zookeeper
  "Install zookeper."
  [session & {:keys [instance-id]}]
  (install session :zookeeper instance-id))

(defn zookeeper-init
  "Create an init file for zookeeper."
  [session & {:keys [instance-id]}]
  (let [{:keys [home user owner group log-path tx-log-path config-path
                data-path no-service-enable]}
        (get-target-settings session :zookeeper instance-id)]
    (->
     session
     (service/init-script
      "zookeeper"
      :link (format "%s/bin/zkServer.sh" home)
      :overwrite-changes true)
     (file/sed
      (format "%s/bin/zkServer.sh" home)
      {"# chkconfig:.*" ""
       "# description:.*" ""
       "# by default we allow local JMX connections"
       "# by default we allow local JMX connections\\n# chkconfig: 2345 20 80\\n# description: zookeeper"
       "ZOOBIN=\"${BASH_SOURCE-$0}\""
       (str "ZOOBIN=\"" home "/bin/zkServer.sh\"")}
      :quote-with "'")
     (if-not-> no-service-enable
               (service/service
                "zookeeper" :action :start-stop
                :sequence-start "20 2 3 4 5"
                :sequence-stop "20 0 1 6")))))


(defn config-content
  "Generate the content of a zookeeper configuration file."
  [session config nodes instance-id]
  (clojure.tools.logging/infof "ZK Config %s" config)
  (str (string/join
        \newline
        (map #(format "%s=%s" (name (key %)) (val %))
             (dissoc config :electionPort :quorumPort)))
       \newline
       (when (> (count nodes) 1)
         (string/join
          \newline
          (map #(let [config (get-node-settings
                              session
                              %1 :zookeeper instance-id)]
                  (format "server.%s=%s:%s:%s"
                          %2
                          (compute/private-ip %1)
                          (:quorumPort config 2888)
                          (:electionPort config 3888)))
               nodes
               (range 1 (inc (count nodes))))))))

(defn id-content
  "Generate the content of a zookeeper id file."
  [target-ip nodes]
  (str (some #(and (= target-ip (second %)) (first %))
                          (map #(vector %1 (compute/primary-ip %2))
                               (range 1 (inc (count nodes)))
                               nodes))))

(defn zookeeper-config
  "Create a zookeeper configuration file.  We sort by name to preserve sequence
   across invocations."
  [session & {:keys [instance-id]}]
  (let [{:keys [home user owner group log-path tx-log-path config-path
                data-path config]}
        (get-target-settings session :zookeeper instance-id)

        target-name (session/target-name session)
        target-ip (session/target-ip session)
        nodes (sort-by compute/hostname (session/nodes-in-group session))]
    (->
     session
     (remote-file/remote-file
      (format "%s/zoo.cfg" config-path)
      :content (config-content session config nodes instance-id)
      :owner owner :group group :mode "0644")
     (remote-file/remote-file
      (format "%s/myid" data-path)
      :content (id-content target-ip nodes)
      :owner owner :group group :mode "0644"))))

(defn zookeeper [{:keys [instance-id] :as settings}]
  (server-spec
   :phases {:settings (phase-fn
                       (zookeeper-settings settings))
            :configure (phase-fn
                        (install-zookeeper :instance-id instance-id)
                        (zookeeper-config :instance-id instance-id)
                        (zookeeper-init :instance-id instance-id))
            :restart-zookeeper (phase-fn
                                (pallet.action.service/service
                                 "zookeeper" :action :restart))}))
