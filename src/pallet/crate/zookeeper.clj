(ns pallet.crate.zookeeper
  "Crate to install and configure Apache zookeeper."
  (:require
   [clojure.pprint :refer [pprint]]
   [clojure.string :as string]
   [clojure.tools.logging :as log]
   [pallet.actions :as actions]
   [pallet.api :as api]
   [pallet.core.session :as session]
   [pallet.crate :as crate]
   [pallet.crate-install :as crate-install]
   [pallet.node :as node]
   [pallet.stevedore :as stevedore]
   [pallet.utils :refer [apply-map]]
   [pallet.version-dispatch
    :refer [defmethod-version-plan
            defmulti-version-plan]]))

;;; # Settings
(def default-settings
  {:version "3.4.5"
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

(defmulti-version-plan install-settings [version settings])

(defmethod-version-plan
 install-settings {:os :linux}
 [os os-version version settings]
 (let [{:keys [download-url download-md5]} settings]
   (cond
    (:strategy settings) settings
    :else (assoc settings :install-strategy ::remote-directory
                 :remote-directory {:url download-url :md5-url download-md5}))))

(crate/defplan settings
  [{:keys [instance-id] :as settings}]
  (let [settings (->> settings
                      (merge default-settings)
                      computed-defaults)
        is (install-settings (:version settings) settings)]
    (crate/assoc-settings :zookeeper is {:instance-id instance-id})))

;;; ## Install

;;; Install via download
(crate/defmethod-plan crate-install/install ::remote-directory
  [facility instance-id]
  (let [{:keys [remote-directory home user owner group log-path tx-log-path
                config-path data-path]}
        (crate/get-settings :zookeeper instance-id)]
    (actions/group group :system true)
    (actions/user user :system true :group group)
    (apply-map
     actions/remote-directory
     home
     (merge
      {:unpack :tar :tar-options "xz"
       :owner user :group group}
      remote-directory))
    (actions/directory log-path :owner user :group group :mode "0755")
    (actions/directory tx-log-path :owner user :group group :mode "0755")
    (actions/directory config-path :owner user :group group :mode "0755")
    (actions/directory data-path :owner user :group group :mode "0755")
    (actions/remote-file
     (format "%s/log4j.properties" config-path)
     :remote-file (format "%s/conf/log4j.properties" home)
     :owner user :group group :mode "0644")
    (actions/sed
     (format "%s/log4j.properties" config-path)
     {"log4j.rootLogger=INFO, CONSOLE"
      "log4j.rootLogger=INFO, ROLLINGFILE"
      "log4j.appender.ROLLINGFILE.File=zookeeper.log"
      (format "log4j.appender.ROLLINGFILE.File=%s/zookeeper.log" log-path)}
     :seperator "|")))

(crate/defplan install
  "Install zookeper."
  [& {:keys [instance-id]}]
  (crate-install/install :zookeeper instance-id))

(crate/defplan init-script
  "Create an init file for zookeeper."
  [& {:keys [instance-id]}]
  (let [{:keys [home user owner group log-path tx-log-path config-path
                data-path no-service-enable]}
        (crate/get-settings :zookeeper instance-id)]
    (actions/service-script
     "zookeeper"
     :link (format "%s/bin/zkServer.sh" home)
     :force true)
    (actions/sed
     (format "%s/bin/zkServer.sh" home)
     {"# chkconfig:.*" ""
      "# description:.*" ""
      "# by default we allow local JMX connections"
      "# by default we allow local JMX connections\\n# chkconfig: 2345 20 80\\n# description: zookeeper"
      "ZOOBIN=\"${BASH_SOURCE-$0}\""
      (str "ZOOBIN=\"" home "/bin/zkServer.sh\"")}
     :quote-with "'")
    (if-not no-service-enable
      (actions/service
       "zookeeper" :action :start-stop
       :sequence-start "20 2 3 4 5"
       :sequence-stop "20 0 1 6"))))


(crate/defplan config-content
  "Generate the content of a zookeeper configuration file."
  [config nodes instance-id]
  (clojure.tools.logging/infof "ZK Config %s" config)
  (str (string/join
        \newline
        (map #(format "%s=%s" (name (key %)) (val %))
             (dissoc config :electionPort :quorumPort)))
       \newline
       (when (> (count nodes) 1)
         (string/join
          \newline
          (map #(let [config (crate/get-node-settings
                              %1 :zookeeper instance-id)]
                  (format "server.%s=%s:%s:%s"
                          %2
                          (node/private-ip %1)
                          (:quorumPort config 2888)
                          (:electionPort config 3888)))
               nodes
               (range 1 (inc (count nodes))))))))

(defn id-content
  "Generate the content of a zookeeper id file."
  [target-ip nodes]
  (str (some #(and (= target-ip (second %)) (first %))
                          (map #(vector %1 (node/primary-ip %2))
                               (range 1 (inc (count nodes)))
                               nodes))))

(crate/defplan config
  "Create a zookeeper configuration file.  We sort by name to preserve sequence
   across invocations."
  [& {:keys [instance-id]}]
  (let [{:keys [home user owner group log-path tx-log-path config-path
                data-path config]}
        (crate/get-settings :zookeeper instance-id)
        target-name (crate/target-name)
        target-ip (node/primary-ip (crate/target-node)) ;; (session/target-ip (session/session))
        nodes (sort-by node/hostname (crate/nodes-in-group))]
    (actions/remote-file
     (format "%s/zoo.cfg" config-path)
     :content (config-content config nodes instance-id)
     :owner owner :group group :mode "0644")
    (actions/remote-file
     (format "%s/myid" data-path)
     :content (id-content target-ip nodes)
     :owner owner :group group :mode "0644")))

(defn zookeeper [{:keys [instance-id] :as settings}]
  (api/server-spec
   :phases {:settings (api/plan-fn
                       (settings settings))
            :configure (api/plan-fn
                        (install :instance-id instance-id)
                        (config :instance-id instance-id)
                        (init-script :instance-id instance-id))
            :restart-zookeeper (api/plan-fn
                                (actions/service
                                 "zookeeper" :action :restart))}))

(def server-spec zookeeper)
