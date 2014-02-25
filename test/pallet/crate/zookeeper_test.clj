(ns pallet.crate.zookeeper-test
  (:use clojure.test)
  (:require
   [pallet.action :as action]
   [pallet.actions :as actions]
   [pallet.build-actions :as build-actions]
   [pallet.api :as api]
   [pallet.crate.automated-admin-user :as automated-admin-user]
   [pallet.crate.java :as java]
   [pallet.crate.network-service :as network-service]
   [pallet.crate.zookeeper :as zookeeper]
   [pallet.live-test :as live-test]
   [pallet.test-utils :as test-utils]))

(deftest zookeeper-test
  (is                                   ; just check for compile errors for now
   (build-actions/build-actions
    {:server {:group-name "tag"
              :image {:os-family :ubuntu}
              :node (test-utils/make-node "tag")}}
    (zookeeper/settings {})
    (zookeeper/install)
    (zookeeper/config)
    (zookeeper/init-script))))


(def zookeeper-test-spec
  (api/server-spec
   :extends [(zookeeper/server-spec {})]
   :phases
   {:bootstrap (api/plan-fn
                (actions/package-manager :update))
    :settings (api/plan-fn
               (java/settings {})
               (zookeeper/settings {}))
    :configure (api/plan-fn
                (java/install {})
                (zookeeper/install)
                (zookeeper/config)
                (zookeeper/init-script)
                (actions/service "zookeeper" :action :restart))
    :test (api/plan-fn
             (network-service/wait-for-port-listen 2181)
             (actions/exec-checked-script
              "check zookeeper"
              (println "zookeeper ruok")
              (pipe (println "ruok") ("nc" -q 2 "localhost" 2181))
              (println "zookeeper stat ")
              (pipe (println "stat") ("nc" -q 2 "localhost" 2181))
              (println "zookeeper dump ")
              (pipe (println "dump") ("nc" -q 2 "localhost" 2181))
              (println "zookeeper imok ")
              (= "imok"
                 @(pipe (println "ruok")
                        ("nc" -q 2 "localhost" 2181)))))}))
