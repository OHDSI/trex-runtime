(ns trexsql.webapi.plugin
  (:require [trexsql.core :as core])
  (:gen-class
   :name org.trex.webapi.TrexSQLPlugin
   :implements [org.ohdsi.webapi.plugins.WebApiPlugin]
   :init init-state
   :state state))

(defn -init-state [] [[] {}])
(defn -getId [this] "trexsql")
(defn -getName [this] "TrexSQL Vocabulary Cache")
(defn -getVersion [this] "0.1.23")
(defn -isActive [this]
  (boolean (some? @core/current-database)))
