(ns metabase.driver.sparql.dimensions
  "Post-sync hook that writes Metabase `dimension` rows from SHACL
   `metabase:displayValueProperty` declarations.

   Why this exists:
     `describe-table` returns per-field metadata, but Metabase's FK display-value
     (a.k.a. external remapping) lives in the `metabase_field.dimension` app-DB
     table, not in field metadata. There is no driver-API surface for writing
     `Dimension` rows during sync. We hook the post-sync event
     `:event/sync-metadata-end`, walk the SHACL shapes for the SPARQL database,
     and upsert one `Dimension` row per (FK field, display-value field) pair.

   Coupling: this ns reaches into two Metabase-internal namespaces
   (`metabase.events.core`, the `:model/Dimension` / `:model/Field` toucan
   models). If `notify-database-updated` ever grows a `:sparql`-aware variant
   in the driver-API, this should migrate there."
  (:require
   [metabase.driver.sparql.shacl :as shacl]
   [metabase.driver.sparql.uri :as uri]
   [metabase.events.core :as events]
   [metabase.util.log :as log]
   [methodical.core :as methodical]
   [toucan2.core :as t2]))

(defn- shacl-shapes
  "Fetch SHACL shapes for `database`. Returns `nil` when no URL configured."
  [database]
  (when-let [url (-> database :details :shacl-url)]
    (try
      (shacl/metadata url
                      (or (-> database :details :default-language) "")
                      {})
      (catch Exception t
        (log/warnf t "[sparql.dimensions] Failed to load SHACL document at %s" url)
        nil))))

(defn- field-for
  "Look up a Field row by (database-id, short table name, short column name)."
  [db-id table-name field-name]
  (t2/select-one [:model/Field :id :name :table_id]
                 {:select    [:f.id :f.name :f.table_id]
                  :from      [[:metabase_field :f]]
                  :left-join [[:metabase_table :t] [:= :t.id :f.table_id]]
                  :where     [:and
                              [:= :t.db_id db-id]
                              [:= :t.name table-name]
                              [:= :f.name field-name]
                              [:= :f.active true]]}))

(defn- upsert-dimension!
  "Idempotently set or update the external-remapping `Dimension` row for `field-id`."
  [field-id display-name human-readable-field-id]
  (if-let [existing (t2/select-one :model/Dimension :field_id field-id)]
    (when (or (not= :external (:type existing))
              (not= human-readable-field-id (:human_readable_field_id existing))
              (not= display-name (:name existing)))
      (t2/update! :model/Dimension (:id existing)
                  {:type                    :external
                   :name                    display-name
                   :human_readable_field_id human-readable-field-id})
      (log/infof "[sparql.dimensions] Updated dimension for field %s -> %s"
                 field-id human-readable-field-id))
    (do
      (t2/insert! :model/Dimension
                  {:field_id                field-id
                   :type                    :external
                   :name                    display-name
                   :human_readable_field_id human-readable-field-id})
      (log/infof "[sparql.dimensions] Created dimension for field %s -> %s"
                 field-id human-readable-field-id))))

(defn sync-display-dimensions!
  "Walk SHACL shapes for `database` and upsert a `Dimension` row for every
   property that declares `metabase:displayValueProperty` and points at an
   `sh:class` target. No-op when no SHACL URL is configured. Tolerant of
   target tables / fields that haven't been synced yet (those are skipped at
   debug level — the next sync resolves them)."
  [database]
  (let [db-id         (:id database)
        default-graph (-> database :details :default-graph)
        shapes        (shacl-shapes database)]
    (doseq [shape shapes
            prop  (:properties shape)
            :let  [display-uri (:display-value-property prop)
                   fk-class    (:fk-target-class prop)]
            :when (and display-uri fk-class)]
      (let [src-table-name (uri/shorten-uri (:class-uri shape) default-graph)
            src-field-name (uri/shorten-uri (:property-uri prop) default-graph)
            tgt-table-name (uri/shorten-uri fk-class default-graph)
            tgt-field-name (uri/shorten-uri display-uri default-graph)
            src-field      (field-for db-id src-table-name src-field-name)
            tgt-field      (field-for db-id tgt-table-name tgt-field-name)]
        (cond
          (not src-field)
          (log/debugf "[sparql.dimensions] Skipping: source field %s.%s not synced yet"
                      src-table-name src-field-name)

          (not tgt-field)
          (log/debugf "[sparql.dimensions] Skipping: display field %s.%s not synced yet"
                      tgt-table-name tgt-field-name)

          :else
          (try
            (upsert-dimension! (:id src-field) src-field-name (:id tgt-field))
            (catch Throwable t
              (log/warnf t "[sparql.dimensions] Failed to upsert dimension for %s.%s"
                         src-table-name src-field-name))))))))

(derive ::sparql-sync-end :metabase/event)
(derive :event/sync-metadata-end ::sparql-sync-end)

(methodical/defmethod events/publish-event! ::sparql-sync-end
  "After SPARQL metadata sync finishes, materialize SHACL displayValueProperty
   declarations as Metabase Dimension rows. No-op for non-SPARQL databases."
  [_topic {:keys [database_id] :as _event}]
  (try
    (when-let [database (and database_id
                             (t2/select-one [:model/Database :id :engine :details]
                                            :id database_id))]
      (when (= :sparql (keyword (:engine database)))
        (sync-display-dimensions! database)))
    (catch Throwable t
      (log/warnf t "[sparql.dimensions] Error handling sync-metadata-end event"))))
