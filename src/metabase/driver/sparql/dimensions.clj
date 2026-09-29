(ns metabase.driver.sparql.dimensions
  "Post-sync hook that writes what `describe-table` cannot:

     - Metabase `dimension` rows from SHACL `sban:displayValueProperty`
       declarations;
     - readable display names for fields named by a full URI (see
       [[sync-display-names!]]).

   Why this exists:
     `describe-table` returns per-field metadata, but Metabase's FK display-value
     (a.k.a. external remapping) lives in the `dimension` app-DB
     table, not in field metadata. There is no driver-API surface for writing
     `Dimension` rows during sync. We hook the post-sync event
     `:event/sync-metadata-end`, walk the SHACL shapes for the SPARQL database,
     and upsert one `Dimension` row per (FK field, display-value field) pair.

   Coupling: this ns reaches into Metabase internals (`metabase.events.core`,
   the `:model/Dimension` / `:model/Field` / `:model/Database` toucan models,
   and the raw `metabase_field` / `metabase_table` tables)."
  (:require
   [metabase.driver.sparql.database :as database]
   [metabase.driver.sparql.uri :as uri]
   [metabase.events.core :as events]
   [metabase.models.humanization :as humanization]
   [metabase.util.log :as log]
   [methodical.core :as methodical]
   [toucan2.core :as t2]))

(defn- field-for
  "Return the active Field `{:id :name :display_name :table_id}` named
   `field-name` in the active table `table-name` of database `db-id` (both
   short names), or nil."
  [db-id table-name field-name]
  (t2/select-one [:model/Field :id :name :display_name :table_id]
                 {:select    [:f.id :f.name :f.display_name :f.table_id]
                  :from      [[:metabase_field :f]]
                  :left-join [[:metabase_table :t] [:= :t.id :f.table_id]]
                  :where     [:and
                              [:= :t.db_id db-id]
                              [:= :t.name table-name]
                              [:= :t.active true]
                              [:= :f.name field-name]
                              [:= :f.active true]]}))

(defn- upsert-dimension!
  "Create or update the external-remapping `Dimension` of `field-id` so it
   shows `human-readable-field-id` under `display-name`. Writes nothing when
   the row already matches."
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
  "Upsert a `Dimension` row for every SHACL property of `database` that
   declares `sban:displayValueProperty` and points at an `sh:class`
   target. No-op outside the `shacl` sync strategy, or when the document
   cannot be loaded. Fields not synced yet are skipped at debug level (the
   next sync resolves them); a failed upsert is logged and skipped."
  [database]
  (let [db-id  (:id database)
        naming (uri/naming-context (:details database))
        shapes (database/shacl-shapes database)]
    (doseq [shape shapes
            prop  (:properties shape)
            :let  [display-uri (:display-value-property prop)
                   fk-class    (:fk-target-class prop)]
            :when (and display-uri fk-class)]
      (let [src-table-name (uri/shorten-uri (:class-uri shape) naming)
            src-field-name (uri/shorten-uri (:property-uri prop) naming)
            tgt-table-name (uri/shorten-uri fk-class naming)
            tgt-field-name (uri/shorten-uri display-uri naming)
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
            ;; Metabase heads the remapped column with the Dimension's name,
            ;; so it takes the FK field's display name, like a remap set in
            ;; Table Metadata.
            (upsert-dimension! (:id src-field) (:display_name src-field) (:id tgt-field))
            (catch Exception t
              (log/warnf t "[sparql.dimensions] Failed to upsert dimension for %s.%s"
                         src-table-name src-field-name))))))))

(defn- readable-display-name
  "Return the display name for a field named by a full URI (a property outside the
   Default Graph and the namespace prefixes, e.g. rdfs:label): its humanized
   local name (\"Label\"). nil for other fields, for a field whose display
   name is no longer sync's default (one an admin renamed), and when nothing
   would change (a URI without a `/` or `#` local name)."
  [{:keys [name display_name]}]
  (when (and (uri/has-scheme? name)
             (= display_name (humanization/name->human-readable-name name)))
    (let [readable (humanization/name->human-readable-name (uri/local-name name))]
      (when (not= readable display_name)
        readable))))

(defn sync-display-names!
  "Give fields named by a full URI a readable display name. Sync ignores a
   driver's field display name and humanizes the name itself, which for a full
   URI reads \"Http://www.w3.org/2000/01/rdf Schema#label\"."
  [database]
  (doseq [field (t2/select [:model/Field :id :name :display_name]
                           {:select    [:f.id :f.name :f.display_name]
                            :from      [[:metabase_field :f]]
                            :left-join [[:metabase_table :t] [:= :t.id :f.table_id]]
                            :where     [:and [:= :t.db_id (:id database)] [:= :t.active true] [:= :f.active true]]})
          :let  [display-name (readable-display-name field)]
          :when display-name]
    (t2/update! :model/Field (:id field) {:display_name display-name})))

;; If the driver API ever gains a post-sync hook, this should migrate there.
(derive ::sparql-sync-end :metabase/event)
(derive :event/sync-metadata-end ::sparql-sync-end)

(defn- sync-end!
  "Run the post-sync steps for the database `database-id` when it is a SPARQL
   database. Display names run first, so a Dimension takes the readable name
   of a full-URI FK field. Each step has its own try, so one failing does not
   skip the other."
  [database-id]
  (when-let [database (and database-id
                           (t2/select-one [:model/Database :id :engine :details]
                                          :id database-id))]
    (when (= :sparql (keyword (:engine database)))
      (doseq [[step-name step] [["display names" sync-display-names!]
                                ["display-value dimensions" sync-display-dimensions!]]]
        (try
          (step database)
          (catch Exception t
            (log/warnf t "[sparql.dimensions] Post-sync step %s failed for database %s"
                       step-name database-id)))))))

(methodical/defmethod events/publish-event! ::sparql-sync-end
  "After SPARQL metadata sync finishes, materialize SHACL displayValueProperty
   declarations as Metabase Dimension rows and fix full-URI display names.
   No-op for non-SPARQL databases."
  [_topic {:keys [database_id] :as _event}]
  (try
    (sync-end! database_id)
    (catch Exception t
      (log/warnf t "[sparql.dimensions] Error handling sync-metadata-end event"))))
