-- Every relation the warehouse DDL creates, as one JSON array: columns, primary key,
-- unique constraints, foreign keys, view dependencies, partitioning and index count.
-- Read-only. Feeds docs/warehouse/erd.md; see its "Regenerating" section.
--
--   psql -At -f docs/warehouse/catalog.sql > catalog.json

WITH rels AS (
  SELECT c.oid, n.nspname AS schema, c.relname AS name, c.relkind, c.relispartition,
         pg_get_partkeydef(c.oid) AS partkey,
         (SELECT pn.nspname||'.'||p.relname FROM pg_inherits i JOIN pg_class p ON p.oid=i.inhparent JOIN pg_namespace pn ON pn.oid=p.relnamespace WHERE i.inhrelid=c.oid LIMIT 1) AS parent,
         obj_description(c.oid,'pg_class') AS comment
  FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE c.relkind IN ('r','p','v','m') AND n.nspname NOT IN ('pg_catalog','information_schema','public','topology','tiger','tiger_data','pg_toast')
)
SELECT json_agg(json_build_object(
  'schema',schema,'name',name,'kind',relkind,'is_partition',relispartition,'partkey',partkey,'parent',parent,
  'comment', comment,
  'columns',(SELECT json_agg(json_build_object('name',a.attname,'type',format_type(a.atttypid,a.atttypmod),'nn',a.attnotnull) ORDER BY a.attnum) FROM pg_attribute a WHERE a.attrelid=r.oid AND a.attnum>0 AND NOT a.attisdropped),
  'pk',(SELECT json_agg(a.attname ORDER BY array_position(con.conkey,a.attnum)) FROM pg_constraint con JOIN pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=ANY(con.conkey) WHERE con.conrelid=r.oid AND con.contype='p'),
  'uniques',(SELECT json_agg(pg_get_constraintdef(con.oid)) FROM pg_constraint con WHERE con.conrelid=r.oid AND con.contype='u'),
  'fks',(SELECT json_agg(json_build_object('cols',(SELECT json_agg(a.attname) FROM unnest(con.conkey) k JOIN pg_attribute a ON a.attrelid=con.conrelid AND a.attnum=k),'ref',fn.nspname||'.'||f.relname,'refcols',(SELECT json_agg(a.attname) FROM unnest(con.confkey) k JOIN pg_attribute a ON a.attrelid=con.confrelid AND a.attnum=k))) FROM pg_constraint con JOIN pg_class f ON f.oid=con.confrelid JOIN pg_namespace fn ON fn.oid=f.relnamespace WHERE con.conrelid=r.oid AND con.contype='f'),
  'deps',(SELECT json_agg(DISTINCT dn.nspname||'.'||dc.relname) FROM pg_rewrite rw JOIN pg_depend d ON d.objid=rw.oid AND d.classid='pg_rewrite'::regclass JOIN pg_class dc ON dc.oid=d.refobjid JOIN pg_namespace dn ON dn.oid=dc.relnamespace WHERE rw.ev_class=r.oid AND dc.oid<>r.oid AND dc.relkind IN ('r','p','v','m')),
  'indexes',(SELECT count(*) FROM pg_index ix WHERE ix.indrelid=r.oid)
)) FROM rels r;
