# Beta schema steps

Apply these checked-in SQL files in numeric order when creating a fresh database. During the beta prototype, rebuilding the database and re-ingesting source history is the default rollback/cutover strategy; production-style compatibility migrations are not required.

Files must remain safe to rerun during bootstrap and must carry the constraints needed by the active design. Do not edit a step after other shared environments depend on it; add the next sequence instead.
