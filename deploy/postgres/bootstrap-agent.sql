-- Local development only. Run as the Executor PostgreSQL administrator.
-- Existing roles/databases are preserved; no password or ownership is changed.
SELECT 'CREATE ROLE agent LOGIN PASSWORD ''agent'''
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agent')
\gexec

SELECT 'CREATE DATABASE agent OWNER agent'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'agent')
\gexec

\connect agent
CREATE EXTENSION IF NOT EXISTS vector;
