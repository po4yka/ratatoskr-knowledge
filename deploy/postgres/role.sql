-- Run as a PostgreSQL administrator (a superuser) before the first Knowledge start, after
-- `01-database-and-roles.sql` has created the `ratatoskr` database. Set the password separately with
-- `\password ratatoskr_knowledge`; no credential belongs in this repository.
--
-- The `vector` line is the reason an administrator runs this file: pgvector is not a trusted
-- extension, so `create extension if not exists vector` in `schema.sql` fails with
-- `permission denied to create extension "vector"` for the unprivileged service role. Installing it
-- here first makes that statement a no-op at service start.

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'ratatoskr_knowledge') then
        create role ratatoskr_knowledge login;
    end if;
end
$$;

alter role ratatoskr_knowledge nosuperuser nocreatedb nocreaterole noreplication nobypassrls;
grant connect, create on database ratatoskr to ratatoskr_knowledge;

\connect ratatoskr
create extension if not exists vector;
-- USAGE on schema public, never CREATE. Platform's 01-database-and-roles.sql runs
-- `revoke all on schema public from public`, and `create extension vector` above puts the `vector`
-- type and its operators in public, so without this grant the service role cannot resolve
-- `vector(1536)` and applying schema.sql at the first start fails with `type "vector" does not
-- exist`. Every object the service owns lives in the `knowledge` schema, which the role creates
-- itself through `create` on the database; it needs no right to add objects to public.
grant usage on schema public to ratatoskr_knowledge;
