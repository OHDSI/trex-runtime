-- TrexSQL Core Schema

CREATE SCHEMA IF NOT EXISTS trex;
SET search_path TO trex;

-- ── Auth tables (Better Auth) ────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS "user" (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  email TEXT NOT NULL UNIQUE,
  "emailVerified" BOOLEAN DEFAULT false,
  image TEXT,
  role TEXT DEFAULT 'user',
  banned BOOLEAN DEFAULT false,
  "banReason" TEXT,
  "banExpires" TIMESTAMPTZ,
  "mustChangePassword" BOOLEAN DEFAULT false,
  "deletedAt" TIMESTAMPTZ,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS session (
  id TEXT PRIMARY KEY,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  token TEXT NOT NULL UNIQUE,
  "expiresAt" TIMESTAMPTZ NOT NULL,
  "ipAddress" TEXT,
  "userAgent" TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS account (
  id TEXT PRIMARY KEY,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "accountId" TEXT NOT NULL,
  "providerId" TEXT NOT NULL,
  "accessToken" TEXT,
  "refreshToken" TEXT,
  "accessTokenExpiresAt" TIMESTAMPTZ,
  "refreshTokenExpiresAt" TIMESTAMPTZ,
  scope TEXT,
  "idToken" TEXT,
  password TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE ("providerId", "accountId")
);

CREATE TABLE IF NOT EXISTS verification (
  id TEXT PRIMARY KEY,
  identifier TEXT NOT NULL,
  value TEXT NOT NULL,
  "expiresAt" TIMESTAMPTZ NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS jwks (
  id TEXT PRIMARY KEY,
  "publicKey" TEXT NOT NULL,
  "privateKey" TEXT NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oauth_application (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  icon TEXT,
  metadata TEXT,
  "clientId" TEXT NOT NULL UNIQUE,
  "clientSecret" TEXT NOT NULL,
  "redirectURLs" TEXT NOT NULL,
  type TEXT DEFAULT 'web',
  disabled BOOLEAN DEFAULT false,
  "userId" TEXT REFERENCES "user"(id) ON DELETE SET NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oauth_access_token (
  id TEXT PRIMARY KEY,
  "accessToken" TEXT NOT NULL,
  "refreshToken" TEXT,
  "accessTokenExpiresAt" TIMESTAMPTZ NOT NULL,
  "refreshTokenExpiresAt" TIMESTAMPTZ,
  "clientId" TEXT NOT NULL,
  "userId" TEXT REFERENCES "user"(id) ON DELETE CASCADE,
  scopes TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oauth_consent (
  id TEXT PRIMARY KEY,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "clientId" TEXT NOT NULL,
  scopes TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS oauth_authorization_code (
  id TEXT PRIMARY KEY,
  code TEXT NOT NULL,
  "clientId" TEXT NOT NULL,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  scopes TEXT,
  "redirectURI" TEXT NOT NULL,
  "codeChallenge" TEXT,
  "codeChallengeMethod" TEXT,
  "expiresAt" TIMESTAMPTZ NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

-- ── Indexes ──────────────────────────────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_user_email ON "user"(email);
CREATE INDEX IF NOT EXISTS idx_user_deleted_at ON "user"("deletedAt") WHERE "deletedAt" IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_account_user_id ON account("userId");
CREATE UNIQUE INDEX IF NOT EXISTS idx_account_provider ON account("providerId", "accountId");
CREATE INDEX IF NOT EXISTS idx_session_user_id ON session("userId");
CREATE INDEX IF NOT EXISTS idx_session_token ON session(token);
CREATE INDEX IF NOT EXISTS idx_session_expires_at ON session("expiresAt");

-- ── Row Level Security ───────────────────────────────────────────────────────

ALTER TABLE "user" ENABLE ROW LEVEL SECURITY;
ALTER TABLE account ENABLE ROW LEVEL SECURITY;
ALTER TABLE session ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_users ON "user"
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_row ON "user"
  FOR ALL
  USING (id = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK (id = nullif(current_setting('app.user_id', true), ''));

CREATE POLICY admin_all_accounts ON account
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_accounts ON account
  FOR SELECT
  USING ("userId" = nullif(current_setting('app.user_id', true), ''));

CREATE POLICY admin_all_sessions ON session
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_sessions ON session
  FOR ALL
  USING ("userId" = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK ("userId" = nullif(current_setting('app.user_id', true), ''));

-- ── Functions ────────────────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION current_user_id() RETURNS TEXT AS $$
  SELECT nullif(current_setting('app.user_id', true), '');
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION current_user_is_admin() RETURNS BOOLEAN AS $$
  SELECT current_setting('app.user_role', true) = 'admin';
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION soft_delete_user(target_user_id TEXT) RETURNS "user" AS $$
  UPDATE "user"
  SET "deletedAt" = NOW(), banned = true, "updatedAt" = NOW()
  WHERE id = target_user_id AND "deletedAt" IS NULL
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER SET search_path = trex;

CREATE OR REPLACE FUNCTION restore_user(target_user_id TEXT) RETURNS "user" AS $$
  UPDATE "user"
  SET "deletedAt" = NULL, banned = false, "banReason" = NULL, "updatedAt" = NOW()
  WHERE id = target_user_id AND "deletedAt" IS NOT NULL
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER SET search_path = trex;

CREATE OR REPLACE FUNCTION search_users(query TEXT)
RETURNS SETOF "user" AS $$
  SELECT * FROM "user"
  WHERE "deletedAt" IS NULL
    AND (name ILIKE '%' || query || '%' OR email ILIKE '%' || query || '%')
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE SECURITY DEFINER SET search_path = trex;

CREATE OR REPLACE FUNCTION purge_deleted_users() RETURNS INTEGER AS $$
  WITH deleted AS (
    DELETE FROM "user"
    WHERE "deletedAt" IS NOT NULL
      AND "deletedAt" < NOW() - INTERVAL '30 days'
    RETURNING id
  )
  SELECT COUNT(*)::INTEGER FROM deleted;
$$ LANGUAGE SQL VOLATILE SECURITY DEFINER SET search_path = trex;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW."updatedAt" = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- ── Smart comments ───────────────────────────────────────────────────────────

COMMENT ON COLUMN "user"."mustChangePassword" IS E'@omit update';
COMMENT ON COLUMN account.password IS E'@omit';
COMMENT ON COLUMN account."accessToken" IS E'@omit';
COMMENT ON COLUMN account."refreshToken" IS E'@omit';
COMMENT ON COLUMN account."idToken" IS E'@omit';
COMMENT ON TABLE verification IS E'@omit';
COMMENT ON TABLE jwks IS E'@omit';
COMMENT ON TABLE oauth_access_token IS E'@omit';
COMMENT ON TABLE oauth_authorization_code IS E'@omit';
COMMENT ON TABLE oauth_consent IS E'@omit';
COMMENT ON COLUMN oauth_application."clientSecret" IS E'@omit';

-- ── Database management ──────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS database (
  id TEXT PRIMARY KEY CHECK (id ~ '^[A-Za-z0-9_]+$'),
  host TEXT NOT NULL,
  port INTEGER NOT NULL DEFAULT 5432,
  "databaseName" TEXT NOT NULL,
  dialect TEXT NOT NULL DEFAULT 'postgresql',
  description TEXT,
  enabled BOOLEAN DEFAULT true,
  "vocabSchemas" JSONB,
  extra JSONB,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS database_credential (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  "databaseId" TEXT NOT NULL REFERENCES database(id) ON DELETE CASCADE,
  username TEXT NOT NULL,
  password TEXT NOT NULL,
  "userScope" TEXT,
  "serviceScope" TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE ("databaseId", username)
);

CREATE INDEX IF NOT EXISTS idx_database_credential_database_id ON database_credential("databaseId");

ALTER TABLE database ENABLE ROW LEVEL SECURITY;
ALTER TABLE database_credential ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_databases ON database
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY admin_all_database_credentials ON database_credential
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

COMMENT ON COLUMN database_credential.password IS E'@omit';

CREATE OR REPLACE FUNCTION search_databases(query TEXT)
RETURNS SETOF database AS $$
  SELECT * FROM database
  WHERE id ILIKE '%' || query || '%'
    OR host ILIKE '%' || query || '%'
    OR "databaseName" ILIKE '%' || query || '%'
    OR description ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION save_database_credential(
  p_database_id TEXT,
  p_username TEXT,
  p_password TEXT,
  p_user_scope TEXT DEFAULT NULL,
  p_service_scope TEXT DEFAULT NULL
) RETURNS database_credential AS $$
  INSERT INTO database_credential ("databaseId", username, password, "userScope", "serviceScope")
  VALUES (p_database_id, p_username, p_password, p_user_scope, p_service_scope)
  ON CONFLICT ("databaseId", username) DO UPDATE SET
    password = EXCLUDED.password,
    "userScope" = EXCLUDED."userScope",
    "serviceScope" = EXCLUDED."serviceScope",
    "updatedAt" = NOW()
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER SET search_path = trex;

DROP TRIGGER IF EXISTS trg_database_updated_at ON database;
CREATE TRIGGER trg_database_updated_at
  BEFORE UPDATE ON database
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS trg_database_credential_updated_at ON database_credential;
CREATE TRIGGER trg_database_credential_updated_at
  BEFORE UPDATE ON database_credential
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── Roles ────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS role (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL UNIQUE,
  description TEXT,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_role (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "roleId" TEXT NOT NULL REFERENCES role(id) ON DELETE CASCADE,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE ("userId", "roleId")
);

CREATE INDEX IF NOT EXISTS idx_user_role_user_id ON user_role("userId");
CREATE INDEX IF NOT EXISTS idx_user_role_role_id ON user_role("roleId");

ALTER TABLE role ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_role ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_roles ON role
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY admin_all_user_roles ON user_role
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_user_roles ON user_role
  FOR SELECT
  USING ("userId" = nullif(current_setting('app.user_id', true), ''));

CREATE OR REPLACE FUNCTION search_roles(query TEXT)
RETURNS SETOF role AS $$
  SELECT * FROM role
  WHERE name ILIKE '%' || query || '%'
    OR description ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

DROP TRIGGER IF EXISTS trg_role_updated_at ON role;
CREATE TRIGGER trg_role_updated_at
  BEFORE UPDATE ON role
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── SSO providers ────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS sso_provider (
  id TEXT PRIMARY KEY CHECK (id ~ '^[a-z][a-z0-9_]*$'),
  "displayName" TEXT NOT NULL,
  "clientId" TEXT NOT NULL,
  "clientSecret" TEXT NOT NULL,
  enabled BOOLEAN DEFAULT false,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE sso_provider ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_sso_providers ON sso_provider
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

COMMENT ON COLUMN sso_provider."clientSecret" IS E'@omit';

CREATE OR REPLACE FUNCTION save_sso_provider(
  p_id TEXT,
  p_display_name TEXT,
  p_client_id TEXT,
  p_client_secret TEXT,
  p_enabled BOOLEAN DEFAULT false
) RETURNS sso_provider AS $$
  INSERT INTO trex.sso_provider (id, "displayName", "clientId", "clientSecret", enabled)
  VALUES (p_id, p_display_name, p_client_id, p_client_secret, p_enabled)
  ON CONFLICT (id) DO UPDATE SET
    "displayName" = EXCLUDED."displayName",
    "clientId" = EXCLUDED."clientId",
    "clientSecret" = CASE
      WHEN EXCLUDED."clientSecret" = '' THEN trex.sso_provider."clientSecret"
      ELSE EXCLUDED."clientSecret"
    END,
    enabled = EXCLUDED.enabled,
    "updatedAt" = NOW()
  RETURNING *;
$$ LANGUAGE SQL VOLATILE SECURITY DEFINER SET search_path = trex;

CREATE OR REPLACE FUNCTION enabled_sso_providers()
RETURNS TABLE(id TEXT, "displayName" TEXT) AS $$
  SELECT id, "displayName" FROM trex.sso_provider WHERE enabled = true ORDER BY id;
$$ LANGUAGE SQL STABLE SECURITY DEFINER SET search_path = trex;

CREATE OR REPLACE FUNCTION search_sso_providers(query TEXT)
RETURNS SETOF sso_provider AS $$
  SELECT * FROM trex.sso_provider
  WHERE id ILIKE '%' || query || '%'
    OR "displayName" ILIKE '%' || query || '%'
    OR "clientId" ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

DROP TRIGGER IF EXISTS trg_sso_provider_updated_at ON sso_provider;
CREATE TRIGGER trg_sso_provider_updated_at
  BEFORE UPDATE ON sso_provider
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ── Event log ────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS event_log (
  id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_type VARCHAR(50) NOT NULL DEFAULT 'Log',
  level VARCHAR(20) NOT NULL DEFAULT 'Info',
  message TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_event_log_created_at ON event_log (created_at DESC);
CREATE INDEX idx_event_log_level ON event_log (level);

-- ── API keys ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS api_key (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL,
  key_hash TEXT NOT NULL UNIQUE,
  key_prefix TEXT NOT NULL,
  "userId" TEXT NOT NULL REFERENCES "user"(id) ON DELETE CASCADE,
  "lastUsedAt" TIMESTAMPTZ,
  "expiresAt" TIMESTAMPTZ,
  "revokedAt" TIMESTAMPTZ,
  "createdAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_key_user_id ON api_key("userId");
CREATE INDEX IF NOT EXISTS idx_api_key_key_hash ON api_key(key_hash);

ALTER TABLE api_key ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_api_keys ON api_key
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

COMMENT ON TABLE api_key IS E'@omit';

-- ── Subscriptions (LISTEN/NOTIFY) ────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS subscription (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL UNIQUE CHECK (name ~ '^[a-z][a-z0-9_]*$'),
  topic TEXT NOT NULL,
  "sourceTable" TEXT NOT NULL,
  events TEXT[] NOT NULL DEFAULT '{INSERT,UPDATE,DELETE}',
  description TEXT,
  enabled BOOLEAN DEFAULT true,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE subscription ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_subscriptions ON subscription
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

COMMENT ON TABLE subscription IS E'@name notifySubscription\n@omit create,update,delete';

DROP TRIGGER IF EXISTS trg_subscription_updated_at ON subscription;
CREATE TRIGGER trg_subscription_updated_at
  BEFORE UPDATE ON subscription
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE FUNCTION search_subscriptions(query TEXT)
RETURNS SETOF subscription AS $$
  SELECT * FROM trex.subscription
  WHERE name ILIKE '%' || query || '%'
    OR topic ILIKE '%' || query || '%'
    OR "sourceTable" ILIKE '%' || query || '%'
    OR description ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

CREATE OR REPLACE FUNCTION save_subscription(
  p_name TEXT,
  p_topic TEXT,
  p_source_table TEXT,
  p_events TEXT[],
  p_description TEXT DEFAULT NULL,
  p_enabled BOOLEAN DEFAULT true
) RETURNS subscription AS $$
DECLARE
  v_event TEXT;
  v_trigger_name TEXT;
  v_event_clause TEXT;
  v_table_oid regclass;
  v_result trex.subscription;
BEGIN
  FOREACH v_event IN ARRAY p_events LOOP
    IF v_event NOT IN ('INSERT', 'UPDATE', 'DELETE') THEN
      RAISE EXCEPTION 'Invalid event: %. Must be INSERT, UPDATE, or DELETE.', v_event;
    END IF;
  END LOOP;

  -- Validates table exists; returns safe quoted identifier
  v_table_oid := p_source_table::regclass;

  v_trigger_name := 'trg_sub_' || p_name;

  BEGIN
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger_name, v_table_oid::TEXT);
  EXCEPTION WHEN undefined_table THEN
    NULL;
  END;

  -- Drop trigger from previous source table if it changed
  BEGIN
    PERFORM 1 FROM trex.subscription WHERE name = p_name AND "sourceTable" != p_source_table;
    IF FOUND THEN
      DECLARE
        v_old_table regclass;
      BEGIN
        SELECT "sourceTable"::regclass INTO v_old_table FROM trex.subscription WHERE name = p_name;
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger_name, v_old_table::TEXT);
      EXCEPTION WHEN undefined_table OR invalid_text_representation THEN
        NULL;
      END;
    END IF;
  END;

  EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE', 'trex', 'trex_sub_notify_' || p_name);

  IF p_enabled THEN
    EXECUTE format(
      $fn$
      CREATE OR REPLACE FUNCTION %I.%I() RETURNS TRIGGER AS $trg$
      DECLARE
        v_payload JSONB;
        v_id TEXT;
      BEGIN
        IF TG_OP = 'DELETE' THEN
          v_id := OLD.id::TEXT;
        ELSE
          v_id := NEW.id::TEXT;
        END IF;

        v_payload := jsonb_build_object(
          'event', TG_OP,
          'table', TG_TABLE_NAME,
          'schema', TG_TABLE_SCHEMA,
          'id', v_id
        );

        PERFORM pg_notify('postgraphile:' || %L, v_payload::TEXT);

        IF TG_OP = 'DELETE' THEN
          RETURN OLD;
        ELSE
          RETURN NEW;
        END IF;
      END;
      $trg$ LANGUAGE plpgsql
      $fn$,
      'trex', 'trex_sub_notify_' || p_name, p_topic
    );

    v_event_clause := array_to_string(p_events, ' OR ');

    EXECUTE format(
      'CREATE TRIGGER %I AFTER %s ON %s FOR EACH ROW EXECUTE FUNCTION %I.%I()',
      v_trigger_name, v_event_clause, v_table_oid::TEXT, 'trex', 'trex_sub_notify_' || p_name
    );
  END IF;

  INSERT INTO trex.subscription (name, topic, "sourceTable", events, description, enabled)
  VALUES (p_name, p_topic, p_source_table, p_events, p_description, p_enabled)
  ON CONFLICT (name) DO UPDATE SET
    topic = EXCLUDED.topic,
    "sourceTable" = EXCLUDED."sourceTable",
    events = EXCLUDED.events,
    description = EXCLUDED.description,
    enabled = EXCLUDED.enabled,
    "updatedAt" = NOW()
  RETURNING * INTO v_result;

  RETURN v_result;
END;
$$ LANGUAGE plpgsql VOLATILE SECURITY DEFINER SET search_path = trex;

CREATE OR REPLACE FUNCTION remove_subscription(p_name TEXT) RETURNS subscription AS $$
DECLARE
  v_sub trex.subscription;
  v_trigger_name TEXT;
  v_table_oid regclass;
BEGIN
  SELECT * INTO v_sub FROM trex.subscription WHERE name = p_name;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'Subscription "%" not found.', p_name;
  END IF;

  v_trigger_name := 'trg_sub_' || p_name;

  BEGIN
    v_table_oid := v_sub."sourceTable"::regclass;
    EXECUTE format('DROP TRIGGER IF EXISTS %I ON %s', v_trigger_name, v_table_oid::TEXT);
  EXCEPTION WHEN undefined_table OR invalid_text_representation THEN
    NULL;
  END;

  EXECUTE format('DROP FUNCTION IF EXISTS %I.%I() CASCADE', 'trex', 'trex_sub_notify_' || p_name);

  DELETE FROM trex.subscription WHERE name = p_name;

  RETURN v_sub;
END;
$$ LANGUAGE plpgsql VOLATILE STRICT SECURITY DEFINER SET search_path = trex;

-- ── Transform deployments ────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS transform_deployment (
  plugin_name TEXT NOT NULL,
  dest_db TEXT NOT NULL,
  dest_schema TEXT NOT NULL,
  deployed_at TIMESTAMPTZ DEFAULT NOW(),
  PRIMARY KEY (plugin_name)
);

-- ── Dashboards ───────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dashboard (
  id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
  name TEXT NOT NULL,
  language TEXT NOT NULL CHECK (language IN ('python', 'r', 'markdown')),
  code TEXT NOT NULL DEFAULT '',
  "userId" TEXT NOT NULL DEFAULT nullif(current_setting('app.user_id', true), '') REFERENCES "user"(id) ON DELETE CASCADE,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dashboard_user_id ON dashboard("userId");

ALTER TABLE dashboard ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_dashboards ON dashboard
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

CREATE POLICY user_own_dashboards ON dashboard
  FOR ALL
  USING ("userId" = nullif(current_setting('app.user_id', true), ''))
  WITH CHECK ("userId" = nullif(current_setting('app.user_id', true), ''));

COMMENT ON COLUMN dashboard."userId" IS E'@omit create,update';

DROP TRIGGER IF EXISTS trg_dashboard_updated_at ON dashboard;
CREATE TRIGGER trg_dashboard_updated_at
  BEFORE UPDATE ON dashboard
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE FUNCTION search_dashboards(query TEXT)
RETURNS SETOF dashboard AS $$
  SELECT * FROM dashboard
  WHERE name ILIKE '%' || query || '%'
    OR language ILIKE '%' || query || '%'
  ORDER BY "createdAt" DESC;
$$ LANGUAGE SQL STABLE;

-- ── Settings ─────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS setting (
  key TEXT PRIMARY KEY CHECK (key ~ '^[a-z][a-zA-Z0-9_.]*$'),
  value JSONB NOT NULL,
  "createdAt" TIMESTAMPTZ DEFAULT NOW(),
  "updatedAt" TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE setting ENABLE ROW LEVEL SECURITY;

CREATE POLICY admin_all_settings ON setting
  FOR ALL
  USING (current_setting('app.user_role', true) = 'admin')
  WITH CHECK (current_setting('app.user_role', true) = 'admin');

DROP TRIGGER IF EXISTS trg_setting_updated_at ON setting;
CREATE TRIGGER trg_setting_updated_at
  BEFORE UPDATE ON setting
  FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE OR REPLACE FUNCTION save_setting(
  p_key TEXT,
  p_value JSONB
) RETURNS setting AS $$
  INSERT INTO trex.setting (key, value)
  VALUES (p_key, p_value)
  ON CONFLICT (key) DO UPDATE SET
    value = EXCLUDED.value,
    "updatedAt" = NOW()
  RETURNING *;
$$ LANGUAGE SQL VOLATILE STRICT SECURITY DEFINER SET search_path = trex;

CREATE OR REPLACE FUNCTION get_setting(
  p_key TEXT,
  p_default JSONB DEFAULT 'null'::JSONB
) RETURNS JSONB AS $$
  SELECT COALESCE(
    (SELECT value FROM trex.setting WHERE key = p_key),
    p_default
  );
$$ LANGUAGE SQL STABLE SECURITY DEFINER SET search_path = trex;

INSERT INTO setting (key, value) VALUES
  ('auth.selfRegistration', 'false'::JSONB),
  ('runtime.functionLogging', '"console"'::JSONB)
ON CONFLICT (key) DO NOTHING;

-- ── Seed admin user ──────────────────────────────────────────────────────────
-- email: admin@trex.local / password: password (scrypt N=16384 r=16 p=1 dkLen=64)

INSERT INTO "user" (id, name, email, "emailVerified", role, "mustChangePassword")
VALUES ('00000000-0000-0000-0000-000000000001', 'Admin', 'admin@trex.local', true, 'admin', true)
ON CONFLICT (id) DO NOTHING;

INSERT INTO account (id, "userId", "accountId", "providerId", password)
VALUES (
  '00000000-0000-0000-0000-000000000002',
  '00000000-0000-0000-0000-000000000001',
  '00000000-0000-0000-0000-000000000001',
  'credential',
  'a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6:0c219b9e9260518faacd40f6bbaf04d86622631a745d5a177a0f8ff18363b52ed0f56a940c02a8bf0bb314b293d8ccfd5383e537bd18f445bb31f5bc979a020f'
)
ON CONFLICT (id) DO NOTHING;
