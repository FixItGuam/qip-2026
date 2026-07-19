const Database = require('better-sqlite3');
const path = require('path');

const db = new Database(path.join(__dirname, 'messenger.db'));

db.pragma('journal_mode = WAL');

db.exec(`
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  nickname TEXT UNIQUE NOT NULL,
  avatar_emoji TEXT DEFAULT 'toolbar-icons/qip-logo-mascot.png',
  qip_number TEXT UNIQUE,
  about TEXT DEFAULT '',
  password_hash TEXT,
  created_at INTEGER NOT NULL,
  last_seen INTEGER
);

CREATE TABLE IF NOT EXISTS messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  from_id INTEGER NOT NULL,
  to_id INTEGER NOT NULL,
  body TEXT NOT NULL,
  created_at INTEGER NOT NULL,
  read_at INTEGER,
  FOREIGN KEY(from_id) REFERENCES users(id),
  FOREIGN KEY(to_id) REFERENCES users(id)
);

CREATE INDEX IF NOT EXISTS idx_messages_pair ON messages(from_id, to_id);
`);

// Safe migrations for databases created by earlier versions.
const userColumns = db.prepare('PRAGMA table_info(users)').all().map(col => col.name);
if (!userColumns.includes('qip_number')) {
  db.exec('ALTER TABLE users ADD COLUMN qip_number TEXT');
}
if (!userColumns.includes('about')) {
  db.exec("ALTER TABLE users ADD COLUMN about TEXT DEFAULT ''");
}
if (!userColumns.includes('password_hash')) {
  db.exec('ALTER TABLE users ADD COLUMN password_hash TEXT');
}

module.exports = db;
