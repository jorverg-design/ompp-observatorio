CREATE TABLE IF NOT EXISTS products (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT UNIQUE,
  name TEXT NOT NULL,
  category TEXT NOT NULL,
  unit TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  product_code TEXT NOT NULL,
  week_date TEXT NOT NULL,
  value REAL NOT NULL,
  geo TEXT DEFAULT 'AMA',
  source TEXT DEFAULT 'Relevamiento',
  created_at TEXT NOT NULL,
  UNIQUE(product_code, week_date, geo, source)
);

CREATE TABLE IF NOT EXISTS computed (
  week_date TEXT PRIMARY KEY,
  index_canasta REAL,
  index_mobility REAL,
  costilla_avg REAL,
  costilla_weekly_change REAL,
  alerts_json TEXT,
  created_at TEXT NOT NULL
);
