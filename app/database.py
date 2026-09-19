"""SQLite 持久化：启动时自动建目录、建表。"""

from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Optional

from .config import settings

DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS batches (
    batch_id     TEXT PRIMARY KEY,
    submitted_at TEXT NOT NULL,
    total_count  INTEGER NOT NULL,
    unique_count INTEGER NOT NULL,
    invalid_count INTEGER NOT NULL,
    tree_count   INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS messages (
    message_id   TEXT PRIMARY KEY,
    in_reply_to  TEXT,
    "references" TEXT NOT NULL DEFAULT '[]',
    sent_at      TEXT NOT NULL,
    subject      TEXT NOT NULL,
    parent_id    TEXT,
    is_invalid   INTEGER NOT NULL DEFAULT 0,
    batch_id     TEXT NOT NULL,
    submitted_seq INTEGER NOT NULL,
    archived_at  TEXT NOT NULL,
    retain_until TEXT,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);
CREATE INDEX IF NOT EXISTS idx_messages_batch ON messages(batch_id);
CREATE INDEX IF NOT EXISTS idx_messages_parent ON messages(parent_id);
CREATE INDEX IF NOT EXISTS idx_messages_sent ON messages(sent_at);
-- idx_messages_retain 在 init_db 中、确保 retain_until 列存在后再建，
-- 以便兼容早期（无该列）数据库的补列迁移。

CREATE TABLE IF NOT EXISTS batch_duplicates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    archived INTEGER NOT NULL DEFAULT 0,
    first_submitted_seq INTEGER,
    occurrences TEXT NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE TABLE IF NOT EXISTS batch_missing_refs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    missing TEXT NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE TABLE IF NOT EXISTS batch_cycles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id TEXT NOT NULL,
    idx INTEGER NOT NULL,
    members TEXT NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

CREATE TABLE IF NOT EXISTS batch_results (
    batch_id TEXT PRIMARY KEY,
    result_json TEXT NOT NULL,
    FOREIGN KEY (batch_id) REFERENCES batches(batch_id)
);

-- 保留期冻结（活跃冻结）：每次冻结以某封“有效邮件”为锚点，只持久化锚点行；
-- 保护集不落库快照，而是按锚点当前所在会话的父子关系实时计算——新批次归档
-- 或补链并入该会话的邮件自动受保护。解除冻结删除锚点行即整组失效。
CREATE TABLE IF NOT EXISTS retention_freezes (
    anchor_id    TEXT PRIMARY KEY,
    frozen_at    TEXT NOT NULL
);
"""

_write_lock = threading.Lock()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(settings.database_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Optional[Path] = None) -> None:
    """确保数据目录存在并执行建表 DDL（幂等）。

    对早期版本做幂等迁移：messages 无 retain_until 列时补列；旧版快照式
    冻结的成员表 retention_freeze_members 已废弃（活跃冻结按当前会话拓扑
    实时计算保护集），存在即删除。
    """

    path = db_path or settings.database_path
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    try:
        conn.executescript(DDL)
        columns = {r[1] for r in conn.execute("PRAGMA table_info(messages)")}
        if "retain_until" not in columns:
            conn.execute("ALTER TABLE messages ADD COLUMN retain_until TEXT")
        # 列就绪后再建索引（新库随建表、旧库随补列，均幂等）
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_messages_retain ON messages(retain_until)"
        )
        # 旧版快照成员表随启动幂等清理（保护集改由锚点 + 当前拓扑派生）
        conn.execute("DROP TABLE IF EXISTS retention_freeze_members")
        conn.commit()
    finally:
        conn.close()


def write_lock() -> threading.Lock:
    """批量写操作的进程内互斥锁。"""

    return _write_lock
