CREATE TABLE nodes (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    gid SMALLINT UNSIGNED NOT NULL,
    node VARCHAR(128) UNIQUE,
    can_read TINYINT UNSIGNED DEFAULT 0,
    is_fresh TINYINT UNSIGNED DEFAULT 0,
    INDEX gid (gid)
) ENGINE=InnoDB;

--# INSERT INTO nodes SET gid=1, node='192.168.0.1', can_read=1, is_fresh=1;
--# INSERT INTO nodes SET gid=1, node='192.168.0.2', can_read=1, is_fresh=1;
--# INSERT INTO nodes SET gid=1, node='192.168.0.3', can_read=1, is_fresh=1;
--# INSERT INTO nodes SET gid=2, node='192.168.0.4', can_read=1, is_fresh=1;
--# INSERT INTO nodes SET gid=2, node='192.168.0.5', can_read=1, is_fresh=1;
--# INSERT INTO nodes SET gid=2, node='192.168.0.6', can_read=1, is_fresh=1;
--# INSERT INTO nodes SET gid=3, node='192.168.0.7', can_read=1, is_fresh=1;
--# INSERT INTO nodes SET gid=3, node='192.168.0.8', can_read=1, is_fresh=1;
--# INSERT INTO nodes SET gid=3, node='192.168.0.9', can_read=1, is_fresh=1;
--# SELECT * FROM nodes WHERE gid IN (SELECT gid FROM nodes WHERE can_read=1 AND is_fresh=1 GROUP BY gid HAVING COUNT(gid) = 3);

CREATE TABLE buckets (
    id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(32) UNIQUE,
    enabled TINYINT UNSIGNED DEFAULT 1, --# 一時停止
    deleted TINYINT UNSIGNED DEFAULT 0 --# 完全削除
) ENGINE=InnoDB;

--# INSERT INTO buckets SET name = 'kazeburo';
--# INSERT INTO buckets SET name = 'foo';
--# INSERT INTO buckets SET name = 'bar';
--# INSERT INTO buckets SET name = 'baz';

CREATE TABLE objects (
    fid INT UNSIGNED NOT NULL, --# murmurhash(filename)
    bucket_id INT UNSIGNED NOT NULL,
    gid SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY(fid, bucket_id),
    INDEX (bucket_id)
) ENGINE=InnoDB;

--# INSERT INTO objects SET bucket_id=1, fid=1, gid=1;
--# INSERT INTO objects SET bucket_id=1, fid=2, gid=1;
--# INSERT INTO objects SET bucket_id=1, fid=3, gid=2;
--# INSERT INTO objects SET bucket_id=1, fid=4, gid=2;
--# INSERT INTO objects SET bucket_id=2, fid=5, gid=1;
--# INSERT INTO objects SET bucket_id=2, fid=6, gid=1;
--# INSERT INTO objects SET bucket_id=3, fid=7, gid=2;
--# INSERT INTO objects SET bucket_id=3, fid=8, gid=1;
--# INSERT INTO objects SET bucket_id=1, fid=9, gid=2;
--# INSERT INTO objects SET bucket_id=1, fid=10, gid=1;

--# SELECT * FROM buckets WHERE name = ?;
--# SELECT nodes.* FROM nodes, objects WHERE objects.bucket_id = 1 AND objects.fid = 1 AND nodes.gid = objects.gid;
