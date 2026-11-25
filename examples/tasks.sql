CREATE TABLE tasks (id INTEGER PRIMARY KEY, title TEXT NOT NULL, done BOOLEAN);
INSERT INTO tasks VALUES (1, 'Write tests', TRUE), (2, 'Review results', FALSE);
SELECT id, title FROM tasks WHERE done = FALSE ORDER BY id LIMIT 10;
SELECT COUNT(*) FROM tasks WHERE done = TRUE;
UPDATE tasks SET done = TRUE WHERE id = 2;
SELECT id, title FROM tasks ORDER BY id LIMIT 1 OFFSET 1;
DELETE FROM tasks WHERE id = 1;
DROP TABLE tasks;
