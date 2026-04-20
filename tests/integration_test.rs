use mini_db::{BitCask, Database};
use mini_db::types::Value;

#[tokio::test]
async fn test_sql_crud() {
    let dir = tempfile::tempdir().unwrap();
    let engine = BitCask::init_db_at(dir.path()).unwrap();
    let db = Database::new(engine);

    // CREATE TABLE
    let result = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name STRING)").await.unwrap();
    assert!(result.rows.is_empty());

    // INSERT
    let result = db.execute("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')").await.unwrap();
    assert!(result.rows.is_empty());

    // SELECT
    let result = db.execute("SELECT * FROM users").await.unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.labels.len(), 2);

    // WHERE filter
    let result = db.execute("SELECT * FROM users WHERE id = 1").await.unwrap();
    assert_eq!(result.rows.len(), 1);

    // UPDATE
    let result = db.execute("UPDATE users SET name = 'alex' WHERE id = 1").await.unwrap();
    assert!(result.rows.is_empty());

    let result = db.execute("SELECT * FROM users WHERE id = 1").await.unwrap();
    assert_eq!(result.rows[0][1], Value::String("alex".into()));

    // DELETE
    let result = db.execute("DELETE FROM users WHERE id = 1").await.unwrap();
    assert!(result.rows.is_empty());

    let result = db.execute("SELECT * FROM users").await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Integer(2));
}

#[tokio::test]
async fn test_sql_order_by_and_group_by() {
    let dir = tempfile::tempdir().unwrap();
    let engine = BitCask::init_db_at(dir.path()).unwrap();
    let db = Database::new(engine);

    db.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, category STRING, amount INTEGER)").await.unwrap();
    db.execute("INSERT INTO items VALUES (3, 'b', 30), (1, 'a', 10), (2, 'a', 20)").await.unwrap();

    // ORDER BY
    let result = db.execute("SELECT * FROM items ORDER BY id").await.unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Integer(1));
    assert_eq!(result.rows[1][0], Value::Integer(2));
    assert_eq!(result.rows[2][0], Value::Integer(3));

    // GROUP BY
    let result = db.execute("SELECT category, COUNT(*) FROM items GROUP BY category").await.unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[tokio::test]
async fn test_sql_persistence() {
    let dir = tempfile::tempdir().unwrap();
    {
        let engine = BitCask::init_db_at(dir.path()).unwrap();
        let db = Database::new(engine);
        db.execute("CREATE TABLE persist (id INTEGER PRIMARY KEY, name STRING)").await.unwrap();
        db.execute("INSERT INTO persist VALUES (1, 'alice')").await.unwrap();
    }

    // Reopen and query
    let engine = BitCask::init_db_at(dir.path()).unwrap();
    let db = Database::new(engine);
    let result = db.execute("SELECT * FROM persist").await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], Value::String("alice".into()));
}
