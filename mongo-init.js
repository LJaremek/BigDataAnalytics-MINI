db = db.getSiblingDB("admin");

if (db.getUser("admin") === null) {
  db.createUser({
    user: "admin",
    pwd: "admin123",
    roles: [
      { role: "userAdminAnyDatabase", db: "admin" },
      { role: "dbAdminAnyDatabase", db: "admin" },
      { role: "readWriteAnyDatabase", db: "admin" }
    ]
  });
}