use admin

db.createUser(
  {
    user: "zelador",
    pwd: "gelador",
    roles: [
      { role: "userAdminAnyDatabase", db: "admin" },
      { role: "readWriteAnyDatabase", db: "admin" }
    ]
  }
)

// db.adminCommand( { shutdown: 1 } )