from py_nosql.document.store import DocumentStore
from py_nosql.document.schema import Schema


store = DocumentStore("./db")

user_schema = Schema({
    "name": {"type": str, "unique": True, "length": {"gte": 3}},
    "age": {"type": int, "$gte": 0, "$lte": 100},
    "role": {"type": str, "enum": ["admin", "member", "guest"]}
})

file_schema = Schema({
    "filename": {"type": str, "length": {"gte": 1}},
    "size": {"type": int, "$gte": 0},
    "user_id": {"type": str, "ref": "users"}
})

users = store.collection("users", schema=user_schema)
files = store.collection("files", schema=file_schema)

bob_id = users.insert({"name": "Bob", "age": 30, "role": "member"})

# Pass collections dict for reference validation
files.insert(
    {"filename": "resume.pdf", "size": 12345, "user_id": bob_id},
    collections={"users": users}
)