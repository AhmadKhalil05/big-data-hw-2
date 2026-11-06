from pymongo import MongoClient
import json
from datetime import datetime, timezone
client = MongoClient("mongodb://127.0.0.1:27017")

print(client.admin.command("ping"))

db = client["university"]
students = db["students"]

print("Database:", db.name)
print("Collection:", students.name)
documents = []

with open("hw2-data.csv", encoding="utf-8-sig") as file:
    next(file)

    for line in file:
        if not line.strip():
            continue

        parts = line.strip().split(",")

        course_ids = json.loads(",".join(parts[4:-1]))

        birth_date = datetime.strptime(parts[3], "%Y-%m-%d")
        birth_date = birth_date.replace(tzinfo=timezone.utc)

        student = {
            "_id": int(parts[0]),
            "firstName": parts[1],
            "lastName": parts[2],
            "dateOfBirth": birth_date,
            "courseIds": course_ids,
            "gpa": float(parts[-1])
        }

        documents.append(student)

print("Parsed students:", len(documents))
print("First student:", documents[0])
students.drop()

result = students.insert_many(documents)

print("Inserted students:", len(result.inserted_ids))
print("Students in MongoDB:", students.count_documents({}))

print("Saved student:", students.find_one({"_id": 1}))
students.create_index("courseIds")
students.create_index("gpa")

print("Indexes:")
for index in students.list_indexes():
    print(index["name"], dict(index["key"]))

with open("students.json", "w", encoding="utf-8") as file:
    for student in documents:
        json_student = student.copy()
        json_student["dateOfBirth"] = student["dateOfBirth"].isoformat()
        file.write(json.dumps(json_student) + "\n")

print("Exported students:", len(documents))


client.close()
