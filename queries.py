from pymongo import MongoClient
from pprint import pprint

client = MongoClient("mongodb://127.0.0.1:27017")
db = client["university"]
students = db["students"]

while True:
    try:
        course_id = int(input("Enter course ID: "))
        break
    except ValueError:
        print("Please enter an integer.")

query = {"courseIds": course_id}

projection = {
    "_id": 1,
    "firstName": 1,
    "lastName": 1,
    "gpa": 1
}

results = students.find(query, projection).sort("lastName", 1)
matches = list(results)

for student in matches:
    full_name = f"{student['firstName']} {student['lastName']}"
    print(f"{student['_id']} | {full_name} | GPA: {student['gpa']:.2f}")

print("Matches:", len(matches))

explanation = students.find(query, projection).sort("lastName", 1).explain()

winning_plan = explanation["queryPlanner"]["winningPlan"]
plan = winning_plan.get("queryPlan", winning_plan)

print("\nWinning plan:")
pprint(plan)

stats = explanation["executionStats"]

print("Returned:", stats["nReturned"])
print("Documents examined:", stats["totalDocsExamined"])
print("Index keys examined:", stats["totalKeysExamined"])

while True:
    try:
        min_gpa = float(input("\nEnter minimum GPA (0.0-4.0): "))

        if 0.0 <= min_gpa <= 4.0:
            break

        print("GPA must be between 0.0 and 4.0.")
    except ValueError:
        print("Please enter a number.")

gpa_query = {"gpa": {"$gt": min_gpa}}

gpa_matches = list(
    students.find(gpa_query, projection).sort("gpa", -1)
)

for student in gpa_matches:
    full_name = f"{student['firstName']} {student['lastName']}"
    print(f"{student['_id']} | {full_name} | GPA: {student['gpa']:.2f}")

print("GPA matches:", len(gpa_matches))

extra_course = None

while True:
    value = input("\n course ID (Enter to skip): ").strip()

    if value == "":
        break

    try:
        extra_course = int(value)
        break
    except ValueError:
        print("Please enter an integer or press Enter.")

if extra_course is not None:
    combined_query = {
        "courseIds": extra_course,
        "gpa": {"$gt": min_gpa}
    }

    combined_matches = list(
        students.find(combined_query, projection).sort("gpa", -1)
    )

    for student in combined_matches:
        full_name = f"{student['firstName']} {student['lastName']}"
        print(f"{student['_id']} | {full_name} | GPA: {student['gpa']:.2f}")

    print("Combined matches:", len(combined_matches))

client.close()