import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")

db = myclient["dhi_analytics"]

def get_academic_years():
    collections = db.dhi_internal
    res = collections.aggregate([{ "$group": { "_id": "null","academicYear":{"$addToSet":"$academicYear"} } },
                                {"$project":{"res":"$academicYear","_id":0}}])
    for y in res:
        y = y ['res']
    return y

def get_semesters():
    collections = db.dhi_student_attendance
    sem = collections.aggregate([
        {"$unwind":"$departments"},
        {"$group":{"_id":"null","sems":{"$addToSet":"$departments.termName"}}},
        {"$project":{"sems":1,"_id":0}}
    ])
    res = []
    for x in sem:
        res = x["sems"]
    res.sort()
    return res