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

def get_placment_offers(term, usn):

    collection=db.pms_placement_student_details
    offers = collection.aggregate([
        {"$unwind":"$studentList"},
        {"$match":{"studentList.regNo":usn,"academicYear":term}},
        {"$project":{"companyName":1,"salary":1,"_id":0}}
    ])
    res = []
    for x in offers:
        res.append(x)
    return res

def get_user_usn(email):
    collection = db.dhi_user
    usn = collection.aggregate([{"$match":{"email":email}},
            {"$project":{"_id":0,"usn":1}}])
    res = []
    for x in usn:
        res.append(x)
    return res