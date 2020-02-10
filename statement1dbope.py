from pymongo import MongoClient
import pymongo
from pprint import pprint
import re
import bson

db = MongoClient()
mydb = db.dhi_analytics
dhi_internal = mydb['dhi_internal']
dhi_term_details = mydb['dhi_term_detail']
dhi_student_attendance = mydb['dhi_student_attendance']
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
db = myclient["dhi_analytics"]
def getacademicYear():
    academicYear = dhi_internal.aggregate([{"$group":{"_id":"null",
    "academicYear":{"$addToSet":"$academicYear"}}},{"$project":{"academicYear":"$academicYear","_id":0}}])
    for year in academicYear:
        year = year['academicYear']
    return year

def get_term_numbers():
    terms_numbers = dhi_term_details.aggregate([ 
        { "$unwind":"$academicCalendar"}, 
        {"$group":{"_id":"null","termNumber":{"$addToSet":"$academicCalendar.termNumber"}}},
        {"$project":{"_id":0}}
    ])
    for term in terms_numbers:
        terms = term['termNumber']
    terms.sort()
    return terms


def get_user_dept(email):
    collection = db.dhi_user
    usn = collection.aggregate([
        {"$match":{"email":email}},
        {"$project":{"_id":0,"usn":1}}
        ])
    res = []
    for x in usn:
        res = x["usn"]
    res = res[5:7]
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

def get_uescore(year,dept,usn,term):

    collection = db.pms_university_exam

    ueScore = collection.aggregate([
    {"$match":{"academicYear" : year,"deptId" : dept}},
    {"$unwind":"$terms"},
    {"$match":{"terms.termNumber":term}},
    {"$unwind":"$terms.scores"},
    {"$match":{"terms.scores.usn":usn}},
    {"$project":{"course":"$terms.scores.courseScores"}},
    {"$unwind":"$course"},
    {"$project":{"courseName":"$course.courseName","ueScore":"$course.ueScore","maxUeScore":"$course.maxUeScore","totalScore":"$course.totalScore","_id":0}}
    ])
    res = []
    for x in ueScore:
        res.append(x)
    return res
    
#returns the list of all faculties in a department
def get_faculties_by_dept(dept):
    collection = db.dhi_user
    pattern = re.compile(f'^{dept}')
    regex = bson.regex.Regex.from_native(pattern)
    regex.flags ^= re.UNICODE 
    faculties = collection.aggregate([
        {"$match":{"roles.roleName":"FACULTY","employeeGivenId":{"$regex":regex}}},
        {"$sort":{"name":1}},
        {"$project":{"employeeGivenId":1,"name":1,"_id":0}}
    ])
    res = []    
    for x in faculties:
        res.append(x)
    return res

def get_faculty_wise_ue_details():
    collection = db.dhi_internal
    course = collection.aggregate([
    {"$match":{"academicYear" : "2017-18"}},
    {"$unwind":"$departments"},
    {"$match":{"departments.termNumber":"3"}},
    {"$unwind":"$faculties"},
    {"$match":{"faculties.facultyGivenId":"CIV598"}},
    {"$group":{"_id":{"courseName":"$courseName","courseCode":"$courseCode"}}},
    {"$project":{"_id":0,"courseName":"$_id.courseName","courseCode":"$_id.courseCode"}}
    ])

    res = []    
    for x in course:
        res.append(x)
    # return res
#     for k,v in res[0].items():
#         print(k,v)

# get_faculty_wise_ue_details()


#placement details of a class handled by empID
def get_emp_sub_placement(empID,sub,sem):
    collection = db.dhi_student_attendance
    students = collection.aggregate([
        {"$match":{"faculties.employeeGivenId" : empID,"departments.termNumber":sem,"courseName":sub}},
        {"$unwind":"$students"},
        {"$group":{"_id":"$courseName","studentUSNs":{"$addToSet":"$students.usn"}}},
    ])
    res = []
    for x in students:
        res.append(x)
    print(students)
    totalStudents = 0
    filtered = []
    for x in res:
        for usn in x["studentUSNs"]:
            status = get_placed_details(usn)
            if status!=0:
                filtered.append(status)
            totalStudents = len(x["studentUSNs"])
    # print("filtered",filtered)
    # print(f"Placed Students :{len(filtered)},No.of Offers : {sum(filtered)}")
    return (totalStudents,len(filtered),sum(filtered))

#returns no of placement offers obtained by a student of passed usn
def get_placed_details(usn):
    collection = db.pms_placement_student_details
    people = collection.aggregate([
    {"$match":{"studentList.regNo":usn}},
    {"$unwind":"$studentList"},
    {"$match":{"studentList.regNo":usn}},
    ])
    res = []
    for x in people:
        res.append(x)
    return len(res)

# get faculty wise student UE score
def get_faculty_id(empID):
    collection =db.dhi_internal
    emp = collection.aggregate([
    {"$unwind":{"path":"$faculties"}},
    {"$match":{"faculties.facultyGivenId":empID}},
    {"$group":{"_id":"null","id":{"$addToSet":"$faculties.facultyId"}}},
    {"$unwind":{"path":"$id"}},
    {"$project":{"id":1,"_id":0}}
    ])
    res = []
    for x in emp:
        res.append(x)
    for i in res:
        id = i
    fac_id = id['id']
    return fac_id


def get_faculty_stu_ue(year,term,id):
    collection =db.dhi_student_attendance
    emp = collection.aggregate([
    {"$match":{"academicYear":year,"students.termNumber":term}},
    {"$unwind":{'path':"$faculties"}},
    {"$match":{"faculties.facultyId":id}},
    {
    "$lookup":
    {
    "from":"pms_university_exam",
    "localField":"students.usn",
    "foreignField":"terms.scores.usn",
    "as":"usn"
    }
    },
    {"$unwind":{'path':"$usn"}},
    {"$unwind":{'path':"$usn.terms"}},
    {"$unwind":{'path':"$usn.terms.scores"}},
    {"$unwind":{'path':"$usn.terms.scores.courseScores"}},
    {"$match":{"$expr":{"$eq":["$usn.terms.scores.courseScores.courseCode","$courseCode"]}}},
    {"$group":{"_id":{"course":"$courseName"},"ue":{"$push":"$usn.terms.scores.courseScores.ueScore"}}},
    {"$project":{"course":"$_id.course","avg":{"$avg":"$ue"},"ue":"$ue","_id":0}}
    ])
    res = []
    for x in emp:
        res.append(x)
    return res
