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


def get_faculty_stu_ue(empid,year,term,id):
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
    for mark in emp:
        place = get_emp_sub_placement(empid,mark['course'],term)
        if mark['avg'] != 0:
            mark['uePercentage'] =  mark['avg']
        else:
            mark['uePercentage'] = 0
        if place[0] != 0:
            mark['placePercentage'] = 100 * place[1] / place[0]
        else:
            mark['placePercentage'] = 0
        res.append(mark)
    return res

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

#returns the list of all department
def get_all_depts():
    collection = db.dhi_user
    depts = collection.aggregate([
        {"$match":{"roles.roleName":"FACULTY"}},
        {"$project":{"_id":0,"employeeGivenId":1}}
    ])
    res = []
    for d in depts:
        if "employeeGivenId" in d:
            res.append(d["employeeGivenId"])
    dept = []
    for d in res:
        name = re.findall('([a-zA-Z]*).*',d)
        if name[0].upper() not in dept:
            dept.append(name[0].upper())
    dept.remove('ADM')
    dept.remove('EC')
    return dept

def get_user_name_by_email(email):
    collection = db.dhi_user

    res = collection.aggregate([

    {"$match":{"email":email}},
    {"$project":{"name":"$name", "_id":0}}
    ])  

    name = []

    for x in res:
        name = x['name']
    return name