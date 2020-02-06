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

