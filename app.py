from flask import Flask, jsonify,request
from flask_cors import CORS, cross_origin
from flask_pymongo import PyMongo
from flask_jwt_extended import (
    JWTManager, jwt_required, create_access_token,
    get_jwt_identity, get_jwt_claims

)
import statement4db as db
app = Flask(__name__)
CORS(app)


app.config["MONGO_URI"] = "mongodb://localhost:27017/dhi_analytics"


mongo = PyMongo(app)
# Setup the Flask-JWT-Extended extension


app.config['JWT_SECRET_KEY'] = 'super-secret' 
jwt = JWTManager(app)


class UserObject:
    def __init__(self, username, roles):
        self.username = username
        self.roles = roles


@jwt.user_claims_loader
def add_claims_to_access_token(user):
    return {'roles': user.roles}

@jwt.user_identity_loader
def user_identity_lookup(user):
    return user.username

# Provide a method to create access tokens. The create_access_token()
# function is used to actually generate the token, and you can return
# it to the caller however you choose.
@app.route('/login', methods=['POST'])
def login():
    if not request.is_json:
        return jsonify({"msg": "Missing JSON in request"}), 400
    username = request.json.get('username', None)
    if not username:
        return jsonify({"msg": "Missing username parameter"}), 400
    user = mongo.db.dhi_user.find_one({'email': username})
    if not user:
        return jsonify({"msg": "Bad username or password"}), 401
    roles = [ x['roleName'] for x in user['roles']]
    user = UserObject(username=user["email"], roles=roles)
    # Identity can be any data that is json serializable
    access_token = create_access_token(identity=user,expires_delta=False)
    return jsonify(access_token=access_token), 200

@app.route('/message')
def message():
    return {"message":"Check you luck"}

# Protect a view with jwt_required, which requires a valid access token
# in the request to access.


@app.route('/user', methods=['GET'])
@jwt_required
def protected():
    # Access the identity of the current user with get_jwt_identity
    ret = {
        'user': get_jwt_identity(),  
        'roles': get_jwt_claims()['roles'] ,
        }
    return jsonify(ret), 200

@app.route("/get-academic-years")
def get_academic_years():
    res = db.get_academic_years()
    return jsonify({"res":res})

@app.route("/get-semesters")
def get_semesters_details():
    res = db.get_semesters()
    return jsonify({"res":res})

if __name__ == "__main__":
    app.run(port=8088,debug=True)