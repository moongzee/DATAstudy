from flask import Flask
from flask_cors import CORS
import os
from pymongo import MongoClient
from bson.json_util import dumps
import pandas as pd 
import json

client = MongoClient('192.168.0.131',27018)

toy_db = client['toy_db']
collection = toy_db['mesh_3d_data']

app =Flask(__name__, static_url_path='', static_folder='')
CORS(app)
curPath=os.path.dirname(os.path.abspath(__file__))


@app.route('/data')
def homepage():
    cursor=collection.find({},{'_id':0,'id':1,'Path':1,'p_num':1,'center':1,'data_set_type':1,'Arch_type':1,'GP_registration':1,'Segmentation':1,'Marginline':1})
    list_cur=list(cursor)
    json_data =dumps(list_cur)
    return json_data


@app.route('/count')
def mg_count():
    cursor1=collection.aggregate([

    {
        '$project': {
            '_id': 'Marginline',
            'TRUE': {
                '$cond': [ {'$eq': ['$Marginline', 'TRUE']}, 1, 0]
            },
            'FALSE': {
                '$cond': [ {'$eq': ['$Marginline', 'FALSE']}, 1, 0]
            },
            'None': {
                '$cond': [ {'$eq': ['$Marginline', 'None']}, 1, 0]
            }
        }
    },
    {
        '$group': {
            '_id': 'Marginline',
            'True': {
                '$sum': '$TRUE'
            },
            'False': {
                '$sum': '$FALSE'
            },
            'None': {
                '$sum': '$None'
            }
        }
    }
    ])
    list_cur=list(cursor1)
    json_mg =  list_cur[0]
    


    cursor2=collection.aggregate([

    {
        '$project': {
            '_id': 'Segmentation',
            'TRUE': {
                '$cond': [ {'$eq': ['$Segmentation', 'TRUE']}, 1, 0]
            },
            'FALSE': {
                '$cond': [ {'$eq': ['$Segmentation', 'FALSE']}, 1, 0]
            },
            'None': {
                '$cond': [ {'$eq': ['$Segmentation', 'None']}, 1, 0]
            }
        }
    },
    {
        '$group': {
            '_id': 'Senmentation',
            'True': {
                '$sum': '$TRUE'
            },
            'False': {
                '$sum': '$FALSE'
            },
            'None': {
                '$sum': '$None'
            }
        }
    }
    ])
    list_cur=list(cursor2)
    json_sg = list_cur[0]


    cursor3=collection.aggregate([

    {
        '$project': {
            '_id': 'GP_registration',
            'TRUE': {
                '$cond': [ {'$eq': ['$GP_registration', 'TRUE']}, 1, 0]
            },
            'FALSE': {
                '$cond': [ {'$eq': ['$GP_registration', 'FALSE']}, 1, 0]
            },
            'None': {
                '$cond': [ {'$eq': ['$GP_registration', 'None']}, 1, 0]
            }
        }
    },
    {
        '$group': {
            '_id': 'GP_registration',
            'True': {
                '$sum': '$TRUE'
            },
            'False': {
                '$sum': '$FALSE'
            },
            'None': {
                '$sum': '$None'
            }
        }
    }
    ])
    list_cur=list(cursor3)
    json_gp = list_cur[0]


    cursor4=collection.aggregate([

    {
        '$project': {
            '_id': 'Center_data',
            'DDH': {
                '$cond': [ {'$eq': ['$center', 'DDH']}, 1, 0]
            },
            'DAEYOU': {
                '$cond': [ {'$eq': ['$center', 'DAEYOU']}, 1, 0]
            },
            'SNU': {
                '$cond': [ {'$eq': ['$center', 'SNU']}, 1, 0]
            },
            'DDH_cut': {
                '$cond': [ {'$eq': ['$center', 'DDH_cut']}, 1, 0]
            },
            'DAEYOU_cut': {
                '$cond': [ {'$eq': ['$center', 'DAEYOU_cut']}, 1, 0]
            }
        }
    },
    {
        '$group': {
            '_id': 'Center_data',
            'DDH': {
                '$sum': '$DDH'
            },
            'DAEYOU': {
                '$sum': '$DAEYOU'
            },
            'SNU': {
                '$sum': '$SNU'
            },
            'DDH_cut':{
                '$sum': '$DDH_cut'
            },
            'DAEYOU_cut':{
                '$sum':'$DAEYOU_cut'
            }
        }
    }
    ])
    list_cur=list(cursor4)
    json_ct= list_cur[0]

    print(json_ct)    

    # json_data=json.dumps([json_ct,json_gp,json_sg,json_mg])

    result = {}    

    for data in [json_ct,json_gp,json_sg,json_mg]:

        key = data["_id"]
        data.pop("_id", None)
        result[key] = data

    return result



if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=4001)