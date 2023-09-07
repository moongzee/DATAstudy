import pandas as pd 
import argparse
import glob
import os 
import glob
import vtk
from vtk.util.numpy_support import vtk_to_numpy
import numpy as np
import pandas as pd
import platform
import json

if platform.system() == 'Windows':
    path = '//host_ip/ports'
else:
    path = '//Volumes/ports'


def label_np(data):
    print(data)
    reader = vtk.vtkXMLPolyDataReader()
    reader.SetFileName(data)
    reader.Update()
    polydata=reader.GetOutput()
    scalars = polydata.GetPointData().GetScalars()
        
    if scalars == None : scalars = polydata.GetPointData().GetArray("Labels")
    if scalars == None : scalars = polydata.GetPointData().GetArray("label")
    if scalars == None : scalars = polydata.GetPointData().GetArray("Scalars_")
    
    label_np = vtk_to_numpy(scalars)
    
    return label_np


if __name__ == "__main__":
    
    data_path = os.path.join(path,'Data\confident\Mesh\IntraoralScan\_data_csv\MESH_3D_data.csv') 
    df = pd.read_csv(data_path)
    df['FolderIndex']=df['center']+str('_')+df['p_num']
    center_index = (df['center']=='DAEYOU')|(df['center']=='DDH')
    filtered_df = df[center_index]
    data = pd.DataFrame()
    data['center']=filtered_df['center']
    data['FolderIndex']=filtered_df['center']+str('_')+filtered_df['p_num']
    data['nasFolderPath']=str('Data/confident_db/')+data['FolderIndex']
    join_data = pd.merge(df, data, on='FolderIndex')
    data = pd.DataFrame()

    data['center']=join_data['center_y']
    data['FolderIndex']=join_data['FolderIndex']
    data['nasFolderPath']=join_data['nasFolderPath']
    data['guideplaneTask']=join_data['GP_registration']
    data['segmentationTask']=join_data['Segmentation']


    mx_num = {
        1 : 21,
        2 : 22,
        3 : 23,
        4 : 24,
        5 : 25,
        6 : 26, 
        7 : 27,
        8 : 28,
        9 : 11,
        10 : 12,
        11 : 13,
        12 : 14,
        13 : 15,
        14 : 16,
        15 : 17,
        16 : 18 
    }

    mn_num = {
        1 : 41,
        2 : 42,
        3 : 43,
        4 : 44,
        5 : 45,
        6 : 46,
        7 : 47,
        8 : 48,
        9 : 31,
        10 : 32,
        11 : 33,
        12 : 34,
        13 : 35,
        14 : 36, 
        15 : 37,
        16 : 38
    }
    
    task={
        'TRUE':'Done',
        'FALSE':'Raw',
        'UNABLE':'Checking'
    }

    file_data = []

    for i in data.iterrows():
        folderpath = str(path)+str('/')+i[1]['nasFolderPath']

        files = os.listdir(folderpath)
        for file in files:
            toothData = []
            file = os.path.join(folderpath,file)
            if file.split('\\')[-1]=='mx.vtp':
                pts = label_np(file)
                pts = np.unique(pts)
                pts = np.vectorize(mx_num.get)(pts)
                pts = list(filter(None, pts))
                for pt in pts:
                    tooth={'check':pt,'toothNumber':pt,'abutmentNumber':'','prepNumber':''}
                    toothData.append(tooth)
                archUpperData = {'archType':'full','toothData':toothData}

            elif file.split('\\')[-1]=='mn.vtp':
                pts = label_np(file)
                pts = np.unique(pts)
                pts = np.vectorize(mn_num.get)(pts)
                pts = list(filter(None, pts))
                for pt in pts:
                    tooth={'check':pt,'toothNumber':pt,'abutmentNumber':'','prepNumber':''}
                    toothData.append(tooth)
                archLowerData = {'archType':'full','toothData':toothData}

        segtask = np.array2string(np.vectorize(task.get)(i[1]['segmentationTask']))[1:-1]
        guidetask = np.array2string(np.vectorize(task.get)(i[1]['guideplaneTask']))[1:-1]
        document ={'center':i[1]['center'], 
                    'memo':'',
                    'segmentationTask':segtask,
                    'guideplaneTask':guidetask,
                    'nasFolderPath':i[1]['nasFolderPath'],
                    'crownNumber':'',
                    'bridgeNumber':'',                    
                    'FolderIndex':i[1]['FolderIndex'],
                    'archUpperData':archUpperData,
                    'archLowerData':archLowerData
                    }


        file_data.append(document)

    with open("data.json", "w") as f:
        json.dump(file_data, f, indent=4, sort_keys=True)
