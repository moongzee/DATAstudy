import vtk
from vtk.util.numpy_support import vtk_to_numpy
import glob
import random


# 이것은 point label 정보 가져오는 함수
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


filelist = glob.glob(r'\\192.168.0.113\Imagoworks\Data\confident\Mesh\IntraoralScan\SNU_full\F5\*.stl')

actors=[]
for file in filelist:
    print(file)
    
    #stl 파일 열때
    reader = vtk.vtkSTLReader()
    
    #vtp 파일 열때
    # reader = vtk.vtkXMLPolyDataReader()
    reader.SetFileName(file)
    reader.Update()
    polydata=reader.GetOutput()

    #point data 볼때
    MeshMapper = vtk.vtkPolyDataMapper()

    # boundary data 볼때
    # MeshMapper = vtk.vtkOpenGLSphereMapper()
    MeshMapper.SetInputData(polydata)
    MeshActor = vtk.vtkActor()
    MeshActor.SetMapper(MeshMapper)
    red = random.random()
    green = random.random()
    blue = random.random()
    MeshActor.GetProperty().SetColor(red,green,blue)
    actors.append(MeshActor)

renderer = vtk.vtkRenderer()
renWin = vtk.vtkRenderWindow()
renWin.AddRenderer(renderer)
renWin.SetSize(500,500)

iren = vtk.vtkRenderWindowInteractor()
iren.SetRenderWindow(renWin)

for actor in actors :
    renderer.AddActor(actor)

iren.Initialize()
renWin.Render()
iren.Start()

print(polydata)
 