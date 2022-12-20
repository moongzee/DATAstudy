import vtk
from vtk.util import numpy_support

###Initialize vtk rendering
iren = vtk.vtkRenderWindowInteractor()
iren.SetInteractorStyle(vtk.vtkInteractorStyleTrackballCamera())
renWin = vtk.vtkRenderWindow()
iren.SetRenderWindow(renWin)
ren = vtk.vtkRenderer()
ren.SetBackground(1, 1, 1)  
renWin.AddRenderer(ren)
renWin.SetSize(800, 800)
renWin.SetPosition(1000, 300)

opacityTransferFunction = vtk.vtkPiecewiseFunction()
opacityTransferFunction.AddPoint(0, 0)
# opacityTransferFunction.AddPoint(1, 1)  # only maxilla
# opacityTransferFunction.AddPoint(2, 1)  # only mandible
# opacityTransferFunction.AddPoint(3, 1)  # only implant
opacityTransferFunction.AddPoint(4, 1)  # only skin
# opacityTransferFunction.AddPoint(5, 1)  # only cartilage

colorTransferFunction = vtk.vtkColorTransferFunction()
colorTransferFunction.AddRGBPoint(0.0, 0.0, 0.0, 0.0)
colorTransferFunction.AddRGBPoint(1.0, 1.0, 0.0, 0.0)
colorTransferFunction.AddRGBPoint(2.0, 1.0, 1.0, 0.0)
colorTransferFunction.AddRGBPoint(3.0, 0.9, 0.7, 0.5)
colorTransferFunction.AddRGBPoint(4.0, 1.1, 0.5, 0.3)
colorTransferFunction.AddRGBPoint(5.0, 1.0, 1.0, 1.0)



file = '//192.168.0.113/Imagoworks/Data/confident/CT/CBCT-CAU/train/0254-0.vti'


#vti file reader
reader = vtk.vtkXMLImageDataReader()
reader.SetFileName(file)
reader.Update()
Imagedata = reader.GetOutput()


### The mapper / ray cast function know how to render the data
volumeMapper = vtk.vtkSmartVolumeMapper()
volumeMapper.SetBlendModeToComposite()
# volumeMapper.SetBlendModeToMaximumIntensity()  #*** vtk공부!
volumeMapper.SetInputData(Imagedata)

### The property describes how the data will look
volumeProperty = vtk.vtkVolumeProperty()
volumeProperty.SetColor(colorTransferFunction)
volumeProperty.SetScalarOpacity(opacityTransferFunction)
volumeProperty.ShadeOn()
# volumeProperty.SetInterpolationTypeToLinear()

### The volume holds the mapper and the property and
# can be used to position/orient the volume
volume = vtk.vtkVolume()
volume.SetMapper(volumeMapper)
volume.SetProperty(volumeProperty)
ren.AddVolume(volume)
#Rendering
renWin.Render()
#*** SetWindowName after renderWindow.Render() is called***
iren.Initialize()
iren.Start()