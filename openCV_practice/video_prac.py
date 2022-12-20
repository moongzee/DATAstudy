import cv2
import glob
import re
import statistics

paths=glob.glob(r"C:\Users\imagoworks-moongzeee\Desktop\video\*")
img_array = []
for filename in paths:
    img = cv2.imread(filename)
    height,width,layers = img.shape
    size=(height,width)
    img_array.append(img)

fourcc = cv2.VideoWriter_fourcc(*'DIVX')
out = cv2.VideoWriter('output.avi',fourcc,5,size)

for i in range(len(img_array)):
    out.write(img_array[i])

out.release()