import cv2
import numpy as np

img1=cv2.imread(r'C:\Users\imagoworks-moongzeee\Desktop\mn_x.png',1)
img2=cv2.imread(r'C:\Users\imagoworks-moongzeee\Desktop\mn_y.png',1)
img3=cv2.imread(r'C:\Users\imagoworks-moongzeee\Desktop\mx_x.png',1)
img4=cv2.imread(r'C:\Users\imagoworks-moongzeee\Desktop\mx_y.png',1)



size=(600,800)

img1=cv2.resize(img1,size)
img2=cv2.resize(img2,size)
img3=cv2.resize(img3,size)
img4=cv2.resize(img4,size)


# img1=cv2.cvtColor(img1,cv2.COLOR_BGR2RG)
# img2=cv2.cvtColor(img2,cv2.COLOR_RGB2BGR)
# img3=cv2.cvtColor(img3,cv2.COLOR_RGB2BGR)
# img4=cv2.cvtColor(img4,cv2.COLOR_RGB2BGR)

# img1=cv2.resize(img1,size)
# img2=cv2.resize(img2,size)
# img3=cv2.resize(img3,size)
# img4=cv2.resize(img4,size)

add_img=np.hstack((img1,img2))
add_img=cv2.cvtColor(add_img, cv2.COLOR_RGBA2BGRA)
# cv2.imwrite('mn.png',add_img)


cv2.imshow('test',add_img)
cv2.waitKey(0)
cv2.destroyAllWindows()