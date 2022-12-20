import matplotlib.pylab as plt
from skimage.io import imread
from skimage.color import rgb2gray
from skimage import filters
import cv2
im=imread(r'C:\Users\imagoworks-moongzeee\test\azure\AI2DSingleCrown\Example\TeethArea2\cul_img_mn_1.png')
edges=filters.sobel(im)
print(edges)
plt.imshow(edges)
plt.show()
# cv2.imshow('contour',edges)
# cv2.waitKey(0)


# cv2.imwrite('cul_img_mn_1_contour.png', edges)
