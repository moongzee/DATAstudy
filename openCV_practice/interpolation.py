import numpy as np
import scipy.interpolate as ip
from scipy.interpolate import splrep, splev
import matplotlib.pyplot as plt
import cv2

img=cv2.imread(r'\\192.168.0.113\Imagoworks\AITeam\_Analysis\U-shape\ver2.0\cul_img_merge_ver2.png')

x0=[80,99,116,133,156,192,230,279,315,351,373,391,409,430]
y0=[136,206,265,311,356,382,399,399,382,357,312,266,207,137]

spl=splrep(x0,y0)

x1=np.linspace(75,436,361)
y1=splev(x1,spl)

plt.imshow(img)
plt.plot(x0,y0,'o')
plt.plot(x1,y1,'r')

plt.show()
