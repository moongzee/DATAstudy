import matplotlib.pyplot as plt
import numpy as np
from skimage import io 

def overlay_grid(image, spacing):
    image_gridded = np.copy(image)
    ## 코드작성 
    grid = np.zeros(image.shape[:2],bool)
    grid[spacing:-1:spacing,:]=True
    grid[:,spacing:-1:spacing]=True
    image_gridded[grid]=[0,0,255]
    return image_gridded



url_astronaut = ('https://raw.githubusercontent.com/scikit-image/scikit-image/''master/skimage/data/astronaut.png') 
astro = io.imread(url_astronaut)
astro_sq = overlay_grid(astro, 125)


# astro_sq = np.copy(astro)
# sq_mask = np.zeros(astro.shape[:2],bool)
# sq_mask[50:100,50:100] = True
# astro_sq[sq_mask]=[0,255,0]



# astro_sq = np.copy(astro)
# astro_sq[50:100,50:100]=[0,255,0]
plt.imshow(astro_sq)
plt.show()

