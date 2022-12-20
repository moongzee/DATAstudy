import cv2
import numpy as np
import glob


#label별 
def label (img,pixel):
    canvas =np.zeros(shape=img.shape, dtype=np.uint8)
    canvas.fill(0)
    canvas[np.where((img==pixel).all(axis=2))] = pixel
    return canvas



if __name__=="__main__":
   #read image data
    imgList = glob.glob(r"C:\Users\imagoworks-moongzeee\test\azure\AI2DSingleCrown\Example\TeethArea2\align_\*")
    pixel=np.array([[0,255,125],[0,255,216],[0,255,254],[0,218,255],[0,128,255],[0,37,255],[0,0,255],[0,255,0],[39,255,0],[131,255,0],[220,255,0],[255,254,0],[255,213,0],[255,121,0]])
    totalcnt = len(imgList)

    merge=np.zeros([512,512,3], dtype=np.float64)
    for i in range(len(pixel)):
        dst = np.zeros([512, 512, 3], dtype=np.float64)
        for ipath in imgList:
            img = cv2.imread(ipath)
            cont = label(img, pixel[i])
            dst += cont / totalcnt
        dst = dst/255

        # merge=cv2.add(merge,dst)

    # cv2.imshow('dst',merge)
    # cv2.waitKey(0)
    # cv2.destroyAllWindows()

        ret , threshold_image=cv2.threshold(dst, 0, 255, cv2.THRESH_BINARY)
        # cv2.imshow('threshold',threshold_image)
        # cv2.waitKey(0)
        # cv2.destroyAllWindows()
        # exit()
        img=np.zeros([512,512,3], dtype=np.float64)
        for j in range(len(threshold_image)):
            for k in range(len(threshold_image)):
                if (threshold_image[j,k]==[255,255,255]).any():
                    img[j,k,:]=pixel[i]
        merge += img

           
    cv2.imshow('merge',merge/255)
    cv2.waitKey(0)
    cv2.destroyAllWindows()