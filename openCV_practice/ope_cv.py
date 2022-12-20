import os
import cv2
import math
import glob
import numpy as np
import pandas as pd 
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt 



#중심좌표 찾기
def centeroid(a1,a2):
    x=int((a1[0]+a2[0])/2)
    y=int((a1[1]+a2[1])/2)
    xy=[x,y]
    return  xy

if __name__ == "__main__":
        
            

    img=cv2.imread(r'C:\Users\imagoworks-moongzeee\test\azure\AI2DSingleCrown\Example\TeethArea2\screen_shots\mn\7151.png')

    pixel=np.array([[0,255,125],[0,255,216],[0,255,254],[0,218,255],[0,128,255],[0,37,255],[0,0,255],[0,255,0],[39,255,0],[131,255,0],[220,255,0],[255,254,0],[255,213,0],[255,121,0]])

    result =np.zeros(shape=img.shape, dtype=np.uint8)
    result.fill(0)
    x=[]
    y=[]
    for i in range(len(pixel)):
        canvas =np.zeros(shape=img.shape, dtype=np.uint8)
        canvas.fill(0)
        canvas[np.where((img==pixel[i]).all(axis=2))] = pixel[i]
        imgray=cv2.cvtColor(canvas, cv2.COLOR_BGR2GRAY)
        ret, imthres = cv2.threshold(imgray,0,255,cv2.THRESH_BINARY_INV)
        contours,hierachy=cv2.findContours(imgray,cv2.RETR_EXTERNAL,cv2.CHAIN_APPROX_SIMPLE)


# 여기수정
        for c in contours:
            try:
                mmt=cv2.moments(c)
                cx=int(mmt['m10']/mmt['m00'])
                cy=int(mmt['m01']/mmt['m00'])
                a=mmt['m00']
                cv2.circle(canvas,(cx,cy),5,(255,255,255),-1)
                cv2.putText(canvas,'X:{}/Y:{}'.format(cx,cy),(cx-50,cy+20),cv2.FONT_HERSHEY_PLAIN,0.7,(0,0,255))

            except ZeroDivisionError:
                continue

            

        x.append(cx)
        y.append(cy)

        result= result + canvas 

    #moment x,y list 생성
    mom=[]
    for i in range(len(x)):
        mom.append([x[i],y[i]])
    
    #moment 중심점 찾기
    center=[]
    for i in range(int(len(mom)/2)):
        center.append(centeroid(mom[i],mom[-(i+1)]))


    #중심점으로 선형회귀 직선 구하기
    df=pd.DataFrame(center)
    df.columns=['x','y']

    X=df['x']
    y=df['y']

    line_fitter=LinearRegression()
    line_fitter.fit(X.values.reshape(-1,1),y)

    #선형회귀 직선 기울기 & 절편 
    print(line_fitter.coef_[0])
    print(line_fitter.intercept_)

    pt1x=int((0-line_fitter.intercept_)/line_fitter.coef_[0])
    pt2x=int((511-line_fitter.intercept_)/line_fitter.coef_[0])

    cv2.line(result,(pt1x,0),(pt2x,511),(0,0,255))

    rad = math.atan(line_fitter.coef_[0])
    degree = np.rad2deg(rad)
    # print(degree)

    if 0 < degree and degree < 90:
        degree = -(90 - degree)
    # elif 90 < degree and degree < 180:
    #     degree = degree - 90
    elif -90 < degree and degree < 0:
        degree = 90 + degree
        pass
    
    
    height,width=img.shape[:2]
    M = cv2.getRotationMatrix2D((width/2,height/2), degree,1)
    img2=cv2.warpAffine(result,M,(width,height))

    # 화면출력 

    cv2.imshow('qwer',img2)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
    # cv2.imwrite(os.path.join('align', 'mx_', iPath.split('\\')[-1]), img2)
