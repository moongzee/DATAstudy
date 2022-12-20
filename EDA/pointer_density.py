import pandas as pd  
import numpy as np
from scipy import stats
from sklearn.preprocessing import MinMaxScaler
from plotly.subplots import make_subplots
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer

pd.set_option('mode.chained_assignment',  None)


#ddh pointer 별 density값 구하기 함수
def pointer(data):
    i=0
    while i <16:
        #if i==8:
           #continue
        pointer=data.real==i
        data_=data[pointer]

        data_x=data_.x
        data_y=data_.y
        data_z=data_.z

        data_xyz=np.vstack([data_x,data_y,data_z])
        data_density=stats.gaussian_kde(data_xyz)(data_xyz)
        data_density_norm=np.interp(data_density,(data_density.min(),data_density.max()),(0,10))
        data_['density']=data_density_norm
        i+=1
    return data_

#메인함수
if __name__ == "__main__":
    #data불러오기
    data=pd.read_csv('C:/Users/imagoworks-moongzeee/Desktop/과제/2021.01.21 과제/EDA_DATA.csv')

    data['real']=data['real'].astype(object)
    data['guess']=data['guess'].astype(object)
    data['Accuracy']=data['Accuracy'].astype(object)

    #ddh density값 추가한 데이터 프레임 만들기
    cat_ddh=data.category=='ddh'
    ddh=data[cat_ddh]
    ddh=pointer(ddh)
    print(ddh)

    #daeyou density
    cat_daeyou=data.category=='daeyou'
    daeyou=data[cat_daeyou]
    daeyou=pointer(daeyou)
    print(daeyou)


