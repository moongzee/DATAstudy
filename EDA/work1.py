import pandas as pd
import glob
import numpy as np

#각 csv파일의 총 포인트 갯수 구하는 함수
def get_number_of_points(csvPath):
    data_df=pd.read_csv(csvPath)
    
    return data_df.count()[0]

#각각 라벨별 데이터 수 구하고, 리스트형으로 반환
def get_number_of_labels(csvPath):
    data_df=pd.read_csv(csvPath)
    data_df.columns=['X_pointer','Y_pointer','Z_pointer','label']
    df=data_df.sort_values('label')
    i=0
    list1=[]
    while i<15:
        if (df['label']==i).any():
            Label_num=df['label']==i
            list1.insert(i,int(df.loc[Label_num,'label'].value_counts()))
        else:
            list1.insert(i,0)
        i+=1    
    return list1
        



#csv파일 긁어오기
if __name__ == "__main__":
    #glob.glob -> glob모듈의 glob 함수는 사용자가 제시한 조간에 맞는 파일명을 리스트 형식으로 반환.
    csvList = glob.glob("C:/Users\moonj/pointcloud_data/*.csv") 
    
    #총 환자수
    print('총환자수:'+str(len(csvList))+'명')


    a=0 #환자별 총 포인트갯수 다 더할 변수
    #환자별 총 포인트 갯수
    for csvFile in csvList:
       nPoints = get_number_of_points(csvFile)
       a+=nPoints
    
    #환자별 평균 포인트개수 출력
    print('평균 포인트 개수:'+str(a/len(csvList))+'개')


    b=np.zeros(15) #라벨별 데이터 수 구한 리스트 합할 넘파이 어레이
    c=np.zeros(15)
    b=b.astype(np.int32)
    c=c.astype(np.int32) 
    for csvFile in csvList:
        Label_Data = np.array(get_number_of_labels(csvFile)) #CSV파일 불러와서 넘파이 어레이로 만들기
        c  = Label_Data.astype(np.int32) #넘파이 어레이 정수형으로 만들기
        b=b+c #넘파이 어레이로 라벨별 데이터 합치기
    print(b)
    