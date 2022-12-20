import pandas as pd 
import numpy as np 
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import statsmodels.api as sm

data=pd.read_csv('C:/Users/imagoworks-moongzeee/Desktop/과제/2021.01.21 과제/EDA_DATA.csv')

data['Accuracy']=data['Accuracy'].astype(int)
x=data[['x','y','z']]
y=data[['Accuracy']]
x_train, x_test, y_train, y_test =train_test_split(x,y,test_size=0.3, random_state=0)


#로지스틱 회귀분석 시행
log=LogisticRegression()

#모델의 정확도를 확인 
log.fit(x_train, y_train)
print('학습용 데이터셋 정확도 : %.2f' % log.score(x_train, y_train))
print('검증용 데이터셋 정확도 : %.2f' % log.score(x_test, y_test))

#모형의 성능평가를 위한 모듈 
y_pred=log.predict(x_test)
print(classification_report(y_test, y_pred))

#또 다른 회귀분석을 시행// 모델의 회귀계수와 오즈비를 구해 독립변수가 분류결정에 미치는 영향의 정도를 알아보기 위함. 
logit=sm.Logit(data[['Accuracy']],x) #logit이라는 변수에 Accuracy 종속변수를 넣고, 앞에 넣은 독립변수 x 값을 넣어 적합시킴  
result=logit.fit()
print(result.summary2())

#통계량에 나와있는 편회귀계수의 값 자체만으로는 각 변수들이 종속변수에 얼마나 영향을 주는지는 파악할 수 없다. 
#종속변수에 미치는 정도는 오즈비를 통해 파악할 수 있다.

print(np.exp(result.params))



