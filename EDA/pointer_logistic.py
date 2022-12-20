import pandas as pd
import numpy as np 
from scipy import stats
from sklearn.preprocessing import StandardScaler 
import seaborn as sns
from sklearn.decomposition import PCA
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import statsmodels.api as sm

data=pd.read_csv('C:/Users/imagoworks-moongzeee/Desktop/과제/2021.01.21 과제/EDA_DATA.csv')
data['Accuracy']=data['Accuracy'].astype(int)

pointer_0=data['guess']==0

pointer_0=data[pointer_0]

x=pointer_0['x']
y=pointer_0['y']
z=pointer_0['z']

xyz=np.vstack([x,y,z])
density=stats.gaussian_kde(xyz)(xyz)
pointer_0['density']=density

false=pointer_0['Accuracy']==0
true=pointer_0['Accuracy']==1
data_false=pointer_0[false]
data_true=pointer_0[true]


x=pointer_0[['x','y','z','density']]
y=pointer_0['Accuracy']

x_train, x_test, y_train, y_test = train_test_split(x,y, test_size=0.3, random_state=0)
log=LogisticRegression()
log.fit(x_train, y_train)
print('학습용 데이터 셋 정확도: %.2f' %log.score(x_train,y_train))
print('검증용 데이터 셋 정확도: %.2f' %log.score(x_test,y_test))

y_pred=log.predict(x_test)
print(classification_report(y_test, y_pred))

logit=sm.Logit(pointer_0[['Accuracy']],x)
result=logit.fit()

print(result.summary2())

print(np.exp(result.params))