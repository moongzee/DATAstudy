import numpy as np
import pandas as pd 
import seaborn as sns
from matplotlib import pyplot as plt
from sklearn.preprocessing import MinMaxScaler 
from sklearn.preprocessing import StandardScaler 
from sklearn.preprocessing import RobustScaler 

CT=pd.read_csv(r'C:\Users\imagoworks-moongzeee\test\CT_data.csv')
df=CT.loc[:,'Spacing_X':'Spacing_Z'].values
print(df)


scaler_a=MinMaxScaler().fit(df)
a=pd.DataFrame(scaler_a.transform(df))
print(a)

scaler_b=StandardScaler().fit(df)
b=pd.DataFrame(scaler_b.transform(df))
print(b)

scaler_c=RobustScaler().fit(df)
c=pd.DataFrame(scaler_c.transform(df))
print(c)

sns.set(style='white')
plt.figure(figsize=[11,11])

plt.subplot(221)
sns.boxplot(x='Spacing_X',data=CT)
plt.title('Original CT data')
plt.xlabel('Spacing_X')

plt.subplot(222)
sns.boxplot(x=a[0],data=a)
plt.title('MinMaxScaler')
plt.xlabel('Spacing_X')

plt.subplot(223)
sns.boxplot(x=b[0],data=b)
plt.title('StandardScaler')
plt.xlabel('Spacing_X')

plt.subplot(224)
sns.boxplot(x=c[0],data=c)
plt.title('RobustScaler')
plt.xlabel('Spacing_X')

plt.show()