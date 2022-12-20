import pandas as pd
import numpy as np
from sklearn.cluster import KMeans

np.random.seed(1)
labels = ['A', 'B', 'C']
N = 20
df = pd.DataFrame(index = range(N))
standardized_cols = []

for col in labels:
    df[col] = np.random.randn(N)
    standardized_colname  =  col + "_standardized"
    standardized_cols.append(standardized_colname)
    df[standardized_colname] = (df[col]-df[col].mean())/df[col].std()

c = KMeans(n_clusters=3, random_state=1).fit(df[standardized_cols]).labels_


print(c)