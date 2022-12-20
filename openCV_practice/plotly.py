import pandas as pd 
import plotly.express as px

data=pd.read_csv(r'C:\Users\imagoworks-moongzeee\Desktop\과제\2021.01.13 과제\CT_data.csv')
fig=px.scatter_3d(data, x='Spacing_X', y='Spacing_Y', z='Spacing_Z')
fig.show()