import pandas as pd 
CT_raw=pd.read_csv(r'C:\Users\imagoworks-moongzeee\test\vti_convert_KEOSAN_MDCT_4practice.csv')

#필요없는 열 제거
df=CT_raw.drop(['patient_dir','converted'],axis=1)

#Spacing 열 값들을 리스트로 변환 후 각각의 값들을 추출해서 3개의 열로 나눠서 삽입
df['a']=df.Spacing.apply(lambda row:row[1:-1].split())
df['Spacing_X']=df.a.apply(lambda x:x[0])
df['Spacing_Y']=df.a.apply(lambda x:x[1])
df['Spacing_Z']=df.a.apply(lambda x:x[-1])

#Dimension 열 값들을 리스트로 변환 후 각각의 값들을 추출해서 3개의 열로 삽입
df['b']=df.Dimension.apply(lambda row:row[1:-1].split(','))
df['Dimension_X']=df.b.apply(lambda x:x[0])
df['Dimension_Y']=df.b.apply(lambda x:x[1])
df['Dimension_Z']=df.b.apply(lambda x:x[-1])

#keywords 열에서 각 keywords의 값들을 열로 만들어 존재유무를 조사해 존재하면 1 없으면 0으로 반환
df['CT']=df['keywords'].str.count('CT')
df['cartilage']=df['keywords'].str.count('cartilage')
df['implant']=df['keywords'].str.count('implant')
df['mandible']=df['keywords'].str.count('mandible')
df['maxilla']=df['keywords'].str.count('maxilla')
df['skin']=df['keywords'].str.count('skin')
df['skull']=df['keywords'].str.count('skull')

#각 키워드들의 합을 계산하여 'keywords_count라는 열 생성
df['keywords_count']=df['CT']+df['cartilage']+df['implant']+df['mandible']+df['maxilla']+df['skin']+df['skull']

#필요없는 열 제거
CT_data=df.drop(['keywords','Dimension','Spacing','a','b'],axis=1)

#중복된 열 제거
CT_data.drop_duplicates(inplace=True)

#정리된 데이터프레임을 CSV파일로 저장하기

CT_data.to_csv(r'C:\Users\imagoworks-moongzeee\test\CT_data.csv')

