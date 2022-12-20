import os
import glob

#파일 긁어와서 필요한 글자만 반환하는 함수
def filename(file):
    s=os.path.split(file)
    a=s[1]
    b=a.split('-')
    c=str(b[5])+'-'+str(b[6])
    return c




#파일명 바꾸는 메인함수
if __name__ == "__main__":

    path=r'C:\Users\imagoworks-moongzeee\Desktop\exp\구강스캔\1\checkpoint'
    files=glob.glob(path+'\*.pt')

    for i in files:
        os.rename(i,filename(i))
        