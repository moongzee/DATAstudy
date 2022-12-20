import pandas as pd 
import numpy as np

import math

import logging

#도로명 주소, 지번 주소를 ','가 붙은 metric 주소로 변환하는 함수
def conv_add(addr: str) -> list:
    """ 

    Parameters
    -----------
    address : 

    Return
    -------
    result : 

    Note
    ----

    """
    
    if not isinstance(addr, str):
        return ' '
    else:

        temp = addr.split(' ', maxsplit=2)
    
        if len(temp)==3:
            result = ','.join([ temp[0], temp[1], temp[2] ])
        elif len(temp)==2:
            result = ','.join([ temp[0], temp[1] ])
        else:
            result = temp[0]

    return result

# 경도 위도를 찾는 함수
def find_loc (
    addr_lst: list, coord: str = 'lat', key = None
    ) -> list:

    import googlemaps

    """ 함수에 대한 요약설명 써줘

    Parameters
    -----------
    addr_lst : list of str
    coord : {'lat', 'lon'}, default 'lat'
    key : 구글 api key 

    Return
    -------
    longitude : 경도

    Note
    ----
    """

    if not coord in ['lat', 'lon']:
        raise ValueError("only recognize lat or lon for coord")

    gmaps = googlemaps.Client(key=key)
    coord_arr = np.array([])
    
    for addr in addr_lst:
        
        result = 0 # 값 초기화
        
        try:
            result = gmaps.geocode(addr) # geocoder 호출
            result = result[0]['geometry']['location'][coord]

            coord_arr = np.append(coord_arr, result)
        except (KeyError, TypeError):
            coord_arr = np.append(coord_arr, 0)

    return coord_arr


if __name__ == '__main__':
    
    df = pd.read_csv(r'C:\Users\moonj\Desktop\geocoding_pandas_pipeline\test.csv', encoding='CP949')

    # delete null value in subset all
    filtered_df= df.dropna(axis=0, how='all', subset=['발생주소(도로)','발생주소(지번)'])

    logging.info('완료')
    # replace '도로' null value with '지번' value
    filtered_df['발생주소(도로)'] = np.where(filtered_df['발생주소(도로)'].isnull(), filtered_df['발생주소(도로)'], filtered_df['발생주소(지번)'])

    # check
    # print(filtered_df['발생주소(도로)'].isnull().sum())


    #convert address to metric address
    filtered_df['발생주소(도로)_metric'] = filtered_df['발생주소(도로)'].apply(conv_add)

    logging.info('완료')

    #key = 구글 api 키 발급 방법 (https://duopix.co.kr/google-map-key/)

    filtered_df['경도'] = find_loc(filtered_df['발생주소(도로)_metric'], coord='lat', key='AIzaSyDKsM05K75bkK1dBFBbhkd87DM2FbXfpbY')
    filtered_df['위도'] = find_loc(filtered_df['발생주소(도로)_metric'], coord='lon', key='AIzaSyDKsM05K75bkK1dBFBbhkd87DM2FbXfpbY')


    print(filtered_df.head())