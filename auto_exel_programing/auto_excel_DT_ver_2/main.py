import os
import re as re_module
import pandas as pd
import math

# input 폴더 안 파일들의 리스트를 불러옵니다
# 이제 굳이 이름을 input1으로 변경하지 않아도 됩니다
path_dir = './input/'
file_list = os.listdir(path_dir)

first_sum_df = pd.DataFrame()  # 원본 순위 / 기관명 / 수치를 데이터프레임으로 쓰겠다는 명시
name_list, value_list = [], []  # 기관명과 값 리스트를 쓰겠다는 명시

for i in range(len(file_list)):
    # 여기서는 데이터를 처리하는 구간입니다
    data_frame = pd.read_excel('./input/' + file_list[i], sheet_name='개방결과정리')  # 엑셀 파일 불러오기
    data_frame = data_frame[['① 광역시도내 기관별개방현황', 'Unnamed: 2', 'Unnamed: 3']][2:7]  # top5 랭킹 데이터만 가져오기
    data_frame.columns = ['Ranking', 'Name', 'Quantity']  # 속성명이 이상하게 되어있으므로 다시 잡아주기

    # 여기서부터는 현재 결과를 sum 결과에 저장합니다

    for j in data_frame.values:
        # 현재 기관이 리스트 내에 없다면 새로 값을 추가
        if j[1] not in name_list:
            name_list.append(j[1])
            value_list.append(j[2])
        # 현재 기관이 리스트 내에 있다면 값에 합산
        else:
            temp = name_list.index(j[1])
            value_list[temp] += j[2]

    data_frame = data_frame.append({'Ranking': '', 'Name': '', 'Quantity': ''}, ignore_index=True)  # 가로로 한칸 띄워주기
    first_sum_df = pd.concat([first_sum_df, data_frame])  # 원본 순위 / 기관명 / 수치를 이제 추가합니다

# 이제 모든 합산값 / 모든 기관의 수로 값의 정확성 체크
for i in range(len(value_list)):
    # 금정구는 1개의 데이터가 존재하나 3개의 기관이 있다고 응답했고 1개의 기관이 없다고 응답했기 때문에 3/4 = 0이 된다
    # 따라서 이에 따른 오류는 1로 다시 표기해준다 / 올림 방법 이용
    if int(value_list[i] / len(file_list)) == 0 and value_list[i] != 0:
        value_list[i] = math.ceil(value_list[i] / len(file_list))
    # 아니라면 패스
    else:
        value_list[i] /= len(file_list)

# 이제 원본 순위 / 기관명 / 수치를 토대로 한 새로운 final 순위 / 기관명 / 수치 데이터 프레임을 명시
second_sum_df = pd.DataFrame(
    {'': '', 'Final_Ranking': '', 'Final_Name': name_list, 'Final_Quantity': value_list, ' ': ''})
# 수치들을 토대로 랭크를 lambda를 이용해서 매겨줍시다
temp_list = list(map(lambda x: int(x), list(second_sum_df['Final_Quantity'].rank(method='min', ascending=False))))

# 값에 따라서 랭킹을 계산한 데이터들을 추가해주고 끝
for i in range(len(temp_list)):
    second_sum_df.loc[i, 'Final_Ranking'] = temp_list[i]

# 이건 나도 어떻게 했는지 기억이 안나는데 여튼 세로로 한칸 띄워서 붙입니다
first_sum_df.reset_index(drop=True, inplace=True)
second_sum_df.reset_index(drop=True, inplace=True)
second_sum_df = pd.concat([first_sum_df, second_sum_df], axis=1)

# 이번 작업은 별개의 표에 작성되어 있으므로
# 다시 불러와서 다시 작업합시다

third_sum_df = pd.DataFrame()  # 원본 기관명과 값, 비고를 데이터 프레임을 쓰겠다는 명시
name_list, value_list, remark_list = [], [], []  # 기관명과 값, 비고 리스트를 쓰겠다는 명시

for i in range(len(file_list)):
    data_frame = pd.read_excel('./input/' + file_list[i], sheet_name='개방결과정리')  # 엑셀 파일 불러오기
    data_frame = data_frame[['광역기관수', 'Unnamed: 15']][12:19]

    # 여기서 부터는 현재 결과를 sum에 저장합니다
    # 참고로 우리가 작업해야하는 것은 j[0]만 작업합니다. j[1]은 비고에 그냥 추가해주면 됩니다.
    data_frame.columns = ['Complex_data', 'Remark']

    for j in data_frame.values:
        find_val = j[0].index('(')
        temp_name, temp_value, temp_remark = '', '', ''  # 딱히 뭐 할게 없으니 NULL값 선언

        # 부산광역시_빅데이터플랫폼_국세청100대업소현황_20180601 (41)와 같이 띄워져 있을 경우
        if j[0][find_val - 1] == ' ':
            temp_name = j[0][:find_val - 1]
            temp_value = int(re_module.findall('\d+',j[0][find_val+1:])[0])
            temp_remark = j[1]
        # 부산광역시_빅데이터플랫폼_국세청100대업소현황_20180601(41)와 붙여져 있을 경우
        else:
            temp_name = j[0][:find_val]
            temp_value = int(re_module.findall('\d+',j[0][find_val+1:])[0])
            temp_remark = j[1]

        # 사하구\n~~ 이런식으로 불필요한 \n문자를 삭제
        # replace 함수가 작동이 안되므로 직접 구현
        temp_name = list(temp_name)
        try:
            temp_name.pop(temp_name.index('\n'))
        except:
            pass
        temp_name = ''.join(temp_name)
        temp_name.replace('\n', '', 2)

        # 현재 기관이 리스트 내에 없다면 새로 값을 추가
        if temp_name not in name_list:
            name_list.append(temp_name)
            value_list.append(temp_value)
            remark_list.append(temp_remark)

        # 현재 기관이 리스트 내에 있다면 값에 합산
        else:
            temp = name_list.index(temp_name)
            value_list[temp] += temp_value

            try:
                remark_list[temp] += " / " + temp_remark
            except TypeError:
                if str(temp_remark) == 'nan':
                    pass
                else:
                    remark_list[temp] = temp_remark

    data_frame = data_frame.append({'Complex_data': '', 'Remark': '', }, ignore_index=True)
    third_sum_df = pd.concat([third_sum_df, data_frame])

third_sum_df.reset_index(drop=True, inplace=True)
third_sum_df[''] = ''
third_sum_df = pd.concat([second_sum_df, third_sum_df], axis=1)

# 정확한 값 산출을 위해 모든 다운로드 회수는 올림으로 처리하여 정수형태가 되도록 합니다.
for i in range(len(value_list)):
    value_list[i] = math.ceil(value_list[i] / len(file_list))

# 여기는 정렬 코드입니다. 후에 삭제하셔도 될 것 같습니다 (시작)

for i in range(len(value_list)):
    for j in range(len(value_list)):
        if value_list[i] > value_list[j]:
            name_list[i], name_list[j] = name_list[j], name_list[i]
            value_list[i], value_list[j] = value_list[j], value_list[i]
            remark_list[i], remark_list[j] = remark_list[j], remark_list[i]

# 여기는 정렬 코드입니다. 후에 삭제하셔도 될 것 같습니다 (끝)

fourth_sum_df = pd.DataFrame(
    {'Final_Ranking_2': '', 'Final_name': name_list, 'Final_value': value_list, 'Final_remark': remark_list})
temp_list = list(map(lambda x: int(x), list(fourth_sum_df['Final_value'].rank(method='min', ascending=False))))

for i in range(len(temp_list)):
    fourth_sum_df.loc[i, 'Final_Ranking_2'] = temp_list[i]

third_sum_df.reset_index(drop=True, inplace=True)
fourth_sum_df = pd.concat([third_sum_df, fourth_sum_df], axis=1)
fourth_sum_df.to_excel('result.xlsx', index=False)