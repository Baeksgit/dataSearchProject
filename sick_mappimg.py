import requests
import pandas as pd
import json

# 매핑 테이블
sex_mapping = {1: "남자", 2: "여자"}

age_group_mapping = {
    1: "0~4세", 2: "5~9세", 3: "10~14세", 4: "15~19세", 5: "20~24세",
    6: "25~29세", 7: "30~34세", 8: "35~39세", 9: "40~44세", 10: "45~49세",
    11: "50~54세", 12: "55~59세", 13: "60~64세", 14: "65~69세", 15: "70~74세",
    16: "75~79세", 17: "80~84세", 18: "85세+"
}

sido_mapping = {
    11: "서울특별시", 26: "부산광역시", 27: "대구광역시", 28: "인천광역시", 29: "광주광역시",
    30: "대전광역시", 31: "울산광역시", 36: "세종특별자치시", 41: "경기도", 42: "강원도",
    43: "충청북도", 44: "충청남도", 45: "전라북도", 46: "전라남도", 47: "경상북도",
    48: "경상남도", 49: "제주특별자치도"
}

main_sick_mapping = {
    "I109": "기타 및 상세불명의 원발성 고혈압",
    "J209": "상세불명의 급성 기관지염",
    "E119": "합병증을 동반하지 않은 2형 당뇨병",
    "K210": "식도염을 동반한 위-식도역류병",
    "K219": "식도염을 동반하지 않은 위-식도역류병",
    "U_": "병인이 불확실",
    "N185": "만성 신장병(5기)",
    "E785": "상세불명의 고지질혈증",
    "K297": "상세불명의 위염",
    "J304": "상세불명의 알레르기비염",
    "J029": "상세불명의 급성 인두염",
    "K291": "기타 급성 위염",
    "J00": "급성 비인두염[감기]",
    "J060": "급성 후두인두염",
    "E782": "혼합성 고지질혈증",
    "A099": "상세불명 기원의 위장염 및 결장염",
    "J0390": "상세불명의 급성 편도염",
    "A090": "감염성 기원의 기타 및 상세불명의 위장염 및 결장염",
    "J40": "급성인지 만성인지 명시되지 않은 기관지염",
    "J303": "기타 알레르기비염",
    "K30": "기능성 소화불량",
    "E7808": "기타 및 상세불명의 순수고콜레스테롤혈증",
    "F_": "정신 및 행동 장애",
    "N_": "비뇨생식계통의 질환"
}

sub_sick_mapping = main_sick_mapping.copy()


# 매핑 적용 함수
def apply_mappings(row):
    row['성별'] = sex_mapping.get(row['성별코드'], '알수없음')
    row['연령대'] = age_group_mapping.get(row['연령대코드'], '알수없음')
    row['시도'] = sido_mapping.get(row['시도코드'], '알수없음')
    row['주상병'] = main_sick_mapping.get(row['주상병코드'], row['주상병코드'])
    row['부상병'] = sub_sick_mapping.get(row['부상병코드'], row['부상병코드'])
    return row


# Elasticsearch에 데이터 전송 함수
def send_data_to_elasticsearch(data, index_name, es_url):
    headers = {'Content-Type': 'application/json'}
    response = requests.post(f'{es_url}/{index_name}/_bulk',
    headers=headers, data=data.encode('utf-8'))
    return response


# CSV 파일을 청크 단위로 읽는 제너레이터 함수
def read_csv_in_chunks(file_path, chunk_size=1000):
    with open(file_path, 'r', encoding='cp949') as f:
        header = f.readline()  # 헤더를 읽고 건너뛴다
        while True:
            lines = []
            for _ in range(chunk_size):
                line = f.readline()
                if not line:
                    break
                lines.append(line)
            if not lines:
                break
            yield lines


# 청크 데이터를 Elasticsearch의 bulk 포맷으로 변환하는 함수
def convert_chunk_to_json(chunk, index_name):
    bulk_data = ''
    for row in chunk:
        row_data = row.strip().split(',')
        row_dict = {
            "기준년도": int(row_data[0]),
            "성별코드": int(row_data[1]),
            "연령대코드": int(row_data[2]),
            "시도코드": int(row_data[3]),
            "주상병코드": row_data[4],
            "부상병코드": row_data[5],
            "심결요양급여비용총액": int(row_data[6]),
            "심결본인부담금": int(row_data[7]),
            "심결보험자부담금": int(row_data[8]),
            "총처방일수": int(row_data[9])
        }

        # 결측치 처리
        for key in row_dict:
            if row_dict[key] == '':
                if key == '총처방일수':
                    row_dict[key] = 0
                else:
                    row_dict[key] = None

        # 이상치 제거
        if row_dict['심결요양급여비용총액'] > 10000000:
            continue

        row_dict = apply_mappings(row_dict)
        bulk_data += json.dumps({"index": {"_index": index_name}}) + '\n'
        bulk_data += json.dumps(row_dict, ensure_ascii=False) + '\n'
    return bulk_data


# CSV 파일 경로와 Elasticsearch URL 설정
csv_file = 'D:/jimin/dataSet/healthData.csv'
index_name = 'medical_data_mapping'
es_url = 'http://localhost:9200'

# 청크 크기 설정
chunk_size = 5000  # 조절 가능한 청크 크기

# CSV 파일을 청크 단위로 읽고 Elasticsearch로 전송
for chunk in read_csv_in_chunks(csv_file, chunk_size):
    bulk_data = convert_chunk_to_json(chunk, index_name)
    response = send_data_to_elasticsearch(bulk_data, index_name, es_url)
    print(response.status_code, response.text)
