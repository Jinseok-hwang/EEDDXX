#EDX_PES_comp_v1.py

#해당 코드는 API용으로 ID 넘버를 가져와서 해당 코드 실행해야함

import requests
import cx_Oracle
import sys
import json


# Oracle 클라이언트 라이브러리 초기화 (한 번만 호출)
cx_Oracle.init_oracle_client(lib_dir=r"D:\VPDTask\DXServer\venv\venv_PES_DB\instantclient_21_6")

# 데이터베이스 연결 설정 (한 번만 설정)
try:
    con = cx_Oracle.connect('acpes_app/acpes48app@CWRNDPTP')
except cx_Oracle.DatabaseError as e:
    print(f"데이터베이스 연결 실패: {e}")
    sys.exit(1)


def main(item_id):
    # API URL
    url = f"http://10.162.36.89:8001/api/comp/detail/{item_id}"  # ID를 URL에 포함

    # GET 요청
    response = requests.get(url)

    # 응답이 성공적일 경우
    if response.status_code == 200:
        # JSON 데이터 파싱
        comp = response.json()
        
        ID = comp['id'] 
        PART_NUMBER = f"[EDX]{comp['part_number']}"  # [EDX] 붙이기
        #PART_NUMBER = '--000000EDXTEST1'
        #MODEL = comp['model'] 
        #KIND = comp['kind']    
        #TYPE = comp['type']
        MODEL = comp['part_number']
        KIND = 'Scroll'      
        TYPE = 'Inverter'
        REF = comp['ref'] 
        DISP = comp['displacement'] 
        MAP_COND = comp['map_cond'] 
        # MAP_DATA_HZ = comp['map_data']['HZ']
        # MAP_DATA_TE = comp['map_data']['TE']
        # MAP_DATA_TC = comp['map_data']['TC']
        # MAP_DATA_CAPA = comp['map_data']['CAPA']
        # MAP_DATA_INPUT = comp['map_data']['INPUT']
        MAP_DATA = comp['map_data'] 
        USERID = comp['userid']
        CREATE_DATE = comp['create_date'][:10]


        try:
            # pes_comp_master_tbl에 데이터 삽입
            comp_master_query = f"""
            INSERT INTO pes_comp_master_tbl (
                PARTNO, COMP_MODEL, COMP_KIND, INVERTER_DIV, REFRIGERANT, 
                CAPACITY_TEST_CONDITION, DISP, PCM_REGI_OWNER, PCM_REGI_DATE
            ) VALUES (
                '{PART_NUMBER}', '{MODEL}', '{KIND}', '{TYPE}', 
                '{REF}', '{MAP_COND}', '{DISP}', 
                '{USERID}', '{CREATE_DATE}'
            )
            """
            print("comp_master_query values:")
            print(f"PARTNO: {PART_NUMBER}")
            print(f"COMP_MODEL: {MODEL}")
            print(f"COMP_KIND: {KIND}")
            print(f"INVERTER_DIV: {TYPE}")
            print(f"REFRIGERANT: {REF}")
            print(f"CAPACITY_TEST_CONDITION: {MAP_COND}")
            print(f"DISP: {DISP}")
            print(f"PCM_REGI_OWNER: {USERID}")
            print(f"PCM_REGI_DATE: {CREATE_DATE}")
            
            cursor = con.cursor()
            cursor.execute(comp_master_query)
            con.commit()

            # map data 파싱 및 삽입 부분 수정
            if isinstance(MAP_DATA, str):
                # 문자열인 경우 JSON으로 파싱
                MAP_DATA = json.loads(MAP_DATA)
            elif isinstance(MAP_DATA, dict):
                # 딕셔너리인 경우 그대로 사용
                MAP_DATA = [
                  list(MAP_DATA['HZ'].values()) if isinstance(MAP_DATA['HZ'], dict) else MAP_DATA['HZ'],
                  MAP_DATA['TE'],  
                  MAP_DATA['TC'],
                  MAP_DATA['CAPA'],
                  MAP_DATA['INPUT']  
                ]
            else:
                # 그 외의 경우는 예외 처리
                raise ValueError("map_data 형식이 올바르지 않습니다.")
            
            MAP_DATA_HZ = [int(float(val)) for val in MAP_DATA[0]]
            MAP_DATA_TE = [str(val) for val in MAP_DATA[1]]
            MAP_DATA_TC = [str(val) for val in MAP_DATA[2]]
            MAP_DATA_CAPA = [str(val) for val in MAP_DATA[3]]
            MAP_DATA_INPUT = [str(val) for val in MAP_DATA[4]]

            # MAP_DATA의 각 리스트 길이가 같다고 가정하고 삽입
            for i in range(len(MAP_DATA_HZ)):
                #MAP_DATA_HZ = val
                # MAP_DATA_TE = MAP_DATA[1][i]
                # MAP_DATA_TC = MAP_DATA[2][i]
                #MAP_DATA_CAPA = MAP_DATA[3][i]
                #MAP_DATA_INPUT = MAP_DATA[4][i]
                # 각 값을 개별적으로 삽입
                perf_insert_query = f"""
                INSERT INTO pes_part_compressor_perf_table (
                  PPC_PARTNO, PPC_FREQUENCY, PPC_TEMP_EVA, PPC_TEMP_COND, PPC_CAPACITY, PPC_INPUT, PPC_CAPA_UNIT, PPC_INPT_UNIT, PPC_USAGE_YN
                ) VALUES (
                  '{PART_NUMBER}', '{MAP_DATA_HZ[i]}', '{MAP_DATA_TE[i]}', '{MAP_DATA_TC[i]}', '{MAP_DATA_CAPA[i]}', '{MAP_DATA_INPUT[i]}', 'BTU/H', 'W', 'Y'
                )
                """
                cursor.execute(perf_insert_query)
                con.commit() 

        except cx_Oracle.DatabaseError as e:
            print("데이터베이스 오류 발생:", e)

    else:
        print(f"Failed to retrieve data for item_id {item_id}: {response.status_code}")



if __name__ == "__main__":
    item_id = 1  # item_id 받아와야함 
    main(item_id)

