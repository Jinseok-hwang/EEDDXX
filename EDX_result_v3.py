#EDX_result_v3.py


import requests
import cx_Oracle

cx_Oracle.init_oracle_client(lib_dir=r"D:\VPDTask\DXServer\venv\venv_PES_DB\instantclient_21_6")
con = cx_Oracle.connect('acpes_app/acpes48app@CWRNDPTP')
a = 901   # 해당 변수 ID로 받아와야 함 

# 첫 번째 쿼리: PES_EDX_OUT_SINGLE_TBL_TEST에서 데이터 가져오기
query1 = f"""
SELECT 
    ID, TIME, COMP_HZ, FAN_RPM_ODU, EEV_PULSE,
    T_SUPERHEATING, T_SUBCOOLING, Q_COND_TOTAL, Q_EVA_TOTAL, 
    Q_EVA_SENSIBLE, Q_EVA_LATENT, T_AIR_OUT, H_AIR_OUT, 
    CURRENT_ODU, POWER_TOTAL, POWER_VLE, MASSFLOW_TOTAL, 
    MASSFLOW_INJ, MASSFLOW_IDU, ACCUM_LEVEL, 
    REFCHARGE_TOTAL, REFCHARGE_COND, REFCHARGE_EVA, REFCHARGE_PIPE
FROM PES_EDX_OUT_SINGLE_TBL_TEST
WHERE ID = {a}  -- ID를 변수 a로 사용
"""

cursor = con.cursor()
cursor.execute(query1)
result = cursor.fetchone()  # 첫 번째 결과 행 가져오기

# 두 번째 쿼리: PES_EDX_OUTPOINT_SINGLE_TBL_TEST에서 데이터 가져오기
query2 = f"""
SELECT 
    POINT, TEMP, PRESSURE_G, ENTHALPY, QUALITY
FROM PES_EDX_OUTPOINT_SINGLE_TBL
WHERE ID = {a}  -- ID를 변수 a로 사용
"""

cursor.execute(query2)
points_result = cursor.fetchall()  # 모든 결과 행 가져오기
print(points_result)
print(next((row[2] for row in points_result if row[0] == '2'), None))


# 세 번째 쿼리: PES_EDX_INPUT_SINGLE_TBL_Test6 에서 데이터 가져오기
query3 =  f"""
SELECT 
   OPER_MODE, REF 
FROM pes_edx_input_single_tbl
WHERE ID = {a}  -- ID를 변수 a로 사용
"""
print(query3)
cursor.execute(query3)
inputs = cursor.fetchall()  # 모든 결과 행 가져오기
cursor.close()


# 결과가 존재하는지 확인
if result and points_result and inputs:
    # 세 번째 쿼리 결과를 변수에 할당
    (oper_mode, ref) = inputs[0]

    # 첫 번째 쿼리 결과를 변수에 할당
    (id, time_li, comp_hz, fan_rpm_odu, eev_pulse,
     t_superheating, t_subcooling, q_cond_total, q_eva_total, 
     q_eva_sensible, q_eva_latent, t_air_out, h_air_out, 
     current_odu, power_total, power_vle, massflow_total, 
     massflow_inj, massflow_idu, accum_level, 
     refcharge_total, refcharge_cond, refcharge_eva, refcharge_pipe) = result

        # API URL
    url = "http://10.162.36.89:8001/api/result/create" 

    # JSON 데이터 생성
    data = {
        "username": "soyeong.bae",  # input 값에서 값 가져와야 함
        "userid": "286866",       # input 값에서 값 가져와야 함
        "project_id": a,      # ID를 project_id로 사용
    # "time_li": [int(time_li)],   # 리스트 형태로 변환
    # "comp_hz": comp_hz,   # 리스트 형태로 변환
    # "fan_rpm_odu": fan_rpm_odu,  # 리스트 형태로 변환
    # "eev_pulse": {"value1": eev_pulse},  # EEV_PULSE를 딕셔너리로 변환
        "t_superheating": [float(t_superheating)],
        "t_subcooling": [float(t_subcooling)],
    #    "q_odu_total": [float(q_cond_total)],
        "q_idu_total": {
            "IDU_1" : [float(q_eva_total)]
        },        
        "q_idu_sensible": {
            "IDU_1" : [float(q_eva_sensible)]
        },
        "q_idu_latent": {
            "IDU_1" : [float(q_eva_latent)]
        },
        "t_air_out": [float(t_air_out)],
        "h_air_out": [float(h_air_out)],
    # "current_odu": current_odu,
        "power_total": [float(power_total)],
        "power_vle": [float(power_vle)],
        "massflow_total": [float(massflow_total)],
    # "massflow_inj": massflow_inj,
    # "massflow_idu": massflow_idu,
    # "accum_level": accum_level,
        "refcharge_total": [float(refcharge_total)],
        "refcharge_odu": [float(refcharge_cond)],
        "refcharge_idu": {
            "IDU_1" : [float(refcharge_eva)]
        },
        "refcharge_pipe": [float(refcharge_pipe)],
    }  

    #엔탈피의 경우 PES와 차이가 있음
    if ref == "R410A":
        dH = 141.3
    elif ref == "R32":
        dH = 133.2
    elif ref == "R290":
        dH = 104

    print(oper_mode)
    # PES 특성상 Cond/Eva로 받고 있어서 순서 변경
    if oper_mode == "COOLING":
        data.update({
            "comp_in": {
                "T": [float(next((row[1] for row in points_result if row[0] == '1'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '1'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '1'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '1'), None))]
            },
            "comp_out": {
                "T": [float(next((row[1] for row in points_result if row[0] == '2'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '2'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '2'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '2'), None))]
            },
            "odu_in": {
                "T": [float(next((row[1] for row in points_result if row[0] == '3'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '3'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '3'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '3'), None))]
            },
            "odu_mid": {
                "T": [float(next((row[1] for row in points_result if row[0] == '4'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '4'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '4'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '4'), None))]
            },
            "odu_out": {
                "T": [float(next((row[1] for row in points_result if row[0] == '6'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '6'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '6'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '6'), None))]
            },
            "idu_in": {
                "IDU_1" : {
                "T": [float(next((row[1] for row in points_result if row[0] == '8'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '8'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '8'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '8'), None))]
                    }   
            },
            "idu_mid": {
                "IDU_1" : {
                "T": [float(next((row[1] for row in points_result if row[0] == '9'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '9'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '9'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '9'), None))]
                    }                 
            },
            "idu_out": {
                 "IDU_1" : {
                "T": [float(next((row[1] for row in points_result if row[0] == '10'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '10'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '10'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '10'), None))]
                    }   
            }
        })
    else:
        data.update({
            "comp_in": {
                "T": [float(next((row[1] for row in points_result if row[0] == '1'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '1'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '1'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '1'), None))]
            },
            "comp_out": {
                "T": [float(next((row[1] for row in points_result if row[0] == '2'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '2'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '2'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '2'), None))]
            },
            "odu_in": {
                "T": [float(next((row[1] for row in points_result if row[0] == '10'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '10'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '10'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '10'), None))]
            },
            "odu_mid": {
                "T": [float(next((row[1] for row in points_result if row[0] == '9'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '9'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '9'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '9'), None))]
            },
            "odu_out": {
                "T": [float(next((row[1] for row in points_result if row[0] == '8'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '8'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '8'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '8'), None))]
            },
            "idu_in": {
                "IDU_1" : {            
                "T": [float(next((row[1] for row in points_result if row[0] == '6'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '6'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '6'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '6'), None))]
                    }                    
            },
            "idu_mid": {
                "IDU_1" : {                            
                "T": [float(next((row[1] for row in points_result if row[0] == '4'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '4'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '4'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '4'), None))]
                    }                      
            },
            "idu_out": {
                "IDU_1" : {                   
                "T": [float(next((row[1] for row in points_result if row[0] == '3'), None))],
                "P": [float(next((row[2] for row in points_result if row[0] == '3'), None)) - 101.3],
                "H": [float(next((row[3] for row in points_result if row[0] == '3'), None)) + dH],
                "X": [float(next((row[4] for row in points_result if row[0] == '3'), None))]
                    }                     
            }
        })

    


        # "eev_in": {  # 새로운 필드 추가
        #     "T": [float(points_result[4][2])],  
        #     "P": [float(points_result[5][3])],  
        #     "H": [float(points_result[6][4])],    
        #     "X": [float(points_result[6][4])]    
        # },
        # "eev_out": {  # 새로운 필드 추가
        #     "T": [float(points_result[4][2])],  
        #     "P": [float(points_result[5][3])],  
        #     "H": [float(points_result[6][4])],    
        #     "X": [float(points_result[6][4])]    
        # },
        # "vihex_in": {  # 새로운 필드 추가
        #     "T": [float(points_result[4][2])],  
        #     "P": [float(points_result[5][3])],  
        #     "H": [float(points_result[6][4])],    
        #     "X": [float(points_result[6][4])]    
        # },
        # "vihex_out": {  # 새로운 필드 추가
        #     "T": [float(points_result[4][2])],  
        #     "P": [float(points_result[5][3])],  
        #     "H": [float(points_result[6][4])],    
        #     "X": [float(points_result[6][4])]    
        # }  
    print("전송할 데이터:", data)

    # # API 요청
    # response = requests.post(url, json=data)

    # # 응답 상태 코드와 내용 출력
    # print("응답 상태 코드:", response.status_code)
    # print("응답 내용:", response.text)

    # # 응답이 성공적일 경우
    # if response.status_code == 201:  # 201 Created
    #     # JSON 데이터 파싱
    #     pjt = response.json()
    #     print("데이터가 성공적으로 추가되었습니다.")
    #     print("응답 데이터:", pjt)
    # else:
    #     print("데이터 추가 실패:", response.status_code, response.text)

con.close()
