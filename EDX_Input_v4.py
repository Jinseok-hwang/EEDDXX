
#EDX_Input_v4.py
#null 값 처리 추가

import requests
import cx_Oracle

# API URL
url = "http://10.162.36.89:8001/api/project/detail/6" #API 호출해서 ID 넘버 받아와 변경


# GET 요청
response = requests.get(url)

# 해당 싱글기준
if response.status_code == 200:
    # JSON 데이터 파싱
    pjt = response.json()

    
    def get_nested_value(data, keys, default=None):
        for key in keys:
            if isinstance(data, dict) and key in data:
                data = data[key]
            elif isinstance(data, list) and len(data) > 0:
                data = data[0]  # 첫 번째 요소를 선택
        return data
    

    ID = get_nested_value(pjt, ['id'], '')
    STATUS = get_nested_value(pjt, ['status'], '')
    print(STATUS)
    NUM_IDU = get_nested_value(pjt, ['num_idus'], '')
    PROGRAM = get_nested_value(pjt, ['program'], '')
    PROJECT_NAME = get_nested_value(pjt, ['project_name'], '')
    PRODUCT_LINE = get_nested_value(pjt, ['product_line'], '')
    ODU_MODEL = get_nested_value(pjt, ['odu_model', 'part_number'], '')
    IDU_MODEL = get_nested_value(pjt, ['idu_associations', 0, 'idu', 'part_number'], '')
    LIQ_DIA = get_nested_value(pjt, ['liq_dia', 'IDU_1'], '')
    GAS_DIA = get_nested_value(pjt, ['gas_dia', 'IDU_1'], '')
    PIPE_LENG = get_nested_value(pjt, ['pipe_len', 'IDU_1'], '')
    REF = get_nested_value(pjt, ['ref'], '')
    OPER_MODE = get_nested_value(pjt, ['mode'], '')
    T_CONDITION = get_nested_value(pjt, ['t_condition'], '')
    T_DRY_IDU = get_nested_value(pjt, ['t_dry_idu'], '')
    T_WET_IDU = get_nested_value(pjt, ['t_wet_idu'], '')
    T_DRY_ODU = get_nested_value(pjt, ['t_dry_odu'], '')
    T_WET_ODU = get_nested_value(pjt, ['t_wet_odu'], '')
    RPM_IDU = get_nested_value(pjt, ['rpm_idu', 'IDU_1'], '')
    CMM_IDU = get_nested_value(pjt, ['cmm_idu', 'IDU_1'], '')
    MOTOR_IDU = get_nested_value(pjt, ['motor_idu', 'IDU_1'], '')
    RPM_ODU = get_nested_value(pjt, ['rpm_odu'], '')
    CMM_ODU = get_nested_value(pjt, ['cmm_odu'], '')
    MOTOR_ODU = get_nested_value(pjt, ['motor_odu'], '')
    COMP_HZ = get_nested_value(pjt, ['comp_hz'], '')
    SH_CONDITION = get_nested_value(pjt, ['sh_condition'], '')
    QUALITY_CONDITION = get_nested_value(pjt, ['quality_condition'], '')
    REFCHARGE_CONDITION = get_nested_value(pjt, ['refcharge_condition'], '')
    SC_CONDITION = get_nested_value(pjt, ['sc_condition'], '')
    EEV_CONDITION = get_nested_value(pjt, ['eev_condition'], '')
    VERSION_COMP_SIM = get_nested_value(pjt, ['version_comp_sim'], '')
    AIRFLOW_MODE = get_nested_value(pjt, ['airflow_mode', 0], '')
    CONV1 = get_nested_value(pjt, ['conv1'], '')
    CONV2 = get_nested_value(pjt, ['conv2'], '')
    ODU_COMP = f"[EDX]{get_nested_value(pjt, ['odu_model', 'comp', 'part_number'], '')}"
    ODU_HEX_TYPE = get_nested_value(pjt, ['odu_model', 'hex_type'], '')
    ODU_HEX = f"[EDX]{get_nested_value(pjt, ['odu_model', 'fintube_association', 0, 'fintube', 'part_number'], '')}"
    ODU_EEV = get_nested_value(pjt, ['eev_association', 0, 'eev', 'part_number'], '')  # EEV 정보는 ODU에서 가져오기로 변경
    #ODU_EEV = pjt['odu_model']['eev']['part_number']
    #ODU_EEV_VI = pjt['odu_model']['eev_vi']
    #ODU_EEV_VI = get_nested_value(pjt, ['odu_model', 'eev_vi', 'part_number'], None)
    ODU_CAPI_DIA = get_nested_value(pjt, ['odu_model', 'capi', 'DIA'], '')  # null 처리
    ODU_CAPI_LEN = get_nested_value(pjt, ['odu_model', 'capi', 'LENG'], '')  # null 처리
    #ODU_FAN_MOTOR = get_nested_value(pjt, ['odu_model', 'fan_association', 0, 'fan', 'part_number'], '')
    IDU_HEX_TYPE = get_nested_value(pjt, ['idu_associations', 0, 'idu', 'hex_type'], '')
    IDU_HEX =  f"[EDX]{get_nested_value(pjt,['idu_associations', 0, 'idu', 'fintube_association', 0, 'fintube', 'part_number'], '')}"
    EEV_DIA = get_nested_value(pjt, ['eev_association', 0, 'eev', 'diameter'], '')
    EEV_MAX_PULSE = get_nested_value(pjt, ['eev_association', 0, 'eev', 'max_pulse'], '')
    EEV_CORR = get_nested_value(pjt, ['eev_association', 0, 'eev', 'corr'], '')
    COMP_DISP = get_nested_value(pjt, ['odu_model', 'comp', 'displacement'], '')
    COMP_EFF_V = get_nested_value(pjt, ['odu_model', 'comp', 'corr_effi', 'EFF_V'], '')
    COMP_EFF_ISEN = get_nested_value(pjt, ['odu_model', 'comp', 'corr_effi', 'EFF_ISEN'], '')
    COMP_EFF_MECH = get_nested_value(pjt, ['odu_model', 'comp', 'corr_effi', 'EFF_MECH'], '')

    print(IDU_HEX)

    # Connect to Oracle Database
    cx_Oracle.init_oracle_client(lib_dir=r"D:\VPDTask\DXServer\venv\venv_PES_DB\instantclient_21_6")
    con = cx_Oracle.connect('acpes_app/acpes48app@CWRNDPTP')

    # Constructing the SQL INSERT statement
    prj_input_query = f"""
    INSERT INTO PES_EDX_INPUT_SINGLE_TBL (
        ID, STATUS, PROGRAM, PROJECT_NAME, PRODUCT_LINE, ODU_MODEL, IDU_MODEL, 
        LIQ_DIA, GAS_DIA, PIPE_LENG, REF, OPER_MODE, T_CONDITION, T_DRY_IDU, 
        T_WET_IDU, T_DRY_ODU, T_WET_ODU, RPM_IDU, CMM_IDU, MOTOR_IDU, 
        RPM_ODU, CMM_ODU, MOTOR_ODU, COMP_HZ, SH_CONDITION, QUALITY_CONDITION, 
        REFCHARGE_CONDITION, SC_CONDITION, EEV_CONDITION, VERSION_COMP_SIM, 
        AIRFLOW_MODE, CONV1, CONV2, ODU_COMP, ODU_HEX_TYPE, ODU_HEX, 
        ODU_EEV, ODU_EEV_VI, ODU_CAPI_DIA, ODU_CAPI_LEN,
        IDU_HEX_TYPE, IDU_HEX, EEV_DIA, EEV_MAX_PULSE, EEV_CORR,
        COMP_DISP, COMP_EFF_V, COMP_EFF_ISEN, COMP_EFF_MECH
    ) VALUES (
        '{ID}', '{STATUS}', '{PROGRAM}', '{PROJECT_NAME}', 
        '{PRODUCT_LINE}', '{ODU_MODEL}', '{IDU_MODEL}', 
        '{LIQ_DIA}', '{GAS_DIA}', '{PIPE_LENG}', '{REF}', 
        '{OPER_MODE}', '{T_CONDITION}', '{T_DRY_IDU}', 
        '{T_WET_IDU}', '{T_DRY_ODU}', '{T_WET_ODU}', 
        '{RPM_IDU}', '{CMM_IDU}', '{MOTOR_IDU}', 
        '{RPM_ODU}', '{CMM_ODU}', '{MOTOR_ODU}', 
        '{COMP_HZ}', '{SH_CONDITION}', '{QUALITY_CONDITION}', 
        '{REFCHARGE_CONDITION}', '{SC_CONDITION}', 
        '{EEV_CONDITION}', '{VERSION_COMP_SIM}', 
        '{AIRFLOW_MODE}', '{CONV1}', '{CONV2}', 
        '{ODU_COMP}', '{ODU_HEX_TYPE}', '{ODU_HEX}', 
        '{ODU_EEV}', '-', '{ODU_CAPI_DIA}', 
        '{ODU_CAPI_LEN}', 
        '{IDU_HEX_TYPE}', '{IDU_HEX}', '{EEV_DIA}', '{EEV_MAX_PULSE}', '{EEV_CORR}',
        '{COMP_DISP}', '{COMP_EFF_V}', '{COMP_EFF_ISEN}', '{COMP_EFF_MECH}'   
    )
    """
    print(prj_input_query)
    cursor = con.cursor()
    cursor.execute(prj_input_query)
    con.commit()
    cursor.close()
    con.close()

else:
    print("Failed to retrieve data:", response.status_code)

print(ID)
