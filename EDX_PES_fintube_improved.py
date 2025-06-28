#EDX_PES_fintube_improved.py

#!/usr/bin/env python3
"""
EDX_PES_fintube_improved.py
EDX API에서 Fintube 데이터를 가져와 PES DB에 저장하는 개선된 스크립트

기능:
- EDX API에서 fintube 상세 정보 조회
- PES DB의 3개 테이블에 데이터 저장
- 에러 처리 및 log 강화
- 설정 파일 support
"""

import requests
import cx_Oracle
import sys
import logging
import json
import os
from datetime import datetime
from typing import Dict, Any, Tuple, Optional, List
import argparse
import configparser
from pathlib import Path

# ===== 설정 관리 =====
class Config:
    """설정 관리 클래스"""
    def __init__(self, config_file: str = "edx_pes_fintube.ini"):
        self.config = configparser.ConfigParser()
        self.config_file = config_file
        self.load_config()
    
    def load_config(self):
        """설정 파일 로드 또는 기본값 생성"""
        if os.path.exists(self.config_file):
            self.config.read(self.config_file)
        else:
            # 기본 설정
            self.config['API'] = {
                'base_url': 'http://10.162.36.89:8001',
                'timeout': '30'
            }
            self.config['Database'] = {
                'connection_string': 'acpes_app/acpes48app@CWRNDPTP',
                'oracle_client_path': 'D:\\VPDTask\\DXServer\\venv\\venv_PES_DB\\instantclient_21_6'
            }
            self.config['Logging'] = {
                'level': 'INFO',
                'file': 'edx_pes_fintube.log'
            }
            self.save_config()
    
    def save_config(self):
        """설정 파일 저장"""
        with open(self.config_file, 'w') as f:
            self.config.write(f)
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """설정 값 가져오기"""
        try:
            return self.config.get(section, key)
        except:
            return default

# ===== 로깅 설정 =====
def setup_logging(config: Config):
    """로깅 설정"""
    log_level = config.get('Logging', 'level', 'INFO')
    log_file = config.get('Logging', 'file', 'edx_pes_fintube.log')
    
    # 로그 디렉토리 생성
    log_dir = Path(log_file).parent
    if log_dir.name and not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)
    
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

# ===== 데이터베이스 관리 =====
class DatabaseManager:
    """데이터베이스 연결 관리 클래스"""
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.connection = None
        self.initialized = False
        self._initialize_oracle_client()
    
    def _initialize_oracle_client(self):
        """Oracle 클라이언트 초기화"""
        try:
            oracle_client_path = self.config.get('Database', 'oracle_client_path')
            
            # 여러 경로 시도
            paths = [
                oracle_client_path,
                r"C:\instantclient_21_6",
                r"D:\instantclient_21_6",
                r"C:\Oracle\instantclient_21_6"
            ]
            
            for path in paths:
                if os.path.exists(path):
                    try:
                        cx_Oracle.init_oracle_client(lib_dir=path)
                        self.logger.info(f"Oracle 클라이언트 초기화 성공: {path}")
                        self.initialized = True
                        return
                    except Exception as e:
                        if "already initialized" in str(e):
                            self.logger.info("Oracle 클라이언트가 이미 초기화되어 있습니다.")
                            self.initialized = True
                            return
                        self.logger.warning(f"경로 {path}에서 초기화 실패: {e}")
            
            self.logger.error("Oracle 클라이언트 초기화 실패")
        except Exception as e:
            self.logger.error(f"Oracle 클라이언트 초기화 중 오류: {e}")
    
    def connect(self) -> bool:
        """데이터베이스 연결"""
        try:
            connection_string = self.config.get('Database', 'connection_string')
            self.connection = cx_Oracle.connect(connection_string)
            self.logger.info("데이터베이스 연결 성공")
            return True
        except Exception as e:
            self.logger.error(f"데이터베이스 연결 실패: {e}")
            return False
    
    def disconnect(self):
        """데이터베이스 연결 해제"""
        if self.connection:
            try:
                self.connection.close()
                self.logger.info("데이터베이스 연결 해제")
            except:
                pass
    
    def execute_query(self, query: str, commit: bool = True) -> bool:
        """쿼리 실행"""
        if not self.connection:
            self.logger.error("데이터베이스 연결이 없습니다.")
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            if commit:
                self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"쿼리 실행 실패: {e}")
            self.logger.debug(f"실패한 쿼리: {query}")
            if commit:
                self.connection.rollback()
            return False

# ===== API 클라이언트 =====
class EDXAPIClient:
    """EDX API 클라이언트"""
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.base_url = config.get('API', 'base_url')
        self.timeout = int(config.get('API', 'timeout', '30'))
    
    def get_fintube_detail(self, item_id: int) -> Optional[Dict[str, Any]]:
        """Fintube 상세 정보 조회"""
        url = f"{self.base_url}/api/fintube/detail/{item_id}"
        
        try:
            self.logger.info(f"API 호출: {url}")
            response = requests.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"Fintube 데이터 조회 성공: ID={item_id}")
                return data
            else:
                self.logger.error(f"API 호출 실패: {response.status_code} - {response.text}")
                return None
                
        except requests.Timeout:
            self.logger.error(f"API 호출 타임아웃: {self.timeout}초")
            return None
        except Exception as e:
            self.logger.error(f"API 호출 중 오류: {e}")
            return None

# ===== 데이터 처리 =====
class FintubeDataProcessor:
    """Fintube 데이터 처리 클래스"""
    def __init__(self, db_manager: DatabaseManager, logger: logging.Logger):
        self.db = db_manager
        self.logger = logger
    
    def process_fintube_data(self, fintube_data: Dict[str, Any]) -> bool:
        """Fintube 데이터를 PES DB에 저장"""
        try:
            # 데이터 추출 및 변환
            processed_data = self._extract_and_transform_data(fintube_data)
            
            # 기존 데이터 삭제 (있는 경우)
            self._delete_existing_data(processed_data['PART_NUMBER'])
            
            # 1. pes_hex_master_tbl 삽입
            if not self._insert_hex_master(processed_data):
                return False
            
            # 2. pes_hex_airflow_profile_tbl 삽입
            if not self._insert_airflow_profile(processed_data):
                return False
            
            # 3. pes_hex_air_seg_profile_tbl 삽입 (데이터가 있는 경우)
            if processed_data.get('AIRFLOW_PROFILE_SEG'):
                if not self._insert_airflow_seg_profile(processed_data):
                    return False
            
            self.logger.info(f"모든 데이터 저장 완료: {processed_data['PART_NUMBER']}")
            return True
            
        except Exception as e:
            self.logger.error(f"데이터 처리 중 오류: {e}")
            return False
    
    def _extract_and_transform_data(self, fintube: Dict[str, Any]) -> Dict[str, Any]:
        """API 데이터를 DB 형식으로 변환"""
        # 기본 데이터
        data = {
            'ID': fintube['id'],
            'PART_NUMBER': f"[EDX]{fintube['part_number']}",  # [EDX] 접두어 추가
            'EVA_COND': "CONDENSER" if fintube.get('odu_idu') == "ODU" else "EVAPORATOR",
            'FIN_TYPE': fintube['fin']['fin_type'],
            'ROW': fintube['row'],
            'COL': fintube['col'],
            'FPI': fintube['fpi'],
            'TUBE_LEN': fintube['tube_len'],
            'BENDING_TYPE': "hex_type1",
            'PATH_DESIGN_1': fintube['path_design1'],
            'PATH_DESIGN_2': fintube['path_design2'],
            'USERID': fintube['userid'],
            'CREATE_DATE': fintube['create_date'][:10],
            'USAGE_YN': "Y",
            'TUBE_MATERIAL': fintube['fin']['tube_material'],
            'HEX_TYPE': "FIN_TUBE",
            'AIRFLOW_PROFILE': fintube.get('airflow_profile', []),
            'AIRFLOW_PROFILE_SEG': fintube.get('airflow_profile_seg', [])
        }
        
        # tube_dia 처리 (PES는 string 값으로 필요)
        tube_dia_value = fintube['fin']['tube_dia']
        if isinstance(tube_dia_value, float) and tube_dia_value.is_integer():
            data['TUBE_DIA'] = str(int(tube_dia_value))
        else:
            data['TUBE_DIA'] = str(tube_dia_value)
        
        return data
    
    def _delete_existing_data(self, part_number: str) -> bool:
        """기존 데이터 삭제"""
        tables = [
            'pes_hex_air_seg_profile_tbl',
            'pes_hex_airflow_profile_tbl',
            'pes_hex_master_tbl'
        ]
        
        for table in tables:
            column = 'PHA_PART_NO' if table != 'pes_hex_master_tbl' else 'PHM_PART_NO'
            query = f"DELETE FROM {table} WHERE {column} = '{part_number}'"
            
            if not self.db.execute_query(query):
                self.logger.warning(f"{table}에서 기존 데이터 삭제 실패")
        
        return True
    
    def _insert_hex_master(self, data: Dict[str, Any]) -> bool:
        """pes_hex_master_tbl 데이터 삽입"""
        query = f"""
        INSERT INTO pes_hex_master_tbl (
            PHM_PART_NO, PHM_EVA_COND_DIV, PHM_FIN_TYPE, PHM_ROW_COUNT, PHM_COLUMN_COUNT, 
            PHM_FPI, PHM_LENGTH, PHM_BENDING_TYPE, PHM_TUBE_DIAMETER, PHM_PATH_DESIGN_1, 
            PHM_PATH_DESIGN_2, PHM_REGISTER, PHM_REGIS_DATE, PHM_USAGE_YN, 
            PHM_TUBE_MATERIAL, PHM_HEX_TYPE
        ) VALUES (
            '{data['PART_NUMBER']}', '{data['EVA_COND']}', '{data['FIN_TYPE']}', 
            {data['ROW']}, {data['COL']}, {data['FPI']}, {data['TUBE_LEN']}, 
            '{data['BENDING_TYPE']}', '{data['TUBE_DIA']}', '{data['PATH_DESIGN_1']}', 
            '{data['PATH_DESIGN_2']}', '{data['USERID']}', '{data['CREATE_DATE']}', 
            '{data['USAGE_YN']}', '{data['TUBE_MATERIAL']}', '{data['HEX_TYPE']}'
        )
        """
        
        if self.db.execute_query(query):
            self.logger.info(f"pes_hex_master_tbl 삽입 성공: {data['PART_NUMBER']}")
            return True
        return False
    
    def _insert_airflow_profile(self, data: Dict[str, Any]) -> bool:
        """pes_hex_airflow_profile_tbl 데이터 삽입"""
        airflow_profile = data.get('AIRFLOW_PROFILE', [])
        
        for order, airflow in enumerate(airflow_profile):
            query = f"""
            INSERT INTO pes_hex_airflow_profile_tbl (
                PHA_PART_NO, PHA_ORDER, PHA_AIRFLOW
            ) VALUES (
                '{data['PART_NUMBER']}', {order}, {float(airflow)}
            )
            """
            
            if not self.db.execute_query(query):
                self.logger.error(f"airflow_profile 삽입 실패: order={order}")
                return False
        
        self.logger.info(f"pes_hex_airflow_profile_tbl 삽입 성공: {len(airflow_profile)}개")
        return True
    
    def _insert_airflow_seg_profile(self, data: Dict[str, Any]) -> bool:
        """pes_hex_air_seg_profile_tbl 데이터 삽입"""
        airflow_seg = data.get('AIRFLOW_PROFILE_SEG', [])
        
        for order, segment in enumerate(airflow_seg):
            # 세그먼트가 정확히 10개의 값을 가지고 있는지 확인
            if len(segment) != 10:
                self.logger.warning(f"세그먼트 {order}는 10개의 값을 가지지 않으므로 건너뜁니다.")
                continue
            
            query = f"""
            INSERT INTO pes_hex_air_seg_profile_tbl (
                PHA_PART_NO, PHA_ORDER, PHA_AIRFLOW_01, PHA_AIRFLOW_02, PHA_AIRFLOW_03, 
                PHA_AIRFLOW_04, PHA_AIRFLOW_05, PHA_AIRFLOW_06, PHA_AIRFLOW_07, 
                PHA_AIRFLOW_08, PHA_AIRFLOW_09, PHA_AIRFLOW_10
            ) VALUES (
                '{data['PART_NUMBER']}', {order}, {segment[0]}, {segment[1]}, {segment[2]}, 
                {segment[3]}, {segment[4]}, {segment[5]}, {segment[6]}, 
                {segment[7]}, {segment[8]}, {segment[9]}
            )
            """
            
            if not self.db.execute_query(query):
                self.logger.error(f"airflow_seg_profile 삽입 실패: order={order}")
                return False
        
        self.logger.info(f"pes_hex_air_seg_profile_tbl 삽입 성공: {len(airflow_seg)}개")
        return True

# ===== 메인 애플리케이션 =====
class EDXPESFintubeApp:
    """메인 애플리케이션 클래스"""
    def __init__(self, config_file: str = "edx_pes_fintube.ini"):
        self.config = Config(config_file)
        self.logger = setup_logging(self.config)
        self.db_manager = DatabaseManager(self.config, self.logger)
        self.api_client = EDXAPIClient(self.config, self.logger)
        self.data_processor = FintubeDataProcessor(self.db_manager, self.logger)
    
    def process_fintube(self, item_id: int) -> bool:
        """Fintube 처리 메인 메서드"""
        self.logger.info(f"===== Fintube 처리 시작: ID={item_id} =====")
        
        # 1. 데이터베이스 연결
        if not self.db_manager.connect():
            self.logger.error("데이터베이스 연결 실패")
            return False
        
        try:
            # 2. API에서 데이터 조회
            fintube_data = self.api_client.get_fintube_detail(item_id)
            if not fintube_data:
                self.logger.error(f"Fintube 데이터 조회 실패: ID={item_id}")
                return False
            
            # 3. 데이터 출력 (디버깅용)
            self._print_fintube_info(fintube_data)
            
            # 4. 데이터베이스에 저장
            success = self.data_processor.process_fintube_data(fintube_data)
            
            if success:
                self.logger.info(f"===== Fintube 처리 완료: ID={item_id} =====")
            else:
                self.logger.error(f"===== Fintube 처리 실패: ID={item_id} =====")
            
            return success
            
        finally:
            # 5. 데이터베이스 연결 해제
            self.db_manager.disconnect()
    
    def _print_fintube_info(self, fintube: Dict[str, Any]):
        """Fintube 정보 출력"""
        print(f"\n{'='*50}")
        print(f"ID: {fintube['id']}")
        print(f"PART_NUMBER: [EDX]{fintube['part_number']}")
        print(f"FIN_TYPE: {fintube['fin']['fin_type']}")
        print(f"ROW: {fintube['row']}")
        print(f"COL: {fintube['col']}")
        print(f"FPI: {fintube['fpi']}")
        print(f"TUBE_LEN: {fintube['tube_len']}")
        print(f"TUBE_DIA: {fintube['fin']['tube_dia']}")
        print(f"USERID: {fintube['userid']}")
        print(f"CREATE_DATE: {fintube['create_date'][:10]}")
        print(f"TUBE_MATERIAL: {fintube['fin']['tube_material']}")
        print(f"{'='*50}\n")

# ===== 커맨드라인 인터페이스 =====
def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='EDX API에서 Fintube 데이터를 가져와 PES DB에 저장합니다.'
    )
    parser.add_argument(
        'item_id', 
        type=int, 
        help='처리할 Fintube의 ID'
    )
    parser.add_argument(
        '--config', 
        type=str, 
        default='edx_pes_fintube.ini',
        help='설정 파일 경로 (기본값: edx_pes_fintube.ini)'
    )
    parser.add_argument(
        '--debug', 
        action='store_true',
        help='디버그 모드 활성화'
    )
    
    args = parser.parse_args()
    
    # 디버그 모드 설정
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 애플리케이션 실행
    app = EDXPESFintubeApp(args.config)
    success = app.process_fintube(args.item_id)
    
    # 종료 코드 반환
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
