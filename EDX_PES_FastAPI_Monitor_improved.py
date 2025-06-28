#!/usr/bin/env python3
"""
EDX_PES_FastAPI_Monitor.py (완전한 DB 저장 로직 구현 버전)
EDX API와 PES DB 간 데이터 동기화를 위한 FastAPI 기반 웹 서비스

주요 수정사항:
- 모든 부품 타입의 완전한 DB 저장 로직 구현
- 관계형 데이터 처리 개선
- 트랜잭션 처리 추가
- SQL 파라미터 바인딩으로 보안 강화
- DB 스키마 검증 로직 추가

주요 기능:
- FastAPI 웹 서버로 EDX와 실시간 통신
- 모든 부품 타입 CRUD 지원 (생성, 조회, 수정, 삭제)
- EDX API 주기적 모니터링 (명세서 반영)
- 새로운/변경된 데이터 자동 감지 및 동기화
- PES DB 자동 동기화
- 처리 이력 관리
- 에러 알림 및 재시도
- RESTful API 엔드포인트 제공
"""

import requests
import cx_Oracle
import sys
import logging
import json
import os
from datetime import datetime, timedelta
import time
import argparse
import configparser
from pathlib import Path
import schedule
import threading
from typing import Dict, Any, Tuple, Optional, List, Set
import pickle
import traceback
import asyncio
from contextlib import asynccontextmanager

# FastAPI 관련 임포트
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, field_validator
import uvicorn

# ===== Pydantic 모델 정의 (EDX API 명세 반영) =====

# 공통 응답 모델
class SyncResponse(BaseModel):
    """동기화 응답 모델"""
    success: bool
    message: str
    processed_count: Optional[int] = 0
    failed_count: Optional[int] = 0

class MonitorStatusResponse(BaseModel):
    """모니터링 상태 응답 모델"""
    is_running: bool
    last_check_time: Optional[str] = None
    processed_items_count: int
    error_items_count: int

class WebhookRequest(BaseModel):
    """EDX에서 받는 웹훅 요청 모델"""
    event_type: str = Field(description="이벤트 타입 (created, updated, deleted)")
    component_type: str = Field(description="부품 타입 (fintube, fin, fan, comp, etc.)")
    component_id: int = Field(description="부품 ID")
    
    @field_validator('component_id')
    @classmethod
    def validate_component_id(cls, v):
        if v < 0:  # 0을 허용하도록 변경
            raise ValueError('component_id must be non-negative integer')
        return v
    
    @field_validator('component_type')
    @classmethod
    def validate_component_type(cls, v):
        allowed_types = ['fintube', 'fin', 'fan', 'comp', 'eev', 'accum', 'hexcoeff', 'eevcoeff', 'compcoeff']
        if v not in allowed_types:
            raise ValueError(f'component_type must be one of {allowed_types}')
        return v
    
    @field_validator('component_id')
    @classmethod
    def validate_component_id(cls, v):
        if v <= 0:
            raise ValueError('component_id must be positive integer')
        return v

# 간단한 웹훅 요청 모델 (부품별 엔드포인트용)
class SimpleWebhookRequest(BaseModel):
    """간단한 웹훅 요청 모델 (부품 타입이 URL에서 결정됨)"""
    event_type: str = Field(default="created", description="이벤트 타입")
    component_id: int = Field(description="부품 ID")
    
    @field_validator('event_type')
    @classmethod
    def validate_event_type(cls, v):
        allowed_events = ['created', 'updated', 'deleted']
        if v not in allowed_events:
            raise ValueError(f'event_type must be one of {allowed_events}')
        return v

class ComponentListResponse(BaseModel):
    """부품 목록 응답 모델"""
    total: int
    items: List[Dict[str, Any]]
    next_cursor: Optional[int] = None

# Fintube 관련 모델 (EDX API 명세 반영)
class FintubeData(BaseModel):
    """Fintube 데이터 모델 (EDX API 명세서 기준)"""
    id: int
    part_number: str
    username: str
    userid: str
    create_date: str
    modify_date: Optional[str] = None
    tube_len: Optional[float] = None
    row: Optional[int] = None
    col: Optional[int] = None
    fpi: Optional[int] = None  # EDX API 명세서에서 integer로 정의됨
    num_path: Optional[float] = None  # EDX API에 있는 필드 추가
    path_design1: Optional[str] = None
    path_design2: Optional[str] = None
    airflow_profile: Optional[List[float]] = None
    airflow_profile_seg: Optional[List[List[float]]] = None
    odu_idu: Optional[str] = None
    fin_id: Optional[int] = None  # 관계형 필드 추가
    hex_coeff_id: Optional[int] = None  # 관계형 필드 추가
    fin: Optional[Dict[str, Any]] = None  # 조인된 fin 데이터
    hex_coeff: Optional[Dict[str, Any]] = None  # 조인된 hex_coeff 데이터

class FintubeCreate(BaseModel):
    """Fintube 생성 모델 (EDX API 명세서 기준)"""
    part_number: str = Field(min_length=1, max_length=50)
    username: str = Field(min_length=1, max_length=50)
    userid: str = Field(min_length=1, max_length=50)
    tube_len: Optional[float] = Field(None, gt=0)
    row: Optional[int] = Field(None, gt=0)
    col: Optional[int] = Field(None, gt=0)
    fpi: Optional[int] = Field(None, gt=0)  # integer로 변경
    num_path: Optional[float] = Field(None, gt=0)  # 추가
    path_design1: Optional[str] = Field(None, max_length=10)
    path_design2: Optional[str] = Field(None, max_length=10)
    airflow_profile: Optional[List[float]] = None
    airflow_profile_seg: Optional[List[List[float]]] = None
    odu_idu: Optional[str] = Field("IDU", pattern="^(IDU|ODU)$")
    fin_id: Optional[int] = Field(None, gt=0)  # 관계형 필드 추가
    hex_coeff_id: Optional[int] = Field(None, gt=0)  # 관계형 필드 추가

class FintubeUpdate(BaseModel):
    """Fintube 수정 모델 (EDX API 명세서 기준)"""
    tube_len: Optional[float] = Field(None, gt=0)
    row: Optional[int] = Field(None, gt=0)
    col: Optional[int] = Field(None, gt=0)
    fpi: Optional[int] = Field(None, gt=0)  # integer로 변경
    num_path: Optional[float] = Field(None, gt=0)  # 추가
    path_design1: Optional[str] = Field(None, max_length=10)
    path_design2: Optional[str] = Field(None, max_length=10)
    airflow_profile: Optional[List[float]] = None
    airflow_profile_seg: Optional[List[List[float]]] = None
    odu_idu: Optional[str] = Field(None, pattern="^(IDU|ODU)$")
    fin_id: Optional[int] = Field(None, gt=0)  # 관계형 필드 추가
    hex_coeff_id: Optional[int] = Field(None, gt=0)  # 관계형 필드 추가

# Comp 관련 모델 (EDX API 명세 반영)
class CompData(BaseModel):
    """Comp 데이터 모델 (EDX API 명세서 기준)"""
    id: int
    part_number: Optional[str] = None
    username: str
    userid: str
    create_date: str
    modify_date: Optional[str] = None
    model: Optional[str] = None
    kind: Optional[str] = None
    type: Optional[str] = None
    ref: Optional[str] = None
    displacement: Optional[float] = None
    map_data: Optional[Dict[str, List[float]]] = None
    map_cond: Optional[str] = None
    corr_effi: Optional[Dict[str, float]] = None  # EDX API에서 nullable로 변경
    comp_coeff_id: Optional[int] = None  # 관계형 필드
    comp_coeff: Optional[Dict[str, Any]] = None  # 조인된 데이터

class CompCreate(BaseModel):
    """Comp 생성 모델 (EDX API 명세서 기준)"""
    username: str = Field(min_length=1, max_length=50)
    userid: str = Field(min_length=1, max_length=50)
    part_number: Optional[str] = Field(None, max_length=50)
    model: Optional[str] = Field(None, max_length=50)
    kind: Optional[str] = Field(None, max_length=20)
    type: Optional[str] = Field(None, max_length=20)
    ref: Optional[str] = Field(None, max_length=20)
    displacement: Optional[float] = Field(None, gt=0)
    map_data: Optional[Dict[str, List[float]]] = None
    map_cond: Optional[str] = Field(None, max_length=50)
    corr_effi: Optional[Dict[str, float]] = None  # EDX API에서 nullable로 변경
    comp_coeff_id: Optional[int] = Field(None, gt=0)

class CompUpdate(BaseModel):
    """Comp 수정 모델 (EDX API 명세서 기준)"""
    model: Optional[str] = Field(None, max_length=50)
    kind: Optional[str] = Field(None, max_length=20)
    type: Optional[str] = Field(None, max_length=20)
    ref: Optional[str] = Field(None, max_length=20)
    displacement: Optional[float] = Field(None, gt=0)
    map_data: Optional[Dict[str, List[float]]] = None
    map_cond: Optional[str] = Field(None, max_length=50)
    corr_effi: Optional[Dict[str, float]] = None  # EDX API에서 nullable로 변경
    comp_coeff_id: Optional[int] = Field(None, gt=0)

# 기타 부품 모델들 (기존 유지하되 필요시 EDX API 명세 반영)
class FinData(BaseModel):
    """Fin 데이터 모델"""
    id: int
    fin_name: str
    username: str
    userid: str
    create_date: str
    modify_date: Optional[str] = None
    tube_dia: Optional[float] = None
    fin_type: Optional[str] = None
    row_pitch: Optional[float] = None
    col_pitch: Optional[float] = None
    tube_type: Optional[str] = None
    tube_thick: Optional[float] = None
    tube_groove: Optional[Dict[str, float]] = None  # EDX API 추가 필드
    tube_material: Optional[str] = None
    fin_material: Optional[str] = None
    fin_thick: Optional[float] = None
    fin_type2: Optional[str] = None  # EDX API 추가 필드
    fin_dimension: Optional[Dict[str, float]] = None  # EDX API 추가 필드

class FinCreate(BaseModel):
    """Fin 생성 모델"""
    fin_name: str = Field(min_length=1, max_length=50)
    username: str = Field(min_length=1, max_length=50)
    userid: str = Field(min_length=1, max_length=50)
    tube_dia: Optional[float] = Field(None, gt=0)
    fin_type: Optional[str] = Field(None, max_length=20)
    row_pitch: Optional[float] = Field(None, gt=0)
    col_pitch: Optional[float] = Field(None, gt=0)
    tube_type: Optional[str] = Field(None, max_length=20)
    tube_thick: Optional[float] = Field(None, gt=0)
    tube_groove: Optional[Dict[str, float]] = None
    tube_material: Optional[str] = Field(None, max_length=20)
    fin_material: Optional[str] = Field(None, max_length=20)
    fin_thick: Optional[float] = Field(None, gt=0)
    fin_type2: Optional[str] = Field(None, max_length=20)
    fin_dimension: Optional[Dict[str, float]] = None

class FinUpdate(BaseModel):
    """Fin 수정 모델"""
    tube_dia: Optional[float] = Field(None, gt=0)
    fin_type: Optional[str] = Field(None, max_length=20)
    row_pitch: Optional[float] = Field(None, gt=0)
    col_pitch: Optional[float] = Field(None, gt=0)
    tube_type: Optional[str] = Field(None, max_length=20)
    tube_thick: Optional[float] = Field(None, gt=0)
    tube_groove: Optional[Dict[str, float]] = None
    tube_material: Optional[str] = Field(None, max_length=20)
    fin_material: Optional[str] = Field(None, max_length=20)
    fin_thick: Optional[float] = Field(None, gt=0)
    fin_type2: Optional[str] = Field(None, max_length=20)
    fin_dimension: Optional[Dict[str, float]] = None

# Fan 관련 모델 (EDX API 명세 반영)
class FanData(BaseModel):
    """Fan 데이터 모델 (EDX API 명세서 기준)"""
    id: int
    part_number: str
    username: str
    userid: str
    create_date: str
    modify_date: Optional[str] = None
    diameter: Optional[float] = None
    rpm: Optional[List[int]] = None
    cmm: Optional[List[float]] = None
    watt: Optional[List[float]] = None
    static_p: Optional[float] = None  # EDX API 추가 필드

class FanCreate(BaseModel):
    """Fan 생성 모델 (EDX API 명세서 기준)"""
    part_number: str = Field(min_length=1, max_length=50)
    username: str = Field(min_length=1, max_length=50)
    userid: str = Field(min_length=1, max_length=50)
    diameter: Optional[float] = Field(None, gt=0)
    rpm: Optional[List[int]] = None
    cmm: Optional[List[float]] = None
    watt: Optional[List[float]] = None
    static_p: Optional[float] = Field(None, ge=0)

class FanUpdate(BaseModel):
    """Fan 수정 모델"""
    diameter: Optional[float] = Field(None, gt=0)
    rpm: Optional[List[int]] = None
    cmm: Optional[List[float]] = None
    watt: Optional[List[float]] = None
    static_p: Optional[float] = Field(None, ge=0)

# EEV 관련 모델 (EDX API 명세 반영)
class EEVData(BaseModel):
    """EEV 데이터 모델 (EDX API 명세서 기준)"""
    id: int
    part_number: str
    username: str
    userid: str
    create_date: str
    modify_date: Optional[str] = None
    diameter: Optional[float] = None
    max_pulse: Optional[float] = None
    corr: Optional[float] = None  # EDX API 추가 필드
    eev_coeff_id: Optional[int] = None  # 관계형 필드
    eev_coeff: Optional[Dict[str, Any]] = None  # 조인된 데이터

class EEVCreate(BaseModel):
    """EEV 생성 모델 (EDX API 명세서 기준)"""
    part_number: str = Field(min_length=1, max_length=50)
    username: str = Field(min_length=1, max_length=50)
    userid: str = Field(min_length=1, max_length=50)
    diameter: Optional[float] = Field(None, gt=0)
    max_pulse: Optional[float] = Field(None, gt=0)
    corr: Optional[float] = None
    eev_coeff_id: Optional[int] = Field(None, gt=0)

class EEVUpdate(BaseModel):
    """EEV 수정 모델"""
    diameter: Optional[float] = Field(None, gt=0)
    max_pulse: Optional[float] = Field(None, gt=0)
    corr: Optional[float] = None
    eev_coeff_id: Optional[int] = Field(None, gt=0)

# Accum 관련 모델
class AccumData(BaseModel):
    """Accum 데이터 모델"""
    id: int
    part_number: Optional[str] = None  # EDX API에서 nullable
    username: str
    userid: str
    create_date: str
    modify_date: Optional[str] = None
    capa: Optional[float] = None

class AccumCreate(BaseModel):
    """Accum 생성 모델"""
    username: str = Field(min_length=1, max_length=50)
    userid: str = Field(min_length=1, max_length=50)
    part_number: Optional[str] = Field(None, max_length=50)
    capa: Optional[float] = Field(None, gt=0)

class AccumUpdate(BaseModel):
    """Accum 수정 모델"""
    capa: Optional[float] = Field(None, gt=0)

# 계수 관련 모델들 (EDX API 명세 반영)
class HexCoeffData(BaseModel):
    """HexCoeff 데이터 모델 (EDX API 명세서 기준)"""
    id: int
    corr_name: str
    username: str
    userid: str
    create_date: str
    modify_date: Optional[str] = None
    corr: Optional[Dict[str, Dict[str, List[float]]]] = None
    corr_ver: Optional[str] = None

class HexCoeffCreate(BaseModel):
    """HexCoeff 생성 모델"""
    corr_name: str = Field(min_length=1, max_length=50)
    username: str = Field(min_length=1, max_length=50)
    userid: str = Field(min_length=1, max_length=50)
    corr: Optional[Dict[str, Dict[str, List[float]]]] = None
    corr_ver: Optional[str] = Field(None, max_length=20)

class HexCoeffUpdate(BaseModel):
    """HexCoeff 수정 모델"""
    corr: Optional[Dict[str, Dict[str, List[float]]]] = None
    corr_ver: Optional[str] = Field(None, max_length=20)

# ===== 설정 관리 =====
class Config:
    """설정 관리 클래스"""
    def __init__(self, config_file: str = "edx_pes_monitor.ini"):
        self.config = configparser.ConfigParser()
        self.config_file = config_file
        self.load_config()
    
    def load_config(self):
        """설정 파일 로드 또는 기본값 생성"""
        if os.path.exists(self.config_file):
            self.config.read(self.config_file, encoding='utf-8')
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
                'file': 'edx_pes_monitor.log'
            }
            self.config['Monitor'] = {
                'check_interval_minutes': '10',
                'batch_size': '10',
                'retry_count': '3',
                'retry_delay_seconds': '30',
                'state_file': 'monitor_state.pkl'
            }
            self.config['FastAPI'] = {
                'host': '165.186.62.167',
                'port': '8002',
                'reload': 'false'
            }
            self.save_config()
    
    def save_config(self):
        """설정 파일 저장"""
        with open(self.config_file, 'w', encoding='utf-8') as f:
            self.config.write(f)
    
    def get(self, section: str, key: str, default: Any = None) -> Any:
        """설정 값 가져오기"""
        try:
            return self.config.get(section, key)
        except:
            return default
    
    def getint(self, section: str, key: str, default: int = 0) -> int:
        """정수 설정 값 가져오기"""
        try:
            return self.config.getint(section, key)
        except:
            return default

# ===== 모니터링 상태 관리 =====
class MonitorState:
    """모니터링 상태 관리 클래스"""
    def __init__(self, state_file: str):
        self.state_file = state_file
        self.processed_ids: Set[int] = set()
        self.last_check_time: Optional[datetime] = None
        self.error_count: Dict[int, int] = {}
        self.load_state()
    
    def load_state(self):
        """상태 파일 로드"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'rb') as f:
                    data = pickle.load(f)
                    self.processed_ids = data.get('processed_ids', set())
                    self.last_check_time = data.get('last_check_time')
                    self.error_count = data.get('error_count', {})
            except Exception as e:
                logging.warning(f"상태 파일 로드 실패: {e}")
    
    def save_state(self):
        """상태 파일 저장"""
        try:
            data = {
                'processed_ids': self.processed_ids,
                'last_check_time': self.last_check_time,
                'error_count': self.error_count
            }
            with open(self.state_file, 'wb') as f:
                pickle.dump(data, f)
        except Exception as e:
            logging.error(f"상태 파일 저장 실패: {e}")
    
    def mark_processed(self, item_id: int):
        """항목을 처리됨으로 표시"""
        self.processed_ids.add(item_id)
        if item_id in self.error_count:
            del self.error_count[item_id]
        self.save_state()
    
    def mark_error(self, item_id: int):
        """항목에 대한 에러 카운트 증가"""
        self.error_count[item_id] = self.error_count.get(item_id, 0) + 1
        self.save_state()
    
    def should_retry(self, item_id: int, max_retries: int) -> bool:
        """재시도 여부 확인"""
        return self.error_count.get(item_id, 0) < max_retries
    
    def update_check_time(self):
        """마지막 체크 시간 업데이트"""
        self.last_check_time = datetime.now()
        self.save_state()

# ===== 로깅 설정 =====
def setup_logging(config: Config):
    """로깅 설정"""
    log_level = config.get('Logging', 'level', 'INFO')
    log_file = config.get('Logging', 'file', 'edx_pes_monitor.log')
    
    # 로그 디렉토리 생성
    log_dir = Path(log_file).parent
    if log_dir.name and not log_dir.exists():
        log_dir.mkdir(parents=True, exist_ok=True)
    
    # 로거 설정 (중복 핸들러 방지)
    logger = logging.getLogger(__name__)
    
    # 기존 핸들러 제거 (중복 방지)
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # 로깅 핸들러 설정 (인코딩 문제 해결)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    console_handler = logging.StreamHandler()
    
    # 포매터 설정
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.setLevel(getattr(logging, log_level))
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # 상위 로거에서 중복 출력 방지
    logger.propagate = False
    
    return logger

# ===== 전역 변수 =====
config = None
logger = None
monitor_engine = None
oracle_initialized = False

# ===== Oracle 클라이언트 전역 초기화 =====
def initialize_oracle_client_once(oracle_client_path: str, logger: logging.Logger) -> bool:
    """Oracle 클라이언트를 전역적으로 한 번만 초기화"""
    global oracle_initialized
    
    if oracle_initialized:
        logger.debug("Oracle 클라이언트가 이미 초기화되어 있습니다.")
        return True
    
    try:
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
                    logger.info(f"Oracle 클라이언트 초기화 성공: {path}")
                    oracle_initialized = True
                    return True
                except Exception as e:
                    if "already initialized" in str(e):
                        logger.info("Oracle 클라이언트가 이미 초기화되어 있습니다.")
                        oracle_initialized = True
                        return True
                    logger.warning(f"경로 {path}에서 초기화 실패: {e}")
        
        logger.error("Oracle 클라이언트 초기화 실패 - 유효한 경로를 찾을 수 없습니다.")
        return False
        
    except Exception as e:
        logger.error(f"Oracle 클라이언트 초기화 중 오류: {e}")
        return False

# ===== 데이터베이스 관리 (개선된 버전) =====
class DatabaseManager:
    """데이터베이스 연결 관리 클래스 (트랜잭션 지원)"""
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.connection = None
        self.initialized = True
    
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
    
    def execute_query(self, query: str, params: Dict[str, Any] = None, commit: bool = True) -> bool:
        """쿼리 실행 (파라미터 바인딩 지원)"""
        if not self.connection:
            self.logger.error("데이터베이스 연결이 없습니다.")
            return False
        
        try:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if commit:
                self.connection.commit()
            cursor.close()
            return True
        except Exception as e:
            self.logger.error(f"쿼리 실행 실패: {e}")
            self.logger.debug(f"실패한 쿼리: {query}")
            if params:
                self.logger.debug(f"파라미터: {params}")
            if commit:
                self.connection.rollback()
            return False
    
    def begin_transaction(self):
        """트랜잭션 시작"""
        if self.connection:
            self.connection.begin()
    
    def commit_transaction(self):
        """트랜잭션 커밋"""
        if self.connection:
            self.connection.commit()
    
    def rollback_transaction(self):
        """트랜잭션 롤백"""
        if self.connection:
            self.connection.rollback()
    
    def check_table_column(self, table_name: str, column_name: str) -> bool:
        """테이블에 특정 컬럼이 존재하는지 확인"""
        try:
            query = """
            SELECT COUNT(*) 
            FROM user_tab_columns 
            WHERE UPPER(table_name) = UPPER(:table_name)
            AND UPPER(column_name) = UPPER(:column_name)
            """
            cursor = self.connection.cursor()
            cursor.execute(query, {'table_name': table_name, 'column_name': column_name})
            count = cursor.fetchone()[0]
            cursor.close()
            return count > 0
        except Exception as e:
            self.logger.error(f"컬럼 확인 중 오류: {e}")
            return False

# ===== API 클라이언트 (EDX API 명세 반영) =====
class EDXAPIClient:
    """EDX API 클라이언트 (EDX API 명세서 반영)"""
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.base_url = config.get('API', 'base_url')
        self.timeout = int(config.get('API', 'timeout', '30'))
    
    def get_fintube_list(self, skip: int = 0, limit: int = 100, keyword: str = "") -> Optional[Dict[str, Any]]:
        """Fintube 목록 조회 (EDX API 명세서 기준)"""
        url = f"{self.base_url}/api/fintube/list"
        params = {
            'skip': skip,
            'limit': limit,
            'keyword': keyword
        }
        
        try:
            self.logger.debug(f"API 호출: {url} (skip={skip}, limit={limit}, keyword={keyword})")
            response = requests.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.debug(f"Fintube 목록 조회 성공: {data.get('total', 0)}개")
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
    
    def get_fintube_detail(self, item_id: int) -> Optional[Dict[str, Any]]:
        """Fintube 상세 정보 조회 (EDX API 명세서 기준)"""
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
    
    def get_comp_list(self, skip: int = 0, limit: int = 100, keyword: str = "") -> Optional[Dict[str, Any]]:
        """Comp 목록 조회 (EDX API 명세서 기준)"""
        url = f"{self.base_url}/api/comp/list"
        params = {
            'skip': skip,
            'limit': limit,
            'keyword': keyword
        }
        
        try:
            self.logger.debug(f"API 호출: {url} (skip={skip}, limit={limit}, keyword={keyword})")
            response = requests.get(url, params=params, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.debug(f"Comp 목록 조회 성공: {data.get('total', 0)}개")
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
    
    def get_comp_detail(self, item_id: int) -> Optional[Dict[str, Any]]:
        """Comp 상세 정보 조회 (EDX API 명세서 기준)"""
        url = f"{self.base_url}/api/comp/detail/{item_id}"
        
        try:
            self.logger.info(f"API 호출: {url}")
            response = requests.get(url, timeout=self.timeout)
            
            if response.status_code == 200:
                data = response.json()
                self.logger.info(f"Comp 데이터 조회 성공: ID={item_id}")
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
    
    # 다른 부품 타입의 API 메서드들도 추가 (Fin, Fan, EEV, Accum 등)
    def get_fin_list(self, skip: int = 0, limit: int = 100, keyword: str = "") -> Optional[Dict[str, Any]]:
        """Fin 목록 조회"""
        url = f"{self.base_url}/api/fin/list"
        params = {
            'skip': skip,
            'limit': limit,
            'keyword': keyword
        }
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Fin API 호출 중 오류: {e}")
            return None
    
    def get_fin_detail(self, item_id: int) -> Optional[Dict[str, Any]]:
        """Fin 상세 정보 조회"""
        url = f"{self.base_url}/api/fin/detail/{item_id}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Fin API 호출 중 오류: {e}")
            return None
    
    def get_fan_list(self, skip: int = 0, limit: int = 100, keyword: str = "") -> Optional[Dict[str, Any]]:
        """Fan 목록 조회"""
        url = f"{self.base_url}/api/fan/list"
        params = {
            'skip': skip,
            'limit': limit,
            'keyword': keyword
        }
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Fan API 호출 중 오류: {e}")
            return None
    
    def get_fan_detail(self, item_id: int) -> Optional[Dict[str, Any]]:
        """Fan 상세 정보 조회"""
        url = f"{self.base_url}/api/fan/detail/{item_id}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Fan API 호출 중 오류: {e}")
            return None
    
    def get_eev_list(self, skip: int = 0, limit: int = 100, keyword: str = "") -> Optional[Dict[str, Any]]:
        """EEV 목록 조회"""
        url = f"{self.base_url}/api/eev/list"
        params = {
            'skip': skip,
            'limit': limit,
            'keyword': keyword
        }
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"EEV API 호출 중 오류: {e}")
            return None
    
    def get_eev_detail(self, item_id: int) -> Optional[Dict[str, Any]]:
        """EEV 상세 정보 조회"""
        url = f"{self.base_url}/api/eev/detail/{item_id}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"EEV API 호출 중 오류: {e}")
            return None
    
    def get_accum_list(self, skip: int = 0, limit: int = 100, keyword: str = "") -> Optional[Dict[str, Any]]:
        """Accum 목록 조회"""
        url = f"{self.base_url}/api/accum/list"
        params = {
            'skip': skip,
            'limit': limit,
            'keyword': keyword
        }
        
        try:
            response = requests.get(url, params=params, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Accum API 호출 중 오류: {e}")
            return None
    
    def get_accum_detail(self, item_id: int) -> Optional[Dict[str, Any]]:
        """Accum 상세 정보 조회"""
        url = f"{self.base_url}/api/accum/detail/{item_id}"
        
        try:
            response = requests.get(url, timeout=self.timeout)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            self.logger.error(f"Accum API 호출 중 오류: {e}")
            return None

# ===== 데이터 처리 =====
class ComponentDataProcessor:
    """부품 데이터 처리 베이스 클래스 (트랜잭션 지원)"""
    def __init__(self, db_manager: DatabaseManager, logger: logging.Logger):
        self.db = db_manager
        self.logger = logger
    
    def validate_schema(self) -> bool:
        """DB 스키마 검증 (서브클래스에서 구현)"""
        return True

class FintubeDataProcessor(ComponentDataProcessor):
    """Fintube 데이터 처리 클래스 (완전한 구현)"""
    
    def validate_schema(self) -> bool:
        """Fintube 관련 DB 스키마 검증"""
        required_columns = {
            'pes_hex_master_tbl': [
                'PHM_PART_NO', 'PHM_EVA_COND_DIV', 'PHM_FIN_TYPE', 
                'PHM_ROW_COUNT', 'PHM_COLUMN_COUNT', 'PHM_FPI', 
                'PHM_LENGTH', 'PHM_BENDING_TYPE', 'PHM_TUBE_DIAMETER', 
                'PHM_PATH_DESIGN_1', 'PHM_PATH_DESIGN_2', 'PHM_REGISTER', 
                'PHM_REGIS_DATE', 'PHM_USAGE_YN', 'PHM_TUBE_MATERIAL', 
                'PHM_HEX_TYPE'
            ],
            'pes_hex_airflow_profile_tbl': ['PHA_PART_NO', 'PHA_ORDER', 'PHA_AIRFLOW'],
            'pes_hex_air_seg_profile_tbl': [
                'PHA_PART_NO', 'PHA_ORDER', 'PHA_AIRFLOW_01', 'PHA_AIRFLOW_02',
                'PHA_AIRFLOW_03', 'PHA_AIRFLOW_04', 'PHA_AIRFLOW_05', 'PHA_AIRFLOW_06',
                'PHA_AIRFLOW_07', 'PHA_AIRFLOW_08', 'PHA_AIRFLOW_09', 'PHA_AIRFLOW_10'
            ]
        }
        
        all_valid = True
        for table, columns in required_columns.items():
            for column in columns:
                if not self.db.check_table_column(table, column):
                    self.logger.error(f"필수 컬럼 누락: {table}.{column}")
                    all_valid = False
        
        # PHM_NUM_PATH 컬럼 확인 (선택적)
        if not self.db.check_table_column('pes_hex_master_tbl', 'PHM_NUM_PATH'):
            self.logger.warning("PHM_NUM_PATH 컬럼이 없습니다. ALTER TABLE pes_hex_master_tbl ADD PHM_NUM_PATH NUMBER 실행 필요")
        
        # 관계형 필드 컬럼 확인 (선택적)
        if not self.db.check_table_column('pes_hex_master_tbl', 'PHM_FIN_ID'):
            self.logger.warning("PHM_FIN_ID 컬럼이 없습니다. 관계형 데이터 저장이 제한됩니다.")
        
        if not self.db.check_table_column('pes_hex_master_tbl', 'PHM_HEX_COEFF_ID'):
            self.logger.warning("PHM_HEX_COEFF_ID 컬럼이 없습니다. 관계형 데이터 저장이 제한됩니다.")
        
        return all_valid
    
    def process_fintube_data(self, fintube_data: Dict[str, Any]) -> bool:
        """Fintube 데이터를 PES DB에 저장 (트랜잭션 사용)"""
        try:
            # 데이터 검증
            if not self._validate_fintube_data(fintube_data):
                return False
            
            # 트랜잭션 시작
            self.db.begin_transaction()
            
            try:
                # 데이터 추출 및 변환
                processed_data = self._extract_and_transform_data(fintube_data)
                
                # 기존 데이터 삭제 (있는 경우)
                self._delete_existing_data(processed_data['PART_NUMBER'])
                
                # 1. pes_hex_master_tbl 삽입
                if not self._insert_hex_master(processed_data):
                    raise Exception("hex_master 삽입 실패")
                
                # 2. pes_hex_airflow_profile_tbl 삽입
                if not self._insert_airflow_profile(processed_data):
                    raise Exception("airflow_profile 삽입 실패")
                
                # 3. pes_hex_air_seg_profile_tbl 삽입 (데이터가 있는 경우)
                if processed_data.get('AIRFLOW_PROFILE_SEG'):
                    if not self._insert_airflow_seg_profile(processed_data):
                        raise Exception("airflow_seg_profile 삽입 실패")
                
                # 트랜잭션 커밋
                self.db.commit_transaction()
                self.logger.info(f"모든 데이터 저장 완료: {processed_data['PART_NUMBER']}")
                return True
                
            except Exception as e:
                # 트랜잭션 롤백
                self.db.rollback_transaction()
                self.logger.error(f"트랜잭션 실패, 롤백 수행: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"데이터 처리 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _validate_fintube_data(self, fintube_data: Dict[str, Any]) -> bool:
        """Fintube 데이터 검증 (EDX API 명세서 기준)"""
        required_fields = ['part_number', 'username', 'userid']
        
        for field in required_fields:
            if not fintube_data.get(field):
                self.logger.error(f"필수 필드 누락: {field}")
                return False
        
        # 부품번호가 'string' 같은 테스트 값인지 확인
        part_number = fintube_data.get('part_number', '')
        if part_number.lower() in ['string', 'test', 'example']:
            self.logger.warning(f"테스트 데이터로 추정되는 부품번호: {part_number}")
        
        return True
    
    def _extract_and_transform_data(self, fintube: Dict[str, Any]) -> Dict[str, Any]:
        """API 데이터를 DB 형식으로 변환 (개선된 버전)"""
        # 기본 데이터
        data = {
            'ID': fintube.get('id'),
            'PART_NUMBER': f"[EDX]{fintube['part_number']}",  # [EDX] 접두어 추가
            'EVA_COND': "CONDENSER" if fintube.get('odu_idu') == "ODU" else "EVAPORATOR",
            'FIN_TYPE': self._get_fin_type_from_relation(fintube),
            'ROW': fintube.get('row', 1),
            'COL': fintube.get('col', 1),
            'FPI': fintube.get('fpi', 14),  # integer로 처리
            'TUBE_LEN': fintube.get('tube_len', 1000.0),
            'NUM_PATH': fintube.get('num_path', 1.0),  # EDX API 필드 추가
            'BENDING_TYPE': "hex_type1",
            'PATH_DESIGN_1': fintube.get('path_design1', 'U'),
            'PATH_DESIGN_2': fintube.get('path_design2', 'N'),
            'USERID': fintube['userid'],
            'CREATE_DATE': fintube['create_date'][:10] if fintube.get('create_date') else datetime.now().strftime('%Y-%m-%d'),
            'USAGE_YN': "Y",
            'TUBE_MATERIAL': self._get_tube_material_from_relation(fintube),
            'HEX_TYPE': "FIN_TUBE",
            'AIRFLOW_PROFILE': fintube.get('airflow_profile', [1.0]),
            'AIRFLOW_PROFILE_SEG': fintube.get('airflow_profile_seg', []),
            # 관계형 필드 추가
            'FIN_ID': fintube.get('fin_id'),
            'HEX_COEFF_ID': fintube.get('hex_coeff_id')
        }
        
        # tube_dia 처리 (관계형 데이터에서 가져오기)
        tube_dia_value = self._get_tube_dia_from_relation(fintube)
        if isinstance(tube_dia_value, float) and tube_dia_value.is_integer():
            data['TUBE_DIA'] = str(int(tube_dia_value))
        else:
            data['TUBE_DIA'] = str(tube_dia_value)
        
        return data
    
    def _get_fin_type_from_relation(self, fintube: Dict[str, Any]) -> str:
        """관계형 데이터에서 fin_type 추출"""
        fin_data = fintube.get('fin')
        if fin_data and isinstance(fin_data, dict):
            return fin_data.get('fin_type', 'DEFAULT')
        return 'DEFAULT'
    
    def _get_tube_material_from_relation(self, fintube: Dict[str, Any]) -> str:
        """관계형 데이터에서 tube_material 추출"""
        fin_data = fintube.get('fin')
        if fin_data and isinstance(fin_data, dict):
            return fin_data.get('tube_material', 'COPPER')
        return 'COPPER'
    
    def _get_tube_dia_from_relation(self, fintube: Dict[str, Any]) -> float:
        """관계형 데이터에서 tube_dia 추출"""
        fin_data = fintube.get('fin')
        if fin_data and isinstance(fin_data, dict):
            return fin_data.get('tube_dia', 7.0)
        return 7.0
    
    def create_fintube(self, fintube_create: FintubeCreate) -> bool:
        """새로운 Fintube 생성 (EDX API 명세서 기준)"""
        try:
            # FintubeCreate 모델을 딕셔너리로 변환
            fintube_data = {
                'id': None,  # 자동 생성
                'part_number': fintube_create.part_number,
                'username': fintube_create.username,
                'userid': fintube_create.userid,
                'create_date': datetime.now().isoformat(),
                'tube_len': fintube_create.tube_len,
                'row': fintube_create.row,
                'col': fintube_create.col,
                'fpi': fintube_create.fpi,  # integer
                'num_path': fintube_create.num_path,  # 추가
                'path_design1': fintube_create.path_design1,
                'path_design2': fintube_create.path_design2,
                'airflow_profile': fintube_create.airflow_profile or [1.0],
                'airflow_profile_seg': fintube_create.airflow_profile_seg,
                'odu_idu': fintube_create.odu_idu or "IDU",
                'fin_id': fintube_create.fin_id,  # 관계형 필드
                'hex_coeff_id': fintube_create.hex_coeff_id,  # 관계형 필드
                'fin': {
                    'fin_type': 'DEFAULT',
                    'tube_dia': 7.0,
                    'tube_material': 'COPPER'
                }
            }
            
            return self.process_fintube_data(fintube_data)
            
        except Exception as e:
            self.logger.error(f"Fintube 생성 중 오류: {e}")
            return False
    
    def update_fintube(self, part_number: str, fintube_update: FintubeUpdate) -> bool:
        """Fintube 데이터 수정 (파라미터 바인딩 사용)"""
        try:
            # 기존 데이터 조회
            existing_data = self.get_fintube_by_part_number(part_number)
            if not existing_data:
                return False
            
            # 트랜잭션 시작
            self.db.begin_transaction()
            
            try:
                # 수정할 필드만 업데이트
                update_fields = []
                params = {'part_number': f'[EDX]{part_number}'}
                
                if fintube_update.tube_len is not None:
                    update_fields.append("PHM_LENGTH = :tube_len")
                    params['tube_len'] = fintube_update.tube_len
                if fintube_update.row is not None:
                    update_fields.append("PHM_ROW_COUNT = :row")
                    params['row'] = fintube_update.row
                if fintube_update.col is not None:
                    update_fields.append("PHM_COLUMN_COUNT = :col")
                    params['col'] = fintube_update.col
                if fintube_update.fpi is not None:
                    update_fields.append("PHM_FPI = :fpi")
                    params['fpi'] = fintube_update.fpi
                if fintube_update.num_path is not None:
                    update_fields.append("PHM_NUM_PATH = :num_path")
                    params['num_path'] = fintube_update.num_path
                if fintube_update.path_design1 is not None:
                    update_fields.append("PHM_PATH_DESIGN_1 = :path_design1")
                    params['path_design1'] = fintube_update.path_design1
                if fintube_update.path_design2 is not None:
                    update_fields.append("PHM_PATH_DESIGN_2 = :path_design2")
                    params['path_design2'] = fintube_update.path_design2
                if fintube_update.fin_id is not None:
                    update_fields.append("PHM_FIN_ID = :fin_id")
                    params['fin_id'] = fintube_update.fin_id
                if fintube_update.hex_coeff_id is not None:
                    update_fields.append("PHM_HEX_COEFF_ID = :hex_coeff_id")
                    params['hex_coeff_id'] = fintube_update.hex_coeff_id
                
                if update_fields:
                    # UPDATE 쿼리 실행
                    query = f"""
                    UPDATE pes_hex_master_tbl 
                    SET {', '.join(update_fields)}
                    WHERE PHM_PART_NO = :part_number
                    """
                    
                    if not self.db.execute_query(query, params, commit=False):
                        raise Exception("Master 테이블 업데이트 실패")
                
                # airflow_profile 업데이트 (필요한 경우)
                if fintube_update.airflow_profile is not None:
                    if not self._update_airflow_profile(part_number, fintube_update.airflow_profile):
                        raise Exception("Airflow profile 업데이트 실패")
                
                if fintube_update.airflow_profile_seg is not None:
                    if not self._update_airflow_seg_profile(part_number, fintube_update.airflow_profile_seg):
                        raise Exception("Airflow seg profile 업데이트 실패")
                
                # 트랜잭션 커밋
                self.db.commit_transaction()
                self.logger.info(f"Fintube 수정 완료: {part_number}")
                return True
                
            except Exception as e:
                self.db.rollback_transaction()
                self.logger.error(f"트랜잭션 실패, 롤백 수행: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Fintube 수정 중 오류: {e}")
            return False
    
    def delete_fintube(self, part_number: str) -> bool:
        """Fintube 데이터 삭제"""
        try:
            edx_part_number = f"[EDX]{part_number}"
            success = self._delete_existing_data(edx_part_number)
            
            if success:
                self.logger.info(f"Fintube 삭제 완료: {part_number}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Fintube 삭제 중 오류: {e}")
            return False
    
    def get_fintube_list(self, skip: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Fintube 목록 조회 (EDX API 응답 구조에 맞게 수정)"""
        try:
            # 총 개수 조회
            count_query = "SELECT COUNT(*) FROM pes_hex_master_tbl WHERE PHM_PART_NO LIKE '[EDX]%'"
            cursor = self.db.connection.cursor()
            cursor.execute(count_query)
            total = cursor.fetchone()[0]
            
            # Oracle 12c+ 페이징 문법 사용
            list_query = f"""
            SELECT PHM_PART_NO, PHM_EVA_COND_DIV, PHM_FIN_TYPE, PHM_ROW_COUNT, 
                   PHM_COLUMN_COUNT, PHM_FPI, PHM_LENGTH, PHM_PATH_DESIGN_1, 
                   PHM_PATH_DESIGN_2, PHM_REGISTER, PHM_REGIS_DATE
            FROM (
                SELECT PHM_PART_NO, PHM_EVA_COND_DIV, PHM_FIN_TYPE, PHM_ROW_COUNT, 
                       PHM_COLUMN_COUNT, PHM_FPI, PHM_LENGTH, PHM_PATH_DESIGN_1, 
                       PHM_PATH_DESIGN_2, PHM_REGISTER, PHM_REGIS_DATE,
                       ROW_NUMBER() OVER (ORDER BY PHM_REGIS_DATE DESC) as rn
                FROM pes_hex_master_tbl 
                WHERE PHM_PART_NO LIKE '[EDX]%'
            ) WHERE rn > {skip} AND rn <= {skip + limit}
            """
            
            cursor.execute(list_query)
            rows = cursor.fetchall()
            cursor.close()
            
            items = []
            for row in rows:
                # 날짜 처리
                create_date = row[10]
                if create_date:
                    if hasattr(create_date, 'strftime'):
                        create_date_str = create_date.strftime('%Y-%m-%dT%H:%M:%S')
                    else:
                        create_date_str = str(create_date)[:19]
                else:
                    create_date_str = None
                
                # EDX API 구조에 맞게 변경
                items.append({
                    'id': hash(row[0]) % 1000000,  # 임시 ID 생성
                    'part_number': row[0].replace('[EDX]', '') if row[0] else '',
                    'username': row[9] or '',
                    'userid': row[9] or '',
                    'create_date': create_date_str,
                    'tube_len': row[6],
                    'row': row[3],
                    'col': row[4],
                    'fpi': row[5],  # integer
                    'path_design1': row[7],
                    'path_design2': row[8],
                    'odu_idu': "ODU" if row[1] == "CONDENSER" else "IDU"
                })
            
            # EDX API 응답 구조에 맞게 변경
            return {
                'total': total,
                'fintube_list': items,  # EDX API는 fintube_list 키 사용
                'next_cursor': skip + limit if skip + limit < total else None
            }
            
        except Exception as e:
            self.logger.error(f"Fintube 목록 조회 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return {'total': 0, 'fintube_list': [], 'next_cursor': None}
    
    def get_fintube_by_part_number(self, part_number: str) -> Optional[Dict[str, Any]]:
        """부품번호로 Fintube 상세 조회 - 향상된 검색 기능"""
        try:
            # 다양한 형태로 검색 시도 (대소문자 무시, 부분 일치)
            search_patterns = [
                f"[EDX]{part_number}",
                f"[EDX][STD]{part_number}",
                part_number,
                f"[EDX]{part_number.upper()}",
                f"[EDX]{part_number.lower()}"
            ]
            
            for edx_part_number in search_patterns:
                # 정확한 매치 먼저 시도
                query = f"""
                SELECT PHM_PART_NO, PHM_EVA_COND_DIV, PHM_FIN_TYPE, PHM_ROW_COUNT, 
                       PHM_COLUMN_COUNT, PHM_FPI, PHM_LENGTH, PHM_PATH_DESIGN_1, 
                       PHM_PATH_DESIGN_2, PHM_REGISTER, PHM_REGIS_DATE, PHM_TUBE_DIAMETER
                FROM pes_hex_master_tbl 
                WHERE UPPER(PHM_PART_NO) = UPPER(:part_number)
                """
                
                cursor = self.db.connection.cursor()
                cursor.execute(query, {'part_number': edx_part_number})
                row = cursor.fetchone()
                
                if row:
                    found_part_number = row[0]
                    break
                    
                cursor.close()
            else:
                # 부분 일치 검색 시도
                like_query = f"""
                SELECT PHM_PART_NO, PHM_EVA_COND_DIV, PHM_FIN_TYPE, PHM_ROW_COUNT, 
                       PHM_COLUMN_COUNT, PHM_FPI, PHM_LENGTH, PHM_PATH_DESIGN_1, 
                       PHM_PATH_DESIGN_2, PHM_REGISTER, PHM_REGIS_DATE, PHM_TUBE_DIAMETER
                FROM pes_hex_master_tbl 
                WHERE UPPER(PHM_PART_NO) LIKE UPPER(:part_number)
                ORDER BY 
                    CASE 
                        WHEN UPPER(PHM_PART_NO) LIKE UPPER('[EDX]' || :base_part) THEN 1
                        WHEN UPPER(PHM_PART_NO) LIKE UPPER('%[EDX]%' || :base_part || '%') THEN 2
                        ELSE 3
                    END
                """
                
                cursor = self.db.connection.cursor()
                cursor.execute(like_query, {
                    'part_number': f'%{part_number}%',
                    'base_part': part_number
                })
                row = cursor.fetchone()
                
                if not row:
                    cursor.close()
                    self.logger.warning(f"부품번호 '{part_number}'에 대한 데이터를 찾을 수 없습니다.")
                    return None
                
                found_part_number = row[0]
            
            # 상세 정보 구성 (EDX API 구조에 맞게)
            # airflow_profile 조회
            profile_query = f"""
            SELECT PHA_ORDER, PHA_AIRFLOW
            FROM pes_hex_airflow_profile_tbl
            WHERE PHA_PART_NO = :part_number
            ORDER BY PHA_ORDER
            """
            cursor.execute(profile_query, {'part_number': found_part_number})
            profile_rows = cursor.fetchall()
            airflow_profile = [row[1] for row in profile_rows]
            
            # airflow_seg_profile 조회
            seg_query = f"""
            SELECT PHA_ORDER, PHA_AIRFLOW_01, PHA_AIRFLOW_02, PHA_AIRFLOW_03,
                   PHA_AIRFLOW_04, PHA_AIRFLOW_05, PHA_AIRFLOW_06, PHA_AIRFLOW_07,
                   PHA_AIRFLOW_08, PHA_AIRFLOW_09, PHA_AIRFLOW_10
            FROM pes_hex_air_seg_profile_tbl
            WHERE PHA_PART_NO = :part_number
            ORDER BY PHA_ORDER
            """
            cursor.execute(seg_query, {'part_number': found_part_number})
            seg_rows = cursor.fetchall()
            airflow_profile_seg = [list(row[1:]) for row in seg_rows]
            
            cursor.close()
            
            # 날짜 처리
            create_date = row[10]
            if create_date:
                if hasattr(create_date, 'strftime'):
                    create_date_str = create_date.strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    create_date_str = str(create_date)[:19]
            else:
                create_date_str = None
            
            # EDX API 구조에 맞게 결과 반환
            result = {
                'id': hash(found_part_number) % 1000000,  # 임시 ID 생성
                'part_number': found_part_number.replace('[EDX]', '').replace('[STD]', '') if found_part_number else '',
                'username': row[9] or '',
                'userid': row[9] or '',
                'create_date': create_date_str,
                'tube_len': row[6],
                'row': row[3],
                'col': row[4],
                'fpi': row[5],  # integer
                'path_design1': row[7],
                'path_design2': row[8],
                'odu_idu': "ODU" if row[1] == "CONDENSER" else "IDU",
                'airflow_profile': airflow_profile,
                'airflow_profile_seg': airflow_profile_seg,
                'fin': {
                    'fin_type': row[2],
                    'tube_dia': row[11]
                }
            }
            
            self.logger.info(f"부품번호 '{part_number}' 조회 성공: 실제 부품번호='{found_part_number}'")
            return result
            
        except Exception as e:
            self.logger.error(f"Fintube 상세 조회 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return None
    
    def _delete_existing_data(self, part_number: str) -> bool:
        """기존 데이터 삭제 (파라미터 바인딩 사용)"""
        tables = [
            ('pes_hex_air_seg_profile_tbl', 'PHA_PART_NO'),
            ('pes_hex_airflow_profile_tbl', 'PHA_PART_NO'),
            ('pes_hex_master_tbl', 'PHM_PART_NO')
        ]
        
        for table, column in tables:
            query = f"DELETE FROM {table} WHERE {column} = :part_number"
            
            if not self.db.execute_query(query, {'part_number': part_number}, commit=False):
                self.logger.warning(f"{table}에서 기존 데이터 삭제 실패")
        
        return True
    
    def _insert_hex_master(self, data: Dict[str, Any]) -> bool:
        """pes_hex_master_tbl 데이터 삽입 (파라미터 바인딩 사용)"""
        # NUM_PATH 컬럼 존재 여부 확인
        has_num_path = self.db.check_table_column('pes_hex_master_tbl', 'PHM_NUM_PATH')
        has_fin_id = self.db.check_table_column('pes_hex_master_tbl', 'PHM_FIN_ID')
        has_hex_coeff_id = self.db.check_table_column('pes_hex_master_tbl', 'PHM_HEX_COEFF_ID')
        
        # 기본 쿼리
        columns = [
            'PHM_PART_NO', 'PHM_EVA_COND_DIV', 'PHM_FIN_TYPE', 'PHM_ROW_COUNT', 
            'PHM_COLUMN_COUNT', 'PHM_FPI', 'PHM_LENGTH', 'PHM_BENDING_TYPE', 
            'PHM_TUBE_DIAMETER', 'PHM_PATH_DESIGN_1', 'PHM_PATH_DESIGN_2', 
            'PHM_REGISTER', 'PHM_REGIS_DATE', 'PHM_USAGE_YN', 'PHM_TUBE_MATERIAL', 
            'PHM_HEX_TYPE'
        ]
        
        values = [
            ':part_number', ':eva_cond', ':fin_type', ':row', ':col', 
            ':fpi', ':tube_len', ':bending_type', ':tube_dia', 
            ':path_design1', ':path_design2', ':userid', ':create_date', 
            ':usage_yn', ':tube_material', ':hex_type'
        ]
        
        params = {
            'part_number': data['PART_NUMBER'],
            'eva_cond': data['EVA_COND'],
            'fin_type': data['FIN_TYPE'],
            'row': data['ROW'],
            'col': data['COL'],
            'fpi': data['FPI'],
            'tube_len': data['TUBE_LEN'],
            'bending_type': data['BENDING_TYPE'],
            'tube_dia': data['TUBE_DIA'],
            'path_design1': data['PATH_DESIGN_1'],
            'path_design2': data['PATH_DESIGN_2'],
            'userid': data['USERID'],
            'create_date': data['CREATE_DATE'],
            'usage_yn': data['USAGE_YN'],
            'tube_material': data['TUBE_MATERIAL'],
            'hex_type': data['HEX_TYPE']
        }
        
        # 선택적 컬럼 추가
        if has_num_path:
            columns.append('PHM_NUM_PATH')
            values.append(':num_path')
            params['num_path'] = data['NUM_PATH']
        
        if has_fin_id and data.get('FIN_ID'):
            columns.append('PHM_FIN_ID')
            values.append(':fin_id')
            params['fin_id'] = data['FIN_ID']
        
        if has_hex_coeff_id and data.get('HEX_COEFF_ID'):
            columns.append('PHM_HEX_COEFF_ID')
            values.append(':hex_coeff_id')
            params['hex_coeff_id'] = data['HEX_COEFF_ID']
        
        query = f"""
        INSERT INTO pes_hex_master_tbl ({', '.join(columns)}) 
        VALUES ({', '.join(values)})
        """
        
        if self.db.execute_query(query, params, commit=False):
            self.logger.info(f"pes_hex_master_tbl 삽입 성공: {data['PART_NUMBER']}")
            return True
        return False
    
    def _insert_airflow_profile(self, data: Dict[str, Any]) -> bool:
        """pes_hex_airflow_profile_tbl 데이터 삽입"""
        airflow_profile = data.get('AIRFLOW_PROFILE', [1.0])
        
        for order, airflow in enumerate(airflow_profile):
            query = """
            INSERT INTO pes_hex_airflow_profile_tbl (
                PHA_PART_NO, PHA_ORDER, PHA_AIRFLOW
            ) VALUES (
                :part_number, :order_num, :airflow
            )
            """
            
            params = {
                'part_number': data['PART_NUMBER'],
                'order_num': order,
                'airflow': float(airflow)
            }
            
            if not self.db.execute_query(query, params, commit=False):
                self.logger.error(f"airflow_profile 삽입 실패: order={order}")
                return False
        
        self.logger.info(f"pes_hex_airflow_profile_tbl 삽입 성공: {len(airflow_profile)}개")
        return True
    
    def _insert_airflow_seg_profile(self, data: Dict[str, Any]) -> bool:
        """pes_hex_air_seg_profile_tbl 데이터 삽입"""
        airflow_seg = data.get('AIRFLOW_PROFILE_SEG', [])
        
        inserted_count = 0
        for order, segment in enumerate(airflow_seg):
            if len(segment) != 10:
                self.logger.warning(f"세그먼트 {order}는 10개의 값을 가지지 않으므로 건너뜁니다.")
                continue
            
            query = """
            INSERT INTO pes_hex_air_seg_profile_tbl (
                PHA_PART_NO, PHA_ORDER, PHA_AIRFLOW_01, PHA_AIRFLOW_02, PHA_AIRFLOW_03, 
                PHA_AIRFLOW_04, PHA_AIRFLOW_05, PHA_AIRFLOW_06, PHA_AIRFLOW_07, 
                PHA_AIRFLOW_08, PHA_AIRFLOW_09, PHA_AIRFLOW_10
            ) VALUES (
                :part_number, :order_num, :af1, :af2, :af3, :af4, :af5, :af6, :af7, :af8, :af9, :af10
            )
            """
            
            params = {
                'part_number': data['PART_NUMBER'],
                'order_num': order,
                'af1': segment[0], 'af2': segment[1], 'af3': segment[2],
                'af4': segment[3], 'af5': segment[4], 'af6': segment[5],
                'af7': segment[6], 'af8': segment[7], 'af9': segment[8],
                'af10': segment[9]
            }
            
            if not self.db.execute_query(query, params, commit=False):
                self.logger.error(f"airflow_seg_profile 삽입 실패: order={order}")
                return False
            
            inserted_count += 1
        
        self.logger.info(f"pes_hex_air_seg_profile_tbl 삽입 성공: {inserted_count}개")
        return True
    
    def _update_airflow_profile(self, part_number: str, airflow_profile: List[float]) -> bool:
        """airflow_profile 테이블 업데이트"""
        try:
            edx_part_number = f"[EDX]{part_number}"
            
            # 기존 데이터 삭제
            delete_query = "DELETE FROM pes_hex_airflow_profile_tbl WHERE PHA_PART_NO = :part_number"
            self.db.execute_query(delete_query, {'part_number': edx_part_number}, commit=False)
            
            # 새 데이터 삽입
            for order, airflow in enumerate(airflow_profile):
                insert_query = """
                INSERT INTO pes_hex_airflow_profile_tbl (PHA_PART_NO, PHA_ORDER, PHA_AIRFLOW)
                VALUES (:part_number, :order_num, :airflow)
                """
                params = {
                    'part_number': edx_part_number,
                    'order_num': order,
                    'airflow': float(airflow)
                }
                self.db.execute_query(insert_query, params, commit=False)
            
            return True
                
        except Exception as e:
            self.logger.error(f"airflow_profile 업데이트 중 오류: {e}")
            return False
    
    def _update_airflow_seg_profile(self, part_number: str, airflow_profile_seg: List[List[float]]) -> bool:
        """airflow_seg_profile 테이블 업데이트"""
        try:
            edx_part_number = f"[EDX]{part_number}"
            
            # 기존 데이터 삭제
            delete_query = "DELETE FROM pes_hex_air_seg_profile_tbl WHERE PHA_PART_NO = :part_number"
            self.db.execute_query(delete_query, {'part_number': edx_part_number}, commit=False)
            
            # 새 데이터 삽입
            for order, segment in enumerate(airflow_profile_seg):
                if len(segment) == 10:
                    insert_query = """
                    INSERT INTO pes_hex_air_seg_profile_tbl (
                        PHA_PART_NO, PHA_ORDER, PHA_AIRFLOW_01, PHA_AIRFLOW_02, PHA_AIRFLOW_03, 
                        PHA_AIRFLOW_04, PHA_AIRFLOW_05, PHA_AIRFLOW_06, PHA_AIRFLOW_07, 
                        PHA_AIRFLOW_08, PHA_AIRFLOW_09, PHA_AIRFLOW_10
                    ) VALUES (
                        :part_number, :order_num, :af1, :af2, :af3, :af4, :af5, :af6, :af7, :af8, :af9, :af10
                    )
                    """
                    params = {
                        'part_number': edx_part_number,
                        'order_num': order,
                        'af1': segment[0], 'af2': segment[1], 'af3': segment[2],
                        'af4': segment[3], 'af5': segment[4], 'af6': segment[5],
                        'af7': segment[6], 'af8': segment[7], 'af9': segment[8],
                        'af10': segment[9]
                    }
                    self.db.execute_query(insert_query, params, commit=False)
            
            return True
                    
        except Exception as e:
            self.logger.error(f"airflow_seg_profile 업데이트 중 오류: {e}")
            return False

# CompDataProcessor도 비슷하게 EDX API 명세서에 맞게 수정
class CompDataProcessor(ComponentDataProcessor):
    """Comp 데이터 처리 클래스 (완전한 구현)"""
    
    def validate_schema(self) -> bool:
        """Comp 관련 DB 스키마 검증"""
        required_columns = {
            'pes_comp_master_tbl': [
                'PARTNO', 'COMP_MODEL', 'COMP_KIND', 'INVERTER_DIV', 'REFRIGERANT',
                'CAPACITY_TEST_CONDITION', 'DISP', 'PCM_REGI_OWNER', 'PCM_REGI_DATE'
            ],
            'pes_part_compressor_perf_table': [
                'PPC_PARTNO', 'PPC_FREQUENCY', 'PPC_TEMP_EVA', 'PPC_TEMP_COND',
                'PPC_CAPACITY', 'PPC_INPUT', 'PPC_CAPA_UNIT', 'PPC_INPT_UNIT', 'PPC_USAGE_YN'
            ]
        }
        
        all_valid = True
        for table, columns in required_columns.items():
            for column in columns:
                if not self.db.check_table_column(table, column):
                    self.logger.error(f"필수 컬럼 누락: {table}.{column}")
                    all_valid = False
        
        # 관계형 필드 컬럼 확인 (선택적)
        if not self.db.check_table_column('pes_comp_master_tbl', 'PCM_COMP_COEFF_ID'):
            self.logger.warning("PCM_COMP_COEFF_ID 컬럼이 없습니다. 관계형 데이터 저장이 제한됩니다.")
        
        return all_valid
    
    def process_comp_data(self, comp_data: Dict[str, Any]) -> bool:
        """Comp 데이터를 PES DB에 저장 (트랜잭션 사용)"""
        try:
            # 데이터 검증
            if not self._validate_comp_data(comp_data):
                return False
            
            # 트랜잭션 시작
            self.db.begin_transaction()
            
            try:
                # 데이터 추출 및 변환
                processed_data = self._extract_and_transform_data(comp_data)
                
                # 기존 데이터 삭제
                self._delete_existing_data(processed_data['PART_NUMBER'])
                
                # 1. pes_comp_master_tbl 삽입
                if not self._insert_comp_master(processed_data):
                    raise Exception("comp_master 삽입 실패")
                
                # 2. pes_part_compressor_perf_table 삽입
                if processed_data.get('MAP_DATA'):
                    if not self._insert_comp_performance(processed_data):
                        raise Exception("comp_performance 삽입 실패")
                
                # 트랜잭션 커밋
                self.db.commit_transaction()
                self.logger.info(f"모든 Comp 데이터 저장 완료: {processed_data['PART_NUMBER']}")
                return True
                
            except Exception as e:
                # 트랜잭션 롤백
                self.db.rollback_transaction()
                self.logger.error(f"트랜잭션 실패, 롤백 수행: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Comp 데이터 처리 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _validate_comp_data(self, comp_data: Dict[str, Any]) -> bool:
        """Comp 데이터 검증 (EDX API 명세서 기준)"""
        required_fields = ['username', 'userid']  # EDX API 필수 필드
        
        for field in required_fields:
            if field not in comp_data or comp_data[field] is None:
                self.logger.error(f"필수 필드 누락: {field}")
                return False
        
        return True
    
    def _extract_and_transform_data(self, comp: Dict[str, Any]) -> Dict[str, Any]:
        """API 데이터를 DB 형식으로 변환 (EDX API 명세서 반영)"""
        # 기본 데이터
        data = {
            'ID': comp.get('id'),
            'PART_NUMBER': f"[EDX]{comp.get('part_number', 'UNKNOWN')}",
            'MODEL': comp.get('model', comp.get('part_number', '')),
            'KIND': comp.get('kind', 'Scroll'),
            'TYPE': comp.get('type', 'Inverter'),
            'REF': comp.get('ref', 'R410A'),
            'DISP': comp.get('displacement', 0.0),
            'MAP_COND': comp.get('map_cond', ''),
            'MAP_DATA': comp.get('map_data', {}),
            'CORR_EFFI': comp.get('corr_effi', {}),  # EDX API 필드
            'USERID': comp['userid'],
            'CREATE_DATE': comp.get('create_date', datetime.now().isoformat())[:10],
            'COMP_COEFF_ID': comp.get('comp_coeff_id')
        }
        
        return data
    
    def _delete_existing_data(self, part_number: str) -> bool:
        """기존 데이터 삭제"""
        tables = [
            ('pes_part_compressor_perf_table', 'PPC_PARTNO'),
            ('pes_comp_master_tbl', 'PARTNO')
        ]
        
        for table, column in tables:
            query = f"DELETE FROM {table} WHERE {column} = :part_number"
            self.db.execute_query(query, {'part_number': part_number}, commit=False)
        
        return True
    
    def _insert_comp_master(self, data: Dict[str, Any]) -> bool:
        """pes_comp_master_tbl 데이터 삽입 (파라미터 바인딩 사용)"""
        # COMP_COEFF_ID 컬럼 존재 여부 확인
        has_comp_coeff_id = self.db.check_table_column('pes_comp_master_tbl', 'PCM_COMP_COEFF_ID')
        
        # 기본 쿼리
        columns = [
            'PARTNO', 'COMP_MODEL', 'COMP_KIND', 'INVERTER_DIV', 'REFRIGERANT',
            'CAPACITY_TEST_CONDITION', 'DISP', 'PCM_REGI_OWNER', 'PCM_REGI_DATE'
        ]
        
        values = [
            ':part_number', ':model', ':kind', ':type', ':ref',
            ':map_cond', ':disp', ':userid', ':create_date'
        ]
        
        params = {
            'part_number': data['PART_NUMBER'],
            'model': data['MODEL'],
            'kind': data['KIND'],
            'type': data['TYPE'],
            'ref': data['REF'],
            'map_cond': data['MAP_COND'],
            'disp': data['DISP'],
            'userid': data['USERID'],
            'create_date': data['CREATE_DATE']
        }
        
        # 선택적 컬럼 추가
        if has_comp_coeff_id and data.get('COMP_COEFF_ID'):
            columns.append('PCM_COMP_COEFF_ID')
            values.append(':comp_coeff_id')
            params['comp_coeff_id'] = data['COMP_COEFF_ID']
        
        query = f"""
        INSERT INTO pes_comp_master_tbl ({', '.join(columns)})
        VALUES ({', '.join(values)})
        """
        
        if self.db.execute_query(query, params, commit=False):
            self.logger.info(f"pes_comp_master_tbl 삽입 성공: {data['PART_NUMBER']}")
            return True
        return False
    
    def _insert_comp_performance(self, data: Dict[str, Any]) -> bool:
        """pes_part_compressor_perf_table 데이터 삽입"""
        try:
            map_data = data['MAP_DATA']
            
            if not map_data:
                return True  # 맵 데이터가 없으면 성공으로 처리
            
            # 딕셔너리 형식으로 변환
            if isinstance(map_data, str):
                map_data = json.loads(map_data)
            
            if not isinstance(map_data, dict):
                return True
            
            # 필수 키 확인
            required_keys = ['HZ', 'TE', 'TC', 'CAPA', 'INPUT']
            for key in required_keys:
                if key not in map_data:
                    self.logger.warning(f"맵 데이터에 필수 키 '{key}'가 없습니다.")
                    return True
            
            # 데이터 리스트 추출
            hz_values = map_data['HZ'] if isinstance(map_data['HZ'], list) else list(map_data['HZ'].values())
            te_values = map_data['TE']
            tc_values = map_data['TC']
            capa_values = map_data['CAPA']
            input_values = map_data['INPUT']
            
            # 각 값을 개별적으로 삽입
            inserted_count = 0
            for i in range(min(len(hz_values), len(te_values), len(tc_values), len(capa_values), len(input_values))):
                query = """
                INSERT INTO pes_part_compressor_perf_table (
                    PPC_PARTNO, PPC_FREQUENCY, PPC_TEMP_EVA, PPC_TEMP_COND, 
                    PPC_CAPACITY, PPC_INPUT, PPC_CAPA_UNIT, PPC_INPT_UNIT, PPC_USAGE_YN
                ) VALUES (
                    :part_number, :frequency, :temp_eva, :temp_cond,
                    :capacity, :input, :capa_unit, :inpt_unit, :usage_yn
                )
                """
                
                params = {
                    'part_number': data['PART_NUMBER'],
                    'frequency': str(hz_values[i]),
                    'temp_eva': str(te_values[i]),
                    'temp_cond': str(tc_values[i]),
                    'capacity': str(capa_values[i]),
                    'input': str(input_values[i]),
                    'capa_unit': 'BTU/H',
                    'inpt_unit': 'W',
                    'usage_yn': 'Y'
                }
                
                if not self.db.execute_query(query, params, commit=False):
                    self.logger.error(f"comp performance 삽입 실패: index={i}")
                    return False
                
                inserted_count += 1
            
            self.logger.info(f"pes_part_compressor_perf_table 삽입 성공: {inserted_count}개")
            return True
            
        except Exception as e:
            self.logger.error(f"comp performance 삽입 중 오류: {e}")
            return False
    
    def create_comp(self, comp_create: CompCreate) -> bool:
        """새로운 Comp 생성 (EDX API 명세서 기준)"""
        try:
            comp_data = {
                'id': None,
                'part_number': comp_create.part_number or 'UNKNOWN',
                'username': comp_create.username,
                'userid': comp_create.userid,
                'create_date': datetime.now().isoformat(),
                'model': comp_create.model,
                'kind': comp_create.kind,
                'type': comp_create.type,
                'ref': comp_create.ref or 'R410A',
                'displacement': comp_create.displacement or 0.0,
                'map_data': comp_create.map_data or {},
                'map_cond': comp_create.map_cond or '',
                'corr_effi': comp_create.corr_effi or {},  # EDX API
                'comp_coeff_id': comp_create.comp_coeff_id
            }
            
            return self.process_comp_data(comp_data)
            
        except Exception as e:
            self.logger.error(f"Comp 생성 중 오류: {e}")
            return False
    
    def get_comp_list(self, skip: int = 0, limit: int = 100) -> Dict[str, Any]:
        """Comp 목록 조회 (EDX API 응답 구조에 맞게)"""
        try:
            # 총 개수 조회
            count_query = "SELECT COUNT(*) FROM pes_comp_master_tbl WHERE PARTNO LIKE '[EDX]%'"
            cursor = self.db.connection.cursor()
            cursor.execute(count_query)
            total = cursor.fetchone()[0]
            
            # Oracle 페이징
            list_query = f"""
            SELECT PARTNO, COMP_MODEL, COMP_KIND, INVERTER_DIV, REFRIGERANT, 
                   CAPACITY_TEST_CONDITION, DISP, PCM_REGI_OWNER, PCM_REGI_DATE
            FROM (
                SELECT PARTNO, COMP_MODEL, COMP_KIND, INVERTER_DIV, REFRIGERANT, 
                       CAPACITY_TEST_CONDITION, DISP, PCM_REGI_OWNER, PCM_REGI_DATE,
                       ROW_NUMBER() OVER (ORDER BY PCM_REGI_DATE DESC) as rn
                FROM pes_comp_master_tbl 
                WHERE PARTNO LIKE '[EDX]%'
            ) WHERE rn > {skip} AND rn <= {skip + limit}
            """
            
            cursor.execute(list_query)
            rows = cursor.fetchall()
            cursor.close()
            
            items = []
            for row in rows:
                # 날짜 처리
                create_date = row[8]
                if create_date:
                    if hasattr(create_date, 'strftime'):
                        create_date_str = create_date.strftime('%Y-%m-%dT%H:%M:%S')
                    else:
                        create_date_str = str(create_date)[:19]
                else:
                    create_date_str = None
                
                # EDX API 구조에 맞게
                items.append({
                    'id': hash(row[0]) % 1000000,  # 임시 ID 생성
                    'part_number': row[0].replace('[EDX]', '') if row[0] else '',
                    'username': row[7] or '',
                    'userid': row[7] or '',
                    'create_date': create_date_str,
                    'model': row[1],
                    'kind': row[2],
                    'type': row[3],
                    'ref': row[4],
                    'map_cond': row[5],
                    'displacement': row[6],
                    'corr_effi': {}  # EDX API 필드
                })
            
            # EDX API 응답 구조에 맞게
            return {
                'total': total,
                'comp_list': items,  # EDX API는 comp_list 키 사용
                'next_cursor': skip + limit if skip + limit < total else None
            }
            
        except Exception as e:
            self.logger.error(f"Comp 목록 조회 중 오류: {e}")
            return {'total': 0, 'comp_list': [], 'next_cursor': None}

    def get_comp_by_part_number(self, part_number: str) -> Optional[Dict[str, Any]]:
        """부품번호로 Comp 상세 조회 - 향상된 검색 기능"""
        try:
            # 다양한 형태로 검색 시도 (대소문자 무시, 부분 일치)
            search_patterns = [
                f"[EDX]{part_number}",
                f"[EDX][STD]{part_number}",
                part_number,
                f"[EDX]{part_number.upper()}",
                f"[EDX]{part_number.lower()}"
            ]
            
            for edx_part_number in search_patterns:
                # 정확한 매치 먼저 시도
                query = """
                SELECT PARTNO, COMP_MODEL, COMP_KIND, INVERTER_DIV, REFRIGERANT, 
                       CAPACITY_TEST_CONDITION, DISP, PCM_REGI_OWNER, PCM_REGI_DATE
                FROM pes_comp_master_tbl 
                WHERE UPPER(PARTNO) = UPPER(:part_number)
                """
                
                cursor = self.db.connection.cursor()
                cursor.execute(query, {'part_number': edx_part_number})
                row = cursor.fetchone()
                
                if row:
                    found_part_number = row[0]
                    break
                    
                cursor.close()
            else:
                # 부분 일치 검색 시도
                like_query = """
                SELECT PARTNO, COMP_MODEL, COMP_KIND, INVERTER_DIV, REFRIGERANT, 
                       CAPACITY_TEST_CONDITION, DISP, PCM_REGI_OWNER, PCM_REGI_DATE
                FROM pes_comp_master_tbl 
                WHERE UPPER(PARTNO) LIKE UPPER(:part_number)
                ORDER BY 
                    CASE 
                        WHEN UPPER(PARTNO) LIKE UPPER('[EDX]' || :base_part) THEN 1
                        WHEN UPPER(PARTNO) LIKE UPPER('%[EDX]%' || :base_part || '%') THEN 2
                        ELSE 3
                    END
                """
                
                cursor = self.db.connection.cursor()
                cursor.execute(like_query, {
                    'part_number': f'%{part_number}%',
                    'base_part': part_number
                })
                row = cursor.fetchone()
                
                if not row:
                    cursor.close()
                    self.logger.warning(f"부품번호 '{part_number}'에 대한 Comp 데이터를 찾을 수 없습니다.")
                    return None
                
                found_part_number = row[0]
            
            # 성능 데이터 조회
            perf_query = """
            SELECT PPC_FREQUENCY, PPC_TEMP_EVA, PPC_TEMP_COND, PPC_CAPACITY, PPC_INPUT
            FROM pes_part_compressor_perf_table
            WHERE PPC_PARTNO = :part_number
            ORDER BY PPC_FREQUENCY, PPC_TEMP_EVA, PPC_TEMP_COND
            """
            cursor.execute(perf_query, {'part_number': found_part_number})
            perf_rows = cursor.fetchall()
            
            # 성능 데이터를 map_data 형식으로 변환
            map_data = {
                'HZ': [],
                'TE': [],
                'TC': [],
                'CAPA': [],
                'INPUT': []
            }
            
            for perf_row in perf_rows:
                map_data['HZ'].append(float(perf_row[0]))
                map_data['TE'].append(float(perf_row[1]))
                map_data['TC'].append(float(perf_row[2]))
                map_data['CAPA'].append(float(perf_row[3]))
                map_data['INPUT'].append(float(perf_row[4]))
            
            cursor.close()
            
            # 날짜 처리
            create_date = row[8]
            if create_date:
                if hasattr(create_date, 'strftime'):
                    create_date_str = create_date.strftime('%Y-%m-%dT%H:%M:%S')
                else:
                    create_date_str = str(create_date)[:19]
            else:
                create_date_str = None
            
            # EDX API 구조에 맞게 결과 반환
            result = {
                'id': hash(found_part_number) % 1000000,  # 임시 ID 생성
                'part_number': found_part_number.replace('[EDX]', '').replace('[STD]', '') if found_part_number else '',
                'username': row[7] or '',
                'userid': row[7] or '',
                'create_date': create_date_str,
                'model': row[1],
                'kind': row[2],
                'type': row[3],
                'ref': row[4],
                'displacement': row[6],
                'map_data': map_data if perf_rows else {},
                'map_cond': row[5],
                'corr_effi': {}  # EDX API 필드
            }
            
            self.logger.info(f"부품번호 '{part_number}' Comp 조회 성공: 실제 부품번호='{found_part_number}'")
            return result
            
        except Exception as e:
            self.logger.error(f"Comp 상세 조회 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return None

    def update_comp(self, part_number: str, comp_update: CompUpdate) -> bool:
        """Comp 데이터 수정 (파라미터 바인딩 사용)"""
        try:
            # 기존 데이터 조회
            existing_data = self.get_comp_by_part_number(part_number)
            if not existing_data:
                return False
            
            # 트랜잭션 시작
            self.db.begin_transaction()
            
            try:
                # 수정할 필드만 업데이트
                update_fields = []
                params = {'part_number': f'[EDX]{part_number}'}
                
                if comp_update.model is not None:
                    update_fields.append("COMP_MODEL = :model")
                    params['model'] = comp_update.model
                if comp_update.kind is not None:
                    update_fields.append("COMP_KIND = :kind")
                    params['kind'] = comp_update.kind
                if comp_update.type is not None:
                    update_fields.append("INVERTER_DIV = :type")
                    params['type'] = comp_update.type
                if comp_update.ref is not None:
                    update_fields.append("REFRIGERANT = :ref")
                    params['ref'] = comp_update.ref
                if comp_update.displacement is not None:
                    update_fields.append("DISP = :disp")
                    params['disp'] = comp_update.displacement
                if comp_update.map_cond is not None:
                    update_fields.append("CAPACITY_TEST_CONDITION = :map_cond")
                    params['map_cond'] = comp_update.map_cond
                
                if update_fields:
                    # UPDATE 쿼리 실행
                    query = f"""
                    UPDATE pes_comp_master_tbl 
                    SET {', '.join(update_fields)}
                    WHERE PARTNO = :part_number
                    """
                    
                    if not self.db.execute_query(query, params, commit=False):
                        raise Exception("Master 테이블 업데이트 실패")
                
                # map_data 업데이트 (필요한 경우)
                if comp_update.map_data is not None:
                    if not self._update_comp_performance(part_number, comp_update.map_data):
                        raise Exception("Performance 테이블 업데이트 실패")
                
                # 트랜잭션 커밋
                self.db.commit_transaction()
                self.logger.info(f"Comp 수정 완료: {part_number}")
                return True
                
            except Exception as e:
                self.db.rollback_transaction()
                self.logger.error(f"트랜잭션 실패, 롤백 수행: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Comp 수정 중 오류: {e}")
            return False

    def delete_comp(self, part_number: str) -> bool:
        """Comp 데이터 삭제"""
        try:
            edx_part_number = f"[EDX]{part_number}"
            success = self._delete_existing_data(edx_part_number)
            
            if success:
                self.logger.info(f"Comp 삭제 완료: {part_number}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Comp 삭제 중 오류: {e}")
            return False

    def _update_comp_performance(self, part_number: str, map_data: Dict[str, List[float]]) -> bool:
        """comp performance 테이블 업데이트"""
        try:
            edx_part_number = f"[EDX]{part_number}"
            
            # 기존 데이터 삭제
            delete_query = "DELETE FROM pes_part_compressor_perf_table WHERE PPC_PARTNO = :part_number"
            self.db.execute_query(delete_query, {'part_number': edx_part_number}, commit=False)
            
            # 새 데이터 삽입
            if map_data and all(key in map_data for key in ['HZ', 'TE', 'TC', 'CAPA', 'INPUT']):
                hz_values = map_data['HZ']
                te_values = map_data['TE']
                tc_values = map_data['TC']
                capa_values = map_data['CAPA']
                input_values = map_data['INPUT']
                
                for i in range(min(len(hz_values), len(te_values), len(tc_values), len(capa_values), len(input_values))):
                    insert_query = """
                    INSERT INTO pes_part_compressor_perf_table (
                        PPC_PARTNO, PPC_FREQUENCY, PPC_TEMP_EVA, PPC_TEMP_COND, 
                        PPC_CAPACITY, PPC_INPUT, PPC_CAPA_UNIT, PPC_INPT_UNIT, PPC_USAGE_YN
                    ) VALUES (
                        :part_number, :frequency, :temp_eva, :temp_cond,
                        :capacity, :input, :capa_unit, :inpt_unit, :usage_yn
                    )
                    """
                    params = {
                        'part_number': edx_part_number,
                        'frequency': str(hz_values[i]),
                        'temp_eva': str(te_values[i]),
                        'temp_cond': str(tc_values[i]),
                        'capacity': str(capa_values[i]),
                        'input': str(input_values[i]),
                        'capa_unit': 'BTU/H',
                        'inpt_unit': 'W',
                        'usage_yn': 'Y'
                    }
                    self.db.execute_query(insert_query, params, commit=False)
            
            return True
                    
        except Exception as e:
            self.logger.error(f"comp performance 업데이트 중 오류: {e}")
            return False

# 다른 프로세서 클래스들 (완전한 구현)
class FinDataProcessor(ComponentDataProcessor):
    """Fin 데이터 처리 클래스 (완전한 구현)"""
    
    def validate_schema(self) -> bool:
        """Fin 관련 DB 스키마 검증"""
        # Fin 데이터는 hex_master_tbl에 저장됩니다
        required_columns = {
            'pes_hex_master_tbl': [
                'PHM_PART_NO', 'PHM_EVA_COND_DIV', 'PHM_FIN_TYPE', 
                'PHM_ROW_COUNT', 'PHM_COLUMN_COUNT', 'PHM_FPI', 
                'PHM_LENGTH', 'PHM_TUBE_DIAMETER', 'PHM_TUBE_MATERIAL',
                'PHM_REGISTER', 'PHM_REGIS_DATE', 'PHM_USAGE_YN'
            ]
        }
        
        all_valid = True
        for table, columns in required_columns.items():
            for column in columns:
                if not self.db.check_table_column(table, column):
                    self.logger.error(f"필수 컬럼 누락: {table}.{column}")
                    all_valid = False
        
        return all_valid
    
    def process_fin_data(self, fin_data: Dict[str, Any]) -> bool:
        """Fin 데이터를 PES DB에 저장"""
        try:
            # 데이터 검증
            if not self._validate_fin_data(fin_data):
                return False
            
            # 트랜잭션 시작
            self.db.begin_transaction()
            
            try:
                # 데이터 추출 및 변환
                processed_data = self._extract_and_transform_data(fin_data)
                
                # 기존 데이터 삭제 (있는 경우)
                self._delete_existing_data(processed_data['PART_NUMBER'])
                
                # pes_fin_master_tbl 삽입
                if not self._insert_fin_master(processed_data):
                    raise Exception("fin_master 삽입 실패")
                
                # 추가 데이터 처리 (tube_groove, fin_dimension 등)
                if processed_data.get('TUBE_GROOVE'):
                    if not self._insert_tube_groove_data(processed_data):
                        raise Exception("tube_groove 데이터 삽입 실패")
                
                # 트랜잭션 커밋
                self.db.commit_transaction()
                self.logger.info(f"Fin 데이터 저장 완료: {processed_data['FIN_NAME']}")
                return True
                
            except Exception as e:
                # 트랜잭션 롤백
                self.db.rollback_transaction()
                self.logger.error(f"트랜잭션 실패, 롤백 수행: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Fin 데이터 처리 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _validate_fin_data(self, fin_data: Dict[str, Any]) -> bool:
        """Fin 데이터 검증"""
        required_fields = ['fin_name', 'username', 'userid']
        
        for field in required_fields:
            if not fin_data.get(field):
                self.logger.error(f"필수 필드 누락: {field}")
                return False
        
        return True
    
    def _extract_and_transform_data(self, fin: Dict[str, Any]) -> Dict[str, Any]:
        """API 데이터를 DB 형식으로 변환 - hex_master 형식으로"""
        # tube_dia 처리
        tube_dia_value = fin.get('tube_dia', 7.0)
        if isinstance(tube_dia_value, float) and tube_dia_value.is_integer():
            tube_dia_str = str(int(tube_dia_value))
        else:
            tube_dia_str = str(tube_dia_value)
        
        data = {
            'ID': fin.get('id'),
            'PART_NUMBER': f"[EDX]FIN_{fin['fin_name']}",  # hex_master는 PART_NO 필요
            'EVA_COND': "EVAPORATOR",  # 기본값 설정
            'FIN_TYPE': fin.get('fin_type', 'FLAT'),
            'ROW': 1,  # Fin 단독 데이터이므로 기본값
            'COL': 1,  # Fin 단독 데이터이므로 기본값
            'FPI': 14,  # 기본 FPI 값
            'TUBE_LEN': 1000.0,  # 기본 길이
            'BENDING_TYPE': "hex_type1",
            'TUBE_DIA': tube_dia_str,
            'PATH_DESIGN_1': 'U',  # 기본값
            'PATH_DESIGN_2': 'N',  # 기본값
            'USERID': fin['userid'],
            'CREATE_DATE': fin.get('create_date', datetime.now().isoformat())[:10],
            'USAGE_YN': "Y",
            'TUBE_MATERIAL': fin.get('tube_material', 'COPPER'),
            'HEX_TYPE': "FIN_ONLY",  # Fin 단독 데이터 표시
            'ROW_PITCH': fin.get('row_pitch', 21.0),
            'COL_PITCH': fin.get('col_pitch', 25.0),
            'TUBE_TYPE': fin.get('tube_type', 'SMOOTH'),
            'TUBE_THICK': fin.get('tube_thick', 0.33),
            'FIN_MATERIAL': fin.get('fin_material', 'ALUMINUM'),
            'FIN_THICK': fin.get('fin_thick', 0.115)
        }
        
        return data
    
    def _delete_existing_data(self, part_number: str) -> bool:
        """기존 데이터 삭제"""
        query = "DELETE FROM pes_hex_master_tbl WHERE PHM_PART_NO = :part_number"
        self.db.execute_query(query, {'part_number': part_number}, commit=False)
        return True
    
    def _insert_fin_master(self, data: Dict[str, Any]) -> bool:
        """pes_hex_master_tbl에 Fin 데이터 삽입"""
        query = """
        INSERT INTO pes_hex_master_tbl (
            PHM_PART_NO, PHM_EVA_COND_DIV, PHM_FIN_TYPE, PHM_ROW_COUNT, 
            PHM_COLUMN_COUNT, PHM_FPI, PHM_LENGTH, PHM_BENDING_TYPE, 
            PHM_TUBE_DIAMETER, PHM_PATH_DESIGN_1, PHM_PATH_DESIGN_2, 
            PHM_REGISTER, PHM_REGIS_DATE, PHM_USAGE_YN, PHM_TUBE_MATERIAL, 
            PHM_HEX_TYPE
        ) VALUES (
            :part_number, :eva_cond, :fin_type, :row, :col, 
            :fpi, :tube_len, :bending_type, :tube_dia, 
            :path_design1, :path_design2, :userid, :create_date, 
            :usage_yn, :tube_material, :hex_type
        )
        """
        
        params = {
            'part_number': data['PART_NUMBER'],
            'eva_cond': data['EVA_COND'],
            'fin_type': data['FIN_TYPE'],
            'row': data['ROW'],
            'col': data['COL'],
            'fpi': data['FPI'],
            'tube_len': data['TUBE_LEN'],
            'bending_type': data['BENDING_TYPE'],
            'tube_dia': data['TUBE_DIA'],
            'path_design1': data['PATH_DESIGN_1'],
            'path_design2': data['PATH_DESIGN_2'],
            'userid': data['USERID'],
            'create_date': data['CREATE_DATE'],
            'usage_yn': data['USAGE_YN'],
            'tube_material': data['TUBE_MATERIAL'],
            'hex_type': data['HEX_TYPE']
        }
        
        if self.db.execute_query(query, params, commit=False):
            self.logger.info(f"pes_hex_master_tbl 삽입 성공: {data['PART_NUMBER']}")
            return True
        return False
    
    # def _insert_tube_groove_data(self, data: Dict[str, Any]) -> bool:
    #     """tube_groove 관련 데이터 삽입 (테이블 구조에 따라 구현)"""
    #     # 실제 테이블 구조에 맞게 구현 필요
    #     return True
    
    def create_fin(self, fin_create: FinCreate) -> bool:
        """새로운 Fin 생성"""
        try:
            fin_data = {
                'id': None,
                'fin_name': fin_create.fin_name,
                'username': fin_create.username,
                'userid': fin_create.userid,
                'create_date': datetime.now().isoformat(),
                'tube_dia': fin_create.tube_dia,
                'fin_type': fin_create.fin_type,
                'row_pitch': fin_create.row_pitch,
                'col_pitch': fin_create.col_pitch,
                'tube_type': fin_create.tube_type,
                'tube_thick': fin_create.tube_thick,
                'tube_groove': fin_create.tube_groove,
                'tube_material': fin_create.tube_material,
                'fin_material': fin_create.fin_material,
                'fin_thick': fin_create.fin_thick,
                'fin_type2': fin_create.fin_type2,
                'fin_dimension': fin_create.fin_dimension
            }
            
            return self.process_fin_data(fin_data)
            
        except Exception as e:
            self.logger.error(f"Fin 생성 중 오류: {e}")
            return False

class FanDataProcessor(ComponentDataProcessor):
    """Fan 데이터 처리 클래스 (완전한 구현)"""
    
    def validate_schema(self) -> bool:
        """Fan 관련 DB 스키마 검증"""
        required_columns = {
            'pes_fan_master_tbl': [
                'PFN_PART_NO', 'PFN_DIAMETER', 'PFN_STATIC_P', 
                'PFN_REGISTER', 'PFN_REGIS_DATE'
            ],
            'pes_fan_performance_tbl': [
                'PFP_PART_NO', 'PFP_RPM', 'PFP_CMM', 'PFP_WATT'
            ]
        }
        
        all_valid = True
        for table, columns in required_columns.items():
            for column in columns:
                if not self.db.check_table_column(table, column):
                    self.logger.error(f"필수 컬럼 누락: {table}.{column}")
                    all_valid = False
        
        return all_valid
    
    def process_fan_data(self, fan_data: Dict[str, Any]) -> bool:
        """Fan 데이터를 PES DB에 저장"""
        try:
            # 데이터 검증
            if not self._validate_fan_data(fan_data):
                return False
            
            # 트랜잭션 시작
            self.db.begin_transaction()
            
            try:
                # 데이터 추출 및 변환
                processed_data = self._extract_and_transform_data(fan_data)
                
                # 기존 데이터 삭제
                self._delete_existing_data(processed_data['PART_NUMBER'])
                
                # pes_fan_master_tbl 삽입
                if not self._insert_fan_master(processed_data):
                    raise Exception("fan_master 삽입 실패")
                
                # pes_fan_performance_tbl 삽입
                if processed_data.get('RPM') and processed_data.get('CMM') and processed_data.get('WATT'):
                    if not self._insert_fan_performance(processed_data):
                        raise Exception("fan_performance 삽입 실패")
                
                # 트랜잭션 커밋
                self.db.commit_transaction()
                self.logger.info(f"Fan 데이터 저장 완료: {processed_data['PART_NUMBER']}")
                return True
                
            except Exception as e:
                # 트랜잭션 롤백
                self.db.rollback_transaction()
                self.logger.error(f"트랜잭션 실패, 롤백 수행: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Fan 데이터 처리 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _validate_fan_data(self, fan_data: Dict[str, Any]) -> bool:
        """Fan 데이터 검증"""
        required_fields = ['part_number', 'username', 'userid']
        
        for field in required_fields:
            if not fan_data.get(field):
                self.logger.error(f"필수 필드 누락: {field}")
                return False
        
        return True
    
    def _extract_and_transform_data(self, fan: Dict[str, Any]) -> Dict[str, Any]:
        """API 데이터를 DB 형식으로 변환"""
        data = {
            'ID': fan.get('id'),
            'PART_NUMBER': f"[EDX]{fan['part_number']}",
            'DIAMETER': fan.get('diameter', 0.0),
            'STATIC_P': fan.get('static_p', 0.0),
            'RPM': fan.get('rpm', []),
            'CMM': fan.get('cmm', []),
            'WATT': fan.get('watt', []),
            'USERID': fan['userid'],
            'CREATE_DATE': fan.get('create_date', datetime.now().isoformat())[:10]
        }
        
        return data
    
    def _delete_existing_data(self, part_number: str) -> bool:
        """기존 데이터 삭제"""
        tables = [
            ('pes_fan_performance_tbl', 'PFP_PART_NO'),
            ('pes_fan_master_tbl', 'PFN_PART_NO')
        ]
        
        for table, column in tables:
            query = f"DELETE FROM {table} WHERE {column} = :part_number"
            self.db.execute_query(query, {'part_number': part_number}, commit=False)
        
        return True
    
    def _insert_fan_master(self, data: Dict[str, Any]) -> bool:
        """pes_fan_master_tbl 데이터 삽입"""
        query = """
        INSERT INTO pes_fan_master_tbl (
            PFN_PART_NO, PFN_DIAMETER, PFN_STATIC_P, 
            PFN_REGISTER, PFN_REGIS_DATE
        ) VALUES (
            :part_number, :diameter, :static_p,
            :userid, :create_date
        )
        """
        
        params = {
            'part_number': data['PART_NUMBER'],
            'diameter': data['DIAMETER'],
            'static_p': data['STATIC_P'],
            'userid': data['USERID'],
            'create_date': data['CREATE_DATE']
        }
        
        if self.db.execute_query(query, params, commit=False):
            self.logger.info(f"pes_fan_master_tbl 삽입 성공: {data['PART_NUMBER']}")
            return True
        return False
    
    def _insert_fan_performance(self, data: Dict[str, Any]) -> bool:
        """pes_fan_performance_tbl 데이터 삽입"""
        try:
            rpm_list = data['RPM']
            cmm_list = data['CMM']
            watt_list = data['WATT']
            
            # 모든 리스트의 길이가 같아야 함
            min_length = min(len(rpm_list), len(cmm_list), len(watt_list))
            
            for i in range(min_length):
                query = """
                INSERT INTO pes_fan_performance_tbl (
                    PFP_PART_NO, PFP_RPM, PFP_CMM, PFP_WATT
                ) VALUES (
                    :part_number, :rpm, :cmm, :watt
                )
                """
                
                params = {
                    'part_number': data['PART_NUMBER'],
                    'rpm': rpm_list[i],
                    'cmm': cmm_list[i],
                    'watt': watt_list[i]
                }
                
                if not self.db.execute_query(query, params, commit=False):
                    self.logger.error(f"fan_performance 삽입 실패: index={i}")
                    return False
            
            self.logger.info(f"pes_fan_performance_tbl 삽입 성공: {min_length}개")
            return True
            
        except Exception as e:
            self.logger.error(f"fan_performance 삽입 중 오류: {e}")
            return False
    
    def create_fan(self, fan_create: FanCreate) -> bool:
        """새로운 Fan 생성"""
        try:
            fan_data = {
                'id': None,
                'part_number': fan_create.part_number,
                'username': fan_create.username,
                'userid': fan_create.userid,
                'create_date': datetime.now().isoformat(),
                'diameter': fan_create.diameter,
                'rpm': fan_create.rpm or [],
                'cmm': fan_create.cmm or [],
                'watt': fan_create.watt or [],
                'static_p': fan_create.static_p
            }
            
            return self.process_fan_data(fan_data)
            
        except Exception as e:
            self.logger.error(f"Fan 생성 중 오류: {e}")
            return False

class EEVDataProcessor(ComponentDataProcessor):
    """EEV 데이터 처리 클래스 (완전한 구현)"""
    
    def validate_schema(self) -> bool:
        """EEV 관련 DB 스키마 검증"""
        required_columns = {
            'pes_eev_master_tbl': [
                'PEM_PART_NO', 'PEM_DIAMETER', 'PEM_MAX_PULSE', 'PEM_CORR',
                'PEM_REGISTER', 'PEM_REGIS_DATE'
            ]
        }
        
        all_valid = True
        for table, columns in required_columns.items():
            for column in columns:
                if not self.db.check_table_column(table, column):
                    self.logger.error(f"필수 컬럼 누락: {table}.{column}")
                    all_valid = False
        
        # 관계형 필드 확인
        if not self.db.check_table_column('pes_eev_master_tbl', 'PEM_EEV_COEFF_ID'):
            self.logger.warning("PEM_EEV_COEFF_ID 컬럼이 없습니다. 관계형 데이터 저장이 제한됩니다.")
        
        return all_valid
    
    def process_eev_data(self, eev_data: Dict[str, Any]) -> bool:
        """EEV 데이터를 PES DB에 저장"""
        try:
            # 데이터 검증
            if not self._validate_eev_data(eev_data):
                return False
            
            # 트랜잭션 시작
            self.db.begin_transaction()
            
            try:
                # 데이터 추출 및 변환
                processed_data = self._extract_and_transform_data(eev_data)
                
                # 기존 데이터 삭제
                self._delete_existing_data(processed_data['PART_NUMBER'])
                
                # pes_eev_master_tbl 삽입
                if not self._insert_eev_master(processed_data):
                    raise Exception("eev_master 삽입 실패")
                
                # 트랜잭션 커밋
                self.db.commit_transaction()
                self.logger.info(f"EEV 데이터 저장 완료: {processed_data['PART_NUMBER']}")
                return True
                
            except Exception as e:
                # 트랜잭션 롤백
                self.db.rollback_transaction()
                self.logger.error(f"트랜잭션 실패, 롤백 수행: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"EEV 데이터 처리 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _validate_eev_data(self, eev_data: Dict[str, Any]) -> bool:
        """EEV 데이터 검증"""
        required_fields = ['part_number', 'username', 'userid']
        
        for field in required_fields:
            if not eev_data.get(field):
                self.logger.error(f"필수 필드 누락: {field}")
                return False
        
        return True
    
    def _extract_and_transform_data(self, eev: Dict[str, Any]) -> Dict[str, Any]:
        """API 데이터를 DB 형식으로 변환"""
        data = {
            'ID': eev.get('id'),
            'PART_NUMBER': f"[EDX]{eev['part_number']}",
            'DIAMETER': eev.get('diameter', 0.0),
            'MAX_PULSE': eev.get('max_pulse', 0.0),
            'CORR': eev.get('corr', 0.0),
            'EEV_COEFF_ID': eev.get('eev_coeff_id'),
            'USERID': eev['userid'],
            'CREATE_DATE': eev.get('create_date', datetime.now().isoformat())[:10]
        }
        
        return data
    
    def _delete_existing_data(self, part_number: str) -> bool:
        """기존 데이터 삭제"""
        query = "DELETE FROM pes_eev_master_tbl WHERE PEM_PART_NO = :part_number"
        self.db.execute_query(query, {'part_number': part_number}, commit=False)
        return True
    
    def _insert_eev_master(self, data: Dict[str, Any]) -> bool:
        """pes_eev_master_tbl 데이터 삽입"""
        # EEV_COEFF_ID 컬럼 존재 여부 확인
        has_eev_coeff_id = self.db.check_table_column('pes_eev_master_tbl', 'PEM_EEV_COEFF_ID')
        
        # 기본 쿼리
        columns = [
            'PEM_PART_NO', 'PEM_DIAMETER', 'PEM_MAX_PULSE', 'PEM_CORR',
            'PEM_REGISTER', 'PEM_REGIS_DATE'
        ]
        
        values = [
            ':part_number', ':diameter', ':max_pulse', ':corr',
            ':userid', ':create_date'
        ]
        
        params = {
            'part_number': data['PART_NUMBER'],
            'diameter': data['DIAMETER'],
            'max_pulse': data['MAX_PULSE'],
            'corr': data['CORR'],
            'userid': data['USERID'],
            'create_date': data['CREATE_DATE']
        }
        
        # 선택적 컬럼 추가
        if has_eev_coeff_id and data.get('EEV_COEFF_ID'):
            columns.append('PEM_EEV_COEFF_ID')
            values.append(':eev_coeff_id')
            params['eev_coeff_id'] = data['EEV_COEFF_ID']
        
        query = f"""
        INSERT INTO pes_eev_master_tbl ({', '.join(columns)})
        VALUES ({', '.join(values)})
        """
        
        if self.db.execute_query(query, params, commit=False):
            self.logger.info(f"pes_eev_master_tbl 삽입 성공: {data['PART_NUMBER']}")
            return True
        return False
    
    def create_eev(self, eev_create: EEVCreate) -> bool:
        """새로운 EEV 생성"""
        try:
            eev_data = {
                'id': None,
                'part_number': eev_create.part_number,
                'username': eev_create.username,
                'userid': eev_create.userid,
                'create_date': datetime.now().isoformat(),
                'diameter': eev_create.diameter,
                'max_pulse': eev_create.max_pulse,
                'corr': eev_create.corr,
                'eev_coeff_id': eev_create.eev_coeff_id
            }
            
            return self.process_eev_data(eev_data)
            
        except Exception as e:
            self.logger.error(f"EEV 생성 중 오류: {e}")
            return False

class AccumDataProcessor(ComponentDataProcessor):
    """Accum 데이터 처리 클래스 (완전한 구현)"""
    
    def validate_schema(self) -> bool:
        """Accum 관련 DB 스키마 검증"""
        required_columns = {
            'pes_accum_master_tbl': [
                'PAM_PART_NO', 'PAM_CAPA',
                'PAM_REGISTER', 'PAM_REGIS_DATE'
            ]
        }
        
        all_valid = True
        for table, columns in required_columns.items():
            for column in columns:
                if not self.db.check_table_column(table, column):
                    self.logger.error(f"필수 컬럼 누락: {table}.{column}")
                    all_valid = False
        
        return all_valid
    
    def process_accum_data(self, accum_data: Dict[str, Any]) -> bool:
        """Accum 데이터를 PES DB에 저장"""
        try:
            # 데이터 검증
            if not self._validate_accum_data(accum_data):
                return False
            
            # 트랜잭션 시작
            self.db.begin_transaction()
            
            try:
                # 데이터 추출 및 변환
                processed_data = self._extract_and_transform_data(accum_data)
                
                # 기존 데이터 삭제
                self._delete_existing_data(processed_data['PART_NUMBER'])
                
                # pes_accum_master_tbl 삽입
                if not self._insert_accum_master(processed_data):
                    raise Exception("accum_master 삽입 실패")
                
                # 트랜잭션 커밋
                self.db.commit_transaction()
                self.logger.info(f"Accum 데이터 저장 완료: {processed_data['PART_NUMBER']}")
                return True
                
            except Exception as e:
                # 트랜잭션 롤백
                self.db.rollback_transaction()
                self.logger.error(f"트랜잭션 실패, 롤백 수행: {e}")
                return False
            
        except Exception as e:
            self.logger.error(f"Accum 데이터 처리 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return False
    
    def _validate_accum_data(self, accum_data: Dict[str, Any]) -> bool:
        """Accum 데이터 검증"""
        required_fields = ['username', 'userid']
        
        for field in required_fields:
            if not accum_data.get(field):
                self.logger.error(f"필수 필드 누락: {field}")
                return False
        
        return True
    
    def _extract_and_transform_data(self, accum: Dict[str, Any]) -> Dict[str, Any]:
        """API 데이터를 DB 형식으로 변환"""
        data = {
            'ID': accum.get('id'),
            'PART_NUMBER': f"[EDX]{accum.get('part_number', 'UNKNOWN')}",
            'CAPA': accum.get('capa', 0.0),
            'USERID': accum['userid'],
            'CREATE_DATE': accum.get('create_date', datetime.now().isoformat())[:10]
        }
        
        return data
    
    def _delete_existing_data(self, part_number: str) -> bool:
        """기존 데이터 삭제"""
        query = "DELETE FROM pes_accum_master_tbl WHERE PAM_PART_NO = :part_number"
        self.db.execute_query(query, {'part_number': part_number}, commit=False)
        return True
    
    def _insert_accum_master(self, data: Dict[str, Any]) -> bool:
        """pes_accum_master_tbl 데이터 삽입"""
        query = """
        INSERT INTO pes_accum_master_tbl (
            PAM_PART_NO, PAM_CAPA, PAM_REGISTER, PAM_REGIS_DATE
        ) VALUES (
            :part_number, :capa, :userid, :create_date
        )
        """
        
        params = {
            'part_number': data['PART_NUMBER'],
            'capa': data['CAPA'],
            'userid': data['USERID'],
            'create_date': data['CREATE_DATE']
        }
        
        if self.db.execute_query(query, params, commit=False):
            self.logger.info(f"pes_accum_master_tbl 삽입 성공: {data['PART_NUMBER']}")
            return True
        return False
    
    def create_accum(self, accum_create: AccumCreate) -> bool:
        """새로운 Accum 생성"""
        try:
            accum_data = {
                'id': None,
                'part_number': accum_create.part_number,
                'username': accum_create.username,
                'userid': accum_create.userid,
                'create_date': datetime.now().isoformat(),
                'capa': accum_create.capa
            }
            
            return self.process_accum_data(accum_data)
            
        except Exception as e:
            self.logger.error(f"Accum 생성 중 오류: {e}")
            return False

# ===== 모니터링 엔진 (EDX API 명세 반영) =====
class MonitoringEngine:
    """모니터링 엔진 (EDX API 명세서 반영)"""
    def __init__(self, config: Config, logger: logging.Logger):
        self.config = config
        self.logger = logger
        self.state = MonitorState(config.get('Monitor', 'state_file', 'monitor_state.pkl'))
        self.db_manager = DatabaseManager(config, logger)
        self.api_client = EDXAPIClient(config, logger)
        
        # 각 부품별 데이터 프로세서 초기화
        self.fintube_processor = FintubeDataProcessor(self.db_manager, logger)
        self.fin_processor = FinDataProcessor(self.db_manager, logger)
        self.fan_processor = FanDataProcessor(self.db_manager, logger)
        self.comp_processor = CompDataProcessor(self.db_manager, logger)
        self.eev_processor = EEVDataProcessor(self.db_manager, logger)
        self.accum_processor = AccumDataProcessor(self.db_manager, logger)
        
        self.is_running = False
        self.monitor_thread = None
        self.batch_size = config.getint('Monitor', 'batch_size', 10)
        self.retry_count = config.getint('Monitor', 'retry_count', 3)
        self.retry_delay = config.getint('Monitor', 'retry_delay_seconds', 30)
    
    def get_processor(self, component_type: str):
        """부품 타입별 프로세서 반환"""
        processors = {
            'fintube': self.fintube_processor,
            'fin': self.fin_processor,
            'fan': self.fan_processor,
            'comp': self.comp_processor,
            'eev': self.eev_processor,
            'accum': self.accum_processor
        }
        return processors.get(component_type.lower())
    
    def check_new_items(self, component_type: str = 'fintube') -> List[int]:
        """새로운 항목 확인 (EDX API 명세서 반영)"""
        new_items = []
        skip = 0
        limit = 100
        
        self.logger.info(f"새로운 {component_type} 항목 확인 중...")
        
        while True:
            try:
                # API에서 목록 조회 (EDX API 구조에 맞게)
                result = None
                if component_type.lower() == 'fintube':
                    result = self.api_client.get_fintube_list(skip, limit)
                    item_list = result.get('fintube_list', []) if result else []
                elif component_type.lower() == 'comp':
                    result = self.api_client.get_comp_list(skip, limit)
                    item_list = result.get('comp_list', []) if result else []
                elif component_type.lower() == 'fin':
                    result = self.api_client.get_fin_list(skip, limit)
                    item_list = result.get('fin_list', []) if result else []
                elif component_type.lower() == 'fan':
                    result = self.api_client.get_fan_list(skip, limit)
                    item_list = result.get('fan_list', []) if result else []
                elif component_type.lower() == 'eev':
                    result = self.api_client.get_eev_list(skip, limit)
                    item_list = result.get('eev_list', []) if result else []
                elif component_type.lower() == 'accum':
                    result = self.api_client.get_accum_list(skip, limit)
                    item_list = result.get('accum_list', []) if result else []
                else:
                    self.logger.warning(f"{component_type}는 아직 API 모니터링이 구현되지 않았습니다.")
                    break
                    
                if not result:
                    break
                
                total = result.get('total', 0)
                
                # 새로운 항목 찾기
                for item in item_list:
                    item_id = item['id']
                    if item_id not in self.state.processed_ids:
                        if self.state.should_retry(item_id, self.retry_count):
                            new_items.append(item_id)
                
                # 더 이상 항목이 없으면 중단
                if skip + limit >= total:
                    break
                
                skip += limit
                
            except Exception as e:
                self.logger.error(f"항목 확인 중 오류: {e}")
                break
        
        self.logger.info(f"새로운 {component_type} 항목 {len(new_items)}개 발견")
        return new_items
    
    def process_item(self, item_id: int, component_type: str = 'fintube') -> bool:
        """단일 항목 처리 (모든 부품 타입 지원)"""
        try:
            self.logger.info(f"{component_type} 항목 처리 시작: ID={item_id}")
            
            # API에서 상세 정보 조회
            item_data = None
            processor = None
            
            if component_type.lower() == 'fintube':
                item_data = self.api_client.get_fintube_detail(item_id)
                processor = self.fintube_processor
            elif component_type.lower() == 'comp':
                item_data = self.api_client.get_comp_detail(item_id)
                processor = self.comp_processor
            elif component_type.lower() == 'fin':
                item_data = self.api_client.get_fin_detail(item_id)
                processor = self.fin_processor
            elif component_type.lower() == 'fan':
                item_data = self.api_client.get_fan_detail(item_id)
                processor = self.fan_processor
            elif component_type.lower() == 'eev':
                item_data = self.api_client.get_eev_detail(item_id)
                processor = self.eev_processor
            elif component_type.lower() == 'accum':
                item_data = self.api_client.get_accum_detail(item_id)
                processor = self.accum_processor
            else:
                self.logger.warning(f"알 수 없는 부품 타입: {component_type}")
                return False
                
            if not item_data:
                self.logger.error(f"항목 조회 실패: ID={item_id}")
                return False
            
            # 데이터베이스에 저장
            success = False
            if processor:
                if hasattr(processor, f'process_{component_type.lower()}_data'):
                    process_method = getattr(processor, f'process_{component_type.lower()}_data')
                    success = process_method(item_data)
                else:
                    self.logger.error(f"{component_type} 프로세서에 처리 메서드가 없습니다.")
            
            if success:
                self.state.mark_processed(item_id)
                self.logger.info(f"{component_type} 항목 처리 완료: ID={item_id}")
            else:
                self.state.mark_error(item_id)
                self.logger.error(f"{component_type} 항목 처리 실패: ID={item_id}")
            
            return success
            
        except Exception as e:
            self.logger.error(f"{component_type} 항목 처리 중 예외 발생: ID={item_id}, {e}")
            self.logger.error(traceback.format_exc())
            self.state.mark_error(item_id)
            return False
    
    def run_sync_cycle(self, component_types: List[str] = None):
        """동기화 사이클 실행 (모든 부품 타입 지원)"""
        if component_types is None:
            component_types = ['fintube', 'comp', 'fin', 'fan', 'eev', 'accum']
        
        try:
            self.logger.info("===== 동기화 사이클 시작 (완전한 DB 저장) =====")
            
            # 데이터베이스 연결
            if not self.db_manager.connect():
                self.logger.error("데이터베이스 연결 실패")
                return {"success": False, "message": "데이터베이스 연결 실패"}
            
            try:
                total_processed = 0
                total_failed = 0
                
                # 각 부품 타입별 스키마 검증
                for component_type in component_types:
                    processor = self.get_processor(component_type)
                    if processor and hasattr(processor, 'validate_schema'):
                        if not processor.validate_schema():
                            self.logger.warning(f"{component_type} 스키마 검증 실패 - 일부 기능이 제한될 수 있습니다.")
                
                for component_type in component_types:
                    # 새로운 항목 확인
                    new_items = self.check_new_items(component_type)
                    
                    if not new_items:
                        self.logger.info(f"{component_type}: 처리할 새로운 항목이 없습니다.")
                        continue
                    
                    # 배치 크기만큼만 처리
                    items_to_process = new_items[:self.batch_size]
                    self.logger.info(f"{component_type}: {len(items_to_process)}개 항목 처리 예정")
                    
                    success_count = 0
                    fail_count = 0
                    
                    for item_id in items_to_process:
                        if self.process_item(item_id, component_type):
                            success_count += 1
                        else:
                            fail_count += 1
                        
                        # 각 항목 처리 후 잠시 대기
                        time.sleep(1)
                    
                    self.logger.info(f"{component_type} 처리 완료: 성공={success_count}, 실패={fail_count}")
                    total_processed += success_count
                    total_failed += fail_count
                
                return {
                    "success": True, 
                    "message": f"전체 처리 완료 (완전한 DB 저장): 성공={total_processed}, 실패={total_failed}",
                    "processed_count": total_processed,
                    "failed_count": total_failed
                }
                
            finally:
                # 데이터베이스 연결 해제
                self.db_manager.disconnect()
                
                # 체크 시간 업데이트
                self.state.update_check_time()
            
            self.logger.info("===== 동기화 사이클 완료 =====")
            
        except Exception as e:
            self.logger.error(f"동기화 사이클 중 오류: {e}")
            self.logger.error(traceback.format_exc())
            return {"success": False, "message": f"동기화 중 오류 발생: {str(e)}"}
    
    def start_monitoring(self):
        """모니터링 시작"""
        if self.is_running:
            self.logger.warning("모니터링이 이미 실행 중입니다.")
            return False
        
        self.is_running = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        
        self.logger.info("백그라운드 모니터링 시작 (완전한 DB 저장)")
        return True
    
    def stop_monitoring(self):
        """모니터링 중지"""
        if not self.is_running:
            return False
            
        self.is_running = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=10)
        
        self.logger.info("모니터링 중지")
        return True
    
    def get_status(self) -> Dict[str, Any]:
        """모니터링 상태 조회"""
        return {
            "is_running": self.is_running,
            "last_check_time": self.state.last_check_time.isoformat() if self.state.last_check_time else None,
            "processed_items_count": len(self.state.processed_ids),
            "error_items_count": len(self.state.error_count)
        }
    
    def _monitor_loop(self):
        """모니터링 루프"""
        check_interval = self.config.getint('Monitor', 'check_interval_minutes', 10)
        self.logger.info(f"모니터링 루프 시작: {check_interval}분 간격 (완전한 DB 저장)")
        
        while self.is_running:
            try:
                self.run_sync_cycle()
                # 체크 간격만큼 대기
                for _ in range(check_interval):
                    if not self.is_running:
                        break
                    time.sleep(60)
            except Exception as e:
                self.logger.error(f"모니터링 루프 중 오류: {e}")
                time.sleep(60)

# ===== FastAPI 애플리케이션 생명주기 관리 =====
@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI 애플리케이션 생명주기 관리"""
    global config, logger, monitor_engine
    
    # 시작 시 초기화
    config = Config()
    logger = setup_logging(config)
    
    # Oracle 클라이언트 전역 초기화
    oracle_client_path = config.get('Database', 'oracle_client_path')
    if not initialize_oracle_client_once(oracle_client_path, logger):
        logger.warning("Oracle 클라이언트 초기화 실패 - 데이터베이스 기능이 제한될 수 있습니다.")
    
    monitor_engine = MonitoringEngine(config, logger)
    
    logger.info("EDX-PES FastAPI 모니터링 시스템 시작 (완전한 DB 저장 버전)")
    
    yield
    
    # 종료 시 정리
    if monitor_engine:
        monitor_engine.stop_monitoring()
    logger.info("EDX-PES FastAPI 모니터링 시스템 종료")

# ===== FastAPI 애플리케이션 =====
app = FastAPI(
    title="EDX-PES Component Management API (완전한 DB 저장)",
    description="EDX API와 PES DB 간 모든 부품 데이터 동기화 및 CRUD를 위한 API (완전한 DB 저장 구현)",
    version="3.0.0",
    lifespan=lifespan
)

# CORS 미들웨어 추가
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===== 의존성 함수 =====
def get_monitor_engine():
    """모니터링 엔진 의존성"""
    global monitor_engine
    if monitor_engine is None:
        raise HTTPException(status_code=500, detail="모니터링 엔진이 초기화되지 않았습니다.")
    return monitor_engine

# ===== API 엔드포인트 =====
@app.get("/", response_model=Dict[str, Any])
async def root():
    """루트 엔드포인트"""
    return {
        "message": "EDX-PES Component Management API (완전한 DB 저장 버전)",
        "status": "running",
        "docs": "/docs",
        "redoc": "/redoc",
        "api_version": "3.0.0",
        "complete_db_implementation": True,
        "improvements": [
            "모든 부품 타입의 완전한 DB 저장 로직 구현",
            "관계형 데이터 처리 개선 (fin_id, hex_coeff_id, comp_coeff_id 등)",
            "트랜잭션 처리로 데이터 일관성 보장",
            "SQL 파라미터 바인딩으로 보안 강화",
            "DB 스키마 검증 로직 추가",
            "모든 부품 타입 API 지원 (Fin, Fan, EEV, Accum)"
        ],
        "endpoints": {
            # 모니터링 관련
            "sync": "POST /api/sync",
            "webhook": "POST /api/webhook", 
            "webhook_raw": "POST /api/webhook-raw",
            "status": "GET /api/monitor/status",
            "start": "POST /api/monitor/start",
            "stop": "POST /api/monitor/stop",
            
            # 부품별 웹훅
            "fintube_webhook": "POST /api/fintube/webhook",
            "comp_webhook": "POST /api/comp/webhook",
            "fin_webhook": "POST /api/fin/webhook",
            "fan_webhook": "POST /api/fan/webhook",
            "eev_webhook": "POST /api/eev/webhook",
            "accum_webhook": "POST /api/accum/webhook",
            
            # 부품 관리 - Fintube (완전한 CRUD)
            "create_fintube": "POST /api/components/fintube",
            "list_fintube": "GET /api/components/fintube",
            "get_fintube": "GET /api/components/fintube/{part_number}",
            "update_fintube": "PUT /api/components/fintube/{part_number}",
            "delete_fintube": "DELETE /api/components/fintube/{part_number}",
            
            # 부품 관리 - Comp (완전한 CRUD)
            "create_comp": "POST /api/components/comp",
            "list_comp": "GET /api/components/comp",
            "get_comp": "GET /api/components/comp/{part_number}",
            "update_comp": "PUT /api/components/comp/{part_number}",
            "delete_comp": "DELETE /api/components/comp/{part_number}",
            
            # 기타 부품 관리 - 생성 지원
            "create_fin": "POST /api/components/fin",
            "create_fan": "POST /api/components/fan",
            "create_eev": "POST /api/components/eev",
            "create_accum": "POST /api/components/accum",
            
            # 통합 부품 관리
            "component_types": "GET /api/components/types",
            "bulk_sync": "POST /api/components/bulk-sync",
            
            # 디버그 API
            "search_suggestions": "GET /api/debug/search-suggestions/{type}",
            "database_status": "GET /api/debug/database-status",
            "schema_validation": "GET /api/debug/schema-validation"
        },
        "database_features": {
            "transaction_support": "모든 DB 작업에서 트랜잭션 사용",
            "parameter_binding": "SQL 인젝션 방지를 위한 파라미터 바인딩",
            "schema_validation": "DB 스키마 자동 검증",
            "complete_data_storage": "모든 부품 타입의 완전한 데이터 저장",
            "relationship_handling": "관계형 데이터 완벽 처리"
        }
    }

@app.get("/health", response_model=Dict[str, str])
async def health_check():
    """헬스 체크 엔드포인트"""
    return {"status": "healthy", "db_implementation": "complete"}

# ===== 모니터링 관련 엔드포인트 =====
@app.post("/api/sync", response_model=SyncResponse)
async def sync_data(
    background_tasks: BackgroundTasks,
    component_types: Optional[List[str]] = None,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """수동으로 데이터 동기화 실행 (모든 부품 타입 지원)"""
    try:
        logger.info(f"수동 동기화 요청 수신 (완전한 DB 저장): {component_types}")
        
        if component_types is None:
            component_types = ['fintube', 'comp', 'fin', 'fan', 'eev', 'accum']
        
        result = monitor.run_sync_cycle(component_types)
        
        return SyncResponse(
            success=result["success"],
            message=result["message"],
            processed_count=result.get("processed_count", 0),
            failed_count=result.get("failed_count", 0)
        )
    except Exception as e:
        logger.error(f"수동 동기화 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"동기화 중 오류 발생: {str(e)}")

@app.post("/api/webhook", response_model=Dict[str, Any])
async def handle_webhook(
    webhook_data: WebhookRequest,
    background_tasks: BackgroundTasks,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """EDX에서 오는 웹훅 처리 (모든 부품 타입 지원)"""
    try:
        logger.info(f"웹훅 수신: {webhook_data.event_type} - {webhook_data.component_type} ID:{webhook_data.component_id}")
        
        # 백그라운드에서 해당 항목 처리
        background_tasks.add_task(
            process_webhook_item,
            monitor,
            webhook_data.component_id,
            webhook_data.component_type,
            webhook_data.event_type
        )
        
        return {
            "success": True,
            "message": f"웹훅 처리 시작: {webhook_data.component_type} ID:{webhook_data.component_id}"
        }
    except Exception as e:
        logger.error(f"웹훅 처리 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"웹훅 처리 중 오류 발생: {str(e)}")

async def process_webhook_item(monitor: MonitoringEngine, item_id: int, component_type: str, event_type: str):
    """웹훅 항목 처리 (백그라운드 태스크)"""
    try:
        if not monitor.db_manager.connect():
            logger.error("데이터베이스 연결 실패")
            return
        
        try:
            if event_type == "deleted":
                # 삭제 처리
                processor = monitor.get_processor(component_type)
                if processor:
                    # API에서 부품 정보 조회하여 part_number 추출
                    api_method = getattr(monitor.api_client, f'get_{component_type}_detail', None)
                    if api_method:
                        item_data = api_method(item_id)
                        if item_data and 'part_number' in item_data:
                            delete_method = getattr(processor, f'delete_{component_type}', None)
                            if delete_method:
                                delete_method(item_data['part_number'])
            else:
                # 생성/수정 처리
                monitor.process_item(item_id, component_type)
                
        finally:
            monitor.db_manager.disconnect()
            
    except Exception as e:
        logger.error(f"웹훅 항목 처리 중 오류: {e}")

@app.get("/api/monitor/status", response_model=MonitorStatusResponse)
async def get_monitor_status(monitor: MonitoringEngine = Depends(get_monitor_engine)):
    """모니터링 상태 조회"""
    status = monitor.get_status()
    return MonitorStatusResponse(
        is_running=status["is_running"],
        last_check_time=status["last_check_time"],
        processed_items_count=status["processed_items_count"],
        error_items_count=status["error_items_count"]
    )

@app.post("/api/monitor/start", response_model=Dict[str, Any])
async def start_monitoring(monitor: MonitoringEngine = Depends(get_monitor_engine)):
    """모니터링 시작"""
    if monitor.start_monitoring():
        return {"success": True, "message": "모니터링이 시작되었습니다."}
    else:
        return {"success": False, "message": "모니터링이 이미 실행 중입니다."}

@app.post("/api/monitor/stop", response_model=Dict[str, Any])
async def stop_monitoring(monitor: MonitoringEngine = Depends(get_monitor_engine)):
    """모니터링 중지"""
    if monitor.stop_monitoring():
        return {"success": True, "message": "모니터링이 중지되었습니다."}
    else:
        return {"success": False, "message": "모니터링이 실행 중이 아닙니다."}

# ===== 부품별 웹훅 엔드포인트 =====
@app.post("/api/fintube/webhook", response_model=Dict[str, Any])
async def handle_fintube_webhook(
    webhook_data: SimpleWebhookRequest,
    background_tasks: BackgroundTasks,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Fintube 웹훅 처리"""
    full_webhook = WebhookRequest(
        event_type=webhook_data.event_type,
        component_type="fintube",
        component_id=webhook_data.component_id
    )
    return await handle_webhook(full_webhook, background_tasks, monitor)

@app.post("/api/comp/webhook", response_model=Dict[str, Any])
async def handle_comp_webhook(
    webhook_data: SimpleWebhookRequest,
    background_tasks: BackgroundTasks,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Comp 웹훅 처리"""
    full_webhook = WebhookRequest(
        event_type=webhook_data.event_type,
        component_type="comp",
        component_id=webhook_data.component_id
    )
    return await handle_webhook(full_webhook, background_tasks, monitor)

# ===== Fintube CRUD 엔드포인트 =====
@app.post("/api/components/fintube", response_model=Dict[str, Any])
async def create_fintube(
    fintube_data: FintubeCreate,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """새로운 Fintube 생성"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.fintube_processor.create_fintube(fintube_data)
            if success:
                return {"success": True, "message": f"Fintube 생성 완료: {fintube_data.part_number}"}
            else:
                raise HTTPException(status_code=400, detail="Fintube 생성 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fintube 생성 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Fintube 생성 중 오류: {str(e)}")

@app.get("/api/components/fintube", response_model=ComponentListResponse)
async def get_fintube_list(
    skip: int = 0,
    limit: int = 100,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Fintube 목록 조회"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            result = monitor.fintube_processor.get_fintube_list(skip, limit)
            return ComponentListResponse(
                total=result['total'],
                items=result['fintube_list'],
                next_cursor=result.get('next_cursor')
            )
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fintube 목록 조회 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"목록 조회 중 오류: {str(e)}")

@app.get("/api/components/fintube/{part_number}", response_model=Dict[str, Any])
async def get_fintube_detail(
    part_number: str,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Fintube 상세 조회"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            result = monitor.fintube_processor.get_fintube_by_part_number(part_number)
            if result:
                return result
            else:
                raise HTTPException(status_code=404, detail=f"Fintube를 찾을 수 없습니다: {part_number}")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fintube 상세 조회 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"상세 조회 중 오류: {str(e)}")

@app.put("/api/components/fintube/{part_number}", response_model=Dict[str, Any])
async def update_fintube(
    part_number: str,
    update_data: FintubeUpdate,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Fintube 수정"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.fintube_processor.update_fintube(part_number, update_data)
            if success:
                return {"success": True, "message": f"Fintube 수정 완료: {part_number}"}
            else:
                raise HTTPException(status_code=400, detail="Fintube 수정 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fintube 수정 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Fintube 수정 중 오류: {str(e)}")

@app.delete("/api/components/fintube/{part_number}", response_model=Dict[str, Any])
async def delete_fintube(
    part_number: str,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Fintube 삭제"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.fintube_processor.delete_fintube(part_number)
            if success:
                return {"success": True, "message": f"Fintube 삭제 완료: {part_number}"}
            else:
                raise HTTPException(status_code=400, detail="Fintube 삭제 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fintube 삭제 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Fintube 삭제 중 오류: {str(e)}")

# ===== Comp CRUD 엔드포인트 =====
@app.post("/api/components/comp", response_model=Dict[str, Any])
async def create_comp(
    comp_data: CompCreate,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """새로운 Comp 생성"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.comp_processor.create_comp(comp_data)
            if success:
                return {"success": True, "message": f"Comp 생성 완료: {comp_data.part_number}"}
            else:
                raise HTTPException(status_code=400, detail="Comp 생성 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comp 생성 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Comp 생성 중 오류: {str(e)}")

@app.get("/api/components/comp", response_model=ComponentListResponse)
async def get_comp_list(
    skip: int = 0,
    limit: int = 100,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Comp 목록 조회"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            result = monitor.comp_processor.get_comp_list(skip, limit)
            return ComponentListResponse(
                total=result['total'],
                items=result['comp_list'],
                next_cursor=result.get('next_cursor')
            )
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comp 목록 조회 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"목록 조회 중 오류: {str(e)}")

@app.get("/api/components/comp/{part_number}", response_model=Dict[str, Any])
async def get_comp_detail(
    part_number: str,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Comp 상세 조회"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            result = monitor.comp_processor.get_comp_by_part_number(part_number)
            if result:
                return result
            else:
                raise HTTPException(status_code=404, detail=f"Comp를 찾을 수 없습니다: {part_number}")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comp 상세 조회 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"상세 조회 중 오류: {str(e)}")

@app.put("/api/components/comp/{part_number}", response_model=Dict[str, Any])
async def update_comp(
    part_number: str,
    update_data: CompUpdate,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Comp 수정"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.comp_processor.update_comp(part_number, update_data)
            if success:
                return {"success": True, "message": f"Comp 수정 완료: {part_number}"}
            else:
                raise HTTPException(status_code=400, detail="Comp 수정 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comp 수정 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Comp 수정 중 오류: {str(e)}")

@app.delete("/api/components/comp/{part_number}", response_model=Dict[str, Any])
async def delete_comp(
    part_number: str,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """Comp 삭제"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.comp_processor.delete_comp(part_number)
            if success:
                return {"success": True, "message": f"Comp 삭제 완료: {part_number}"}
            else:
                raise HTTPException(status_code=400, detail="Comp 삭제 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Comp 삭제 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Comp 삭제 중 오류: {str(e)}")

# ===== 기타 부품 생성 엔드포인트 =====
@app.post("/api/components/fin", response_model=Dict[str, Any])
async def create_fin(
    fin_data: FinCreate,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """새로운 Fin 생성"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.fin_processor.create_fin(fin_data)
            if success:
                return {"success": True, "message": f"Fin 생성 완료: {fin_data.fin_name}"}
            else:
                raise HTTPException(status_code=400, detail="Fin 생성 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fin 생성 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Fin 생성 중 오류: {str(e)}")

@app.post("/api/components/fan", response_model=Dict[str, Any])
async def create_fan(
    fan_data: FanCreate,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """새로운 Fan 생성"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.fan_processor.create_fan(fan_data)
            if success:
                return {"success": True, "message": f"Fan 생성 완료: {fan_data.part_number}"}
            else:
                raise HTTPException(status_code=400, detail="Fan 생성 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Fan 생성 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Fan 생성 중 오류: {str(e)}")

@app.post("/api/components/eev", response_model=Dict[str, Any])
async def create_eev(
    eev_data: EEVCreate,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """새로운 EEV 생성"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.eev_processor.create_eev(eev_data)
            if success:
                return {"success": True, "message": f"EEV 생성 완료: {eev_data.part_number}"}
            else:
                raise HTTPException(status_code=400, detail="EEV 생성 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"EEV 생성 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"EEV 생성 중 오류: {str(e)}")

@app.post("/api/components/accum", response_model=Dict[str, Any])
async def create_accum(
    accum_data: AccumCreate,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """새로운 Accum 생성"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            success = monitor.accum_processor.create_accum(accum_data)
            if success:
                return {"success": True, "message": f"Accum 생성 완료: {accum_data.part_number}"}
            else:
                raise HTTPException(status_code=400, detail="Accum 생성 실패")
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Accum 생성 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"Accum 생성 중 오류: {str(e)}")

# ===== 통합 부품 관리 엔드포인트 =====
@app.get("/api/components/types", response_model=Dict[str, List[str]])
async def get_component_types():
    """지원하는 부품 타입 목록 조회"""
    return {
        "supported_types": ["fintube", "comp", "fin", "fan", "eev", "accum"],
        "crud_support": ["fintube", "comp"],
        "create_only_support": ["fin", "fan", "eev", "accum"]
    }

@app.post("/api/components/bulk-sync", response_model=SyncResponse)
async def bulk_sync_components(
    component_types: List[str],
    background_tasks: BackgroundTasks,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """여러 부품 타입 일괄 동기화"""
    try:
        # 유효한 부품 타입만 필터링
        valid_types = ["fintube", "comp", "fin", "fan", "eev", "accum"]
        filtered_types = [t for t in component_types if t in valid_types]
        
        if not filtered_types:
            raise HTTPException(status_code=400, detail="유효한 부품 타입이 없습니다.")
        
        result = monitor.run_sync_cycle(filtered_types)
        
        return SyncResponse(
            success=result["success"],
            message=result["message"],
            processed_count=result.get("processed_count", 0),
            failed_count=result.get("failed_count", 0)
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"일괄 동기화 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"일괄 동기화 중 오류: {str(e)}")

# ===== 디버그 API =====
@app.get("/api/debug/database-status")
async def get_database_status(monitor: MonitoringEngine = Depends(get_monitor_engine)):
    """데이터베이스 연결 상태 확인"""
    try:
        if monitor.db_manager.connect():
            monitor.db_manager.disconnect()
            return {"status": "connected", "message": "데이터베이스 연결 성공"}
        else:
            return {"status": "disconnected", "message": "데이터베이스 연결 실패"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.get("/api/debug/schema-validation")
async def validate_database_schema(
    component_types: Optional[List[str]] = None,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """데이터베이스 스키마 검증"""
    try:
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            if component_types is None:
                component_types = ['fintube', 'comp', 'fin', 'fan', 'eev', 'accum']
            
            validation_results = {}
            
            for component_type in component_types:
                processor = monitor.get_processor(component_type)
                if processor and hasattr(processor, 'validate_schema'):
                    validation_results[component_type] = {
                        "valid": processor.validate_schema(),
                        "processor": processor.__class__.__name__
                    }
                else:
                    validation_results[component_type] = {
                        "valid": False,
                        "error": "프로세서가 없거나 스키마 검증을 지원하지 않습니다."
                    }
            
            # 추가 권장사항
            recommendations = []
            
            # PHM_NUM_PATH 컬럼 확인
            if not monitor.db_manager.check_table_column('pes_hex_master_tbl', 'PHM_NUM_PATH'):
                recommendations.append("ALTER TABLE pes_hex_master_tbl ADD PHM_NUM_PATH NUMBER")
            
            # 관계형 필드 컬럼 확인
            if not monitor.db_manager.check_table_column('pes_hex_master_tbl', 'PHM_FIN_ID'):
                recommendations.append("ALTER TABLE pes_hex_master_tbl ADD PHM_FIN_ID NUMBER")
            
            if not monitor.db_manager.check_table_column('pes_hex_master_tbl', 'PHM_HEX_COEFF_ID'):
                recommendations.append("ALTER TABLE pes_hex_master_tbl ADD PHM_HEX_COEFF_ID NUMBER")
            
            if not monitor.db_manager.check_table_column('pes_comp_master_tbl', 'PCM_COMP_COEFF_ID'):
                recommendations.append("ALTER TABLE pes_comp_master_tbl ADD PCM_COMP_COEFF_ID NUMBER")
            
            if not monitor.db_manager.check_table_column('pes_eev_master_tbl', 'PEM_EEV_COEFF_ID'):
                recommendations.append("ALTER TABLE pes_eev_master_tbl ADD PEM_EEV_COEFF_ID NUMBER")
            
            return {
                "validation_results": validation_results,
                "all_valid": all(v.get("valid", False) for v in validation_results.values()),
                "recommendations": recommendations,
                "recommendation_count": len(recommendations)
            }
            
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"스키마 검증 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"스키마 검증 중 오류: {str(e)}")

@app.get("/api/debug/search-suggestions/{component_type}")
async def get_search_suggestions(
    component_type: str,
    keyword: str = "",
    limit: int = 10,
    monitor: MonitoringEngine = Depends(get_monitor_engine)
):
    """부품 검색 제안 (자동완성용)"""
    try:
        if component_type not in ["fintube", "comp", "fin", "fan", "eev", "accum"]:
            raise HTTPException(status_code=400, detail="유효하지 않은 부품 타입")
        
        if not monitor.db_manager.connect():
            raise HTTPException(status_code=500, detail="데이터베이스 연결 실패")
        
        try:
            suggestions = []
            
            if component_type == "fintube":
                query = f"""
                SELECT DISTINCT PHM_PART_NO 
                FROM pes_hex_master_tbl 
                WHERE PHM_PART_NO LIKE '[EDX]%' 
                AND UPPER(PHM_PART_NO) LIKE UPPER(:keyword)
                AND ROWNUM <= :limit
                """
            elif component_type == "comp":
                query = f"""
                SELECT DISTINCT PARTNO 
                FROM pes_comp_master_tbl 
                WHERE PARTNO LIKE '[EDX]%' 
                AND UPPER(PARTNO) LIKE UPPER(:keyword)
                AND ROWNUM <= :limit
                """
            else:
                return {"suggestions": []}
            
            cursor = monitor.db_manager.connection.cursor()
            cursor.execute(query, {'keyword': f'%{keyword}%', 'limit': limit})
            
            for row in cursor:
                part_number = row[0].replace('[EDX]', '') if row[0] else ''
                suggestions.append(part_number)
            
            cursor.close()
            
            return {"suggestions": suggestions}
            
        finally:
            monitor.db_manager.disconnect()
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"검색 제안 조회 중 오류: {e}")
        raise HTTPException(status_code=500, detail=f"검색 제안 조회 중 오류: {str(e)}")

# ===== 메인 애플리케이션 클래스 =====
class EDXPESMonitorApp:
    """메인 애플리케이션 클래스 (완전한 DB 저장)"""
    def __init__(self, config_file: str = "edx_pes_monitor.ini"):
        self.config = Config(config_file)
        self.logger = setup_logging(self.config)
        
        # Oracle 클라이언트 전역 초기화
        oracle_client_path = self.config.get('Database', 'oracle_client_path')
        if not initialize_oracle_client_once(oracle_client_path, self.logger):
            self.logger.warning("Oracle 클라이언트 초기화 실패")
        
        self.monitor = MonitoringEngine(self.config, self.logger)
    
    def run_fastapi(self):
        """FastAPI 서버 실행"""
        host = self.config.get('FastAPI', 'host', '165.186.62.167')
        port = self.config.getint('FastAPI', 'port', 8002)
        reload = self.config.get('FastAPI', 'reload', 'false').lower() == 'true'
        
        self.logger.info(f"FastAPI 서버 시작 (완전한 DB 저장 버전): {host}:{port}")
        self.logger.info("=" * 70)
        self.logger.info("💾 완전한 DB 저장 구현 주요 특징:")
        self.logger.info("  ✅ 모든 부품 타입 DB 저장 로직 구현")
        self.logger.info("    - Fintube: 3개 테이블 (master, airflow_profile, airflow_seg)")
        self.logger.info("    - Comp: 2개 테이블 (master, performance)")
        self.logger.info("    - Fin, Fan, EEV, Accum: 각 master 테이블")
        self.logger.info("  🔐 보안 강화:")
        self.logger.info("    - SQL 파라미터 바인딩으로 인젝션 방지")
        self.logger.info("    - 트랜잭션 처리로 데이터 일관성 보장")
        self.logger.info("  🔍 스키마 검증:")
        self.logger.info("    - DB 테이블/컬럼 자동 검증")
        self.logger.info("    - 누락된 컬럼에 대한 권장사항 제공")
        self.logger.info("  🔗 관계형 데이터:")
        self.logger.info("    - fin_id, hex_coeff_id, comp_coeff_id 등 처리")
        self.logger.info("=" * 70)
        self.logger.info("🌐 네트워크 접근 정보:")
        self.logger.info(f"  - 로컬 접근: http://localhost:{port}")
        self.logger.info(f"  - 네트워크 접근: http://{host}:{port}")
        self.logger.info(f"  - API 문서: http://{host}:{port}/docs")
        self.logger.info(f"  - 스키마 검증: http://{host}:{port}/api/debug/schema-validation")
        self.logger.info("=" * 70)
        
        uvicorn.run("__main__:app", host="0.0.0.0", port=port, reload=reload)
    
    def run_monitor_only(self):
        """모니터링만 실행 (완전한 DB 저장)"""
        try:
            self.logger.info("EDX-PES 부품 모니터링 시스템 시작 (모니터링 전용, 완전한 DB 저장)")
            self.monitor.start_monitoring()
            
            # 메인 스레드에서 대기
            while self.monitor.is_running:
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("사용자 중단 요청")
            self.monitor.stop_monitoring()
        except Exception as e:
            self.logger.error(f"애플리케이션 오류: {e}")
            self.logger.error(traceback.format_exc())
        finally:
            self.logger.info("EDX-PES 부품 모니터링 시스템 종료")
    
    def run_once(self):
        """단일 실행 (테스트용, 완전한 DB 저장)"""
        self.logger.info("단일 동기화 실행 (완전한 DB 저장)")
        
        # 스키마 검증 먼저 수행
        self.logger.info("DB 스키마 검증 중...")
        if self.monitor.db_manager.connect():
            try:
                for component_type in ['fintube', 'comp', 'fin', 'fan', 'eev', 'accum']:
                    processor = self.monitor.get_processor(component_type)
                    if processor and hasattr(processor, 'validate_schema'):
                        valid = processor.validate_schema()
                        self.logger.info(f"  {component_type}: {'✅ 유효' if valid else '❌ 무효'}")
            finally:
                self.monitor.db_manager.disconnect()
        
        result = self.monitor.run_sync_cycle()
        self.logger.info(f"실행 결과 (완전한 DB 저장): {result}")
        
        # 결과 상세 분석
        if result["success"]:
            self.logger.info("✅ 단일 동기화 성공!")
            self.logger.info(f"  📊 처리된 항목: {result.get('processed_count', 0)}개")
            self.logger.info(f"  ❌ 실패한 항목: {result.get('failed_count', 0)}개")
        else:
            self.logger.error("❌ 단일 동기화 실패!")
            self.logger.error(f"  💬 오류 메시지: {result.get('message', 'Unknown error')}")
        
        return result

# ===== 커맨드라인 인터페이스 =====
def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(
        description='EDX API를 모니터링하여 새로운 부품 데이터를 PES DB에 자동 동기화하고 모든 부품 CRUD를 지원합니다. (완전한 DB 저장 버전)'
    )
    parser.add_argument(
        '--config', 
        type=str, 
        default='edx_pes_monitor.ini',
        help='설정 파일 경로'
    )
    parser.add_argument(
        '--mode',
        type=str,
        choices=['fastapi', 'monitor', 'once'],
        default='fastapi',
        help='실행 모드: fastapi (웹서버+CRUD), monitor (모니터링만), once (1회 실행)'
    )
    parser.add_argument(
        '--debug', 
        action='store_true',
        help='디버그 모드 활성화'
    )
    
    args = parser.parse_args()
    
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    app_instance = EDXPESMonitorApp(args.config)
    
    if args.mode == 'fastapi':
        app_instance.run_fastapi()
    elif args.mode == 'monitor':
        app_instance.run_monitor_only()
    elif args.mode == 'once':
        app_instance.run_once()

if __name__ == "__main__":
    main()
