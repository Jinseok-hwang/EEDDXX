# enhanced_pes_launcher_concurrent.py - 개선된 동시 처리 지원 버전

import sys
import os
import platform
print("--- Python 환경 진단 정보 ---")
print(f"실행 파일 경로: {sys.executable}")
print(f"파이썬 버전: {sys.version}")
print(f"아키텍처: {platform.architecture()[0]}")
print("---------------------------------\n")

import subprocess
import time
import os
import psutil
import win32gui
import win32con
import win32api
import win32ui
import win32print
import logging
import sys
import threading
import argparse
import configparser
from datetime import datetime
import uuid
import pandas as pd
import numpy as np
from PIL import ImageGrab
import re
import traceback
import ctypes
from ctypes import wintypes
import queue
import json
import multiprocessing
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Any
import weakref

# 설정 파일 경로
CONFIG_FILE = "enhanced_pes_launcher_concurrent_config.ini"

# 전역 변수 선언
TERMINATE_ALL = False
simulation_results = []
results_file_path = "pes_results.xlsx"
window_monitor_queue = queue.Queue()
error_detection_enabled = True

# 동기화 객체
window_handles_lock = threading.RLock()  # 재진입 가능한 락
process_lock = threading.RLock()
results_lock = threading.RLock()
used_handles_lock = threading.RLock()

# 활성 인스턴스 추적
active_instances = {}  # {unique_id: PESInstance}
active_windows = {}    # {hwnd: unique_id}
used_handles = set()   # 전역 사용 중인 핸들 세트

# 기본 설정
DEFAULT_CONFIG = {
    "Paths": {
        "PES_EXECUTABLE_PATH": "C:\\PES2\\PES.exe",
        "PES_WORKING_DIR": "C:\\PES2"
    },
    "Credentials": {
        "PES_USERNAME": "EDXSERVER",
        "PES_PASSWORD": "EDXSERVER"
    },
    "Settings": {
        "PES_PROCESS_NAME": "PES.exe",
        "LOG_FILE": "enhanced_pes_launcher_concurrent.log",
        "INSTANCE_COUNT": "30",
        "LOGIN_DELAY": "1",
        "POST_LOGIN_DELAY": "2",
        "WINDOW_STABLE_DELAY": "2",
        "SIMULATION_MONITOR_INTERVAL": "1",
        "WINDOW_CHANGE_CHECK_TIMEOUT": "120",
        "FORCE_QUIT_AFTER_SIMULATION": "True",
        "RESULT_WAIT_TIME": "3",
        "RESULTS_EXCEL_FILE": "enhanced_pes_launcher_concurrent_results.xlsx",
        "ASK_INSTANCE_COUNT": "True",
        "ERROR_DETECTION_TIMEOUT": "10",
        "MAX_RETRY_ATTEMPTS": "3",
        "WINDOW_ARRANGEMENT": "AUTO",
        "MONITOR_INDEX": "0",
        "ENABLE_DPI_AWARENESS": "True",
        "WINDOW_ACTIVATION_RETRY": "5",
        "WINDOW_ACTIVATION_DELAY": "0.5",
        "USE_WINDOW_MANAGER": "True",
        "ENABLE_PERFORMANCE_MODE": "True"
    }
}

# PES 인스턴스 관리 클래스
@dataclass
class PESInstance:
    """PES 인스턴스를 관리하는 데이터 클래스"""
    instance_id: int
    unique_id: str
    process: subprocess.Popen
    position: Tuple[int, int, int, int]
    project_id: Optional[str] = None
    login_hwnd: Optional[int] = None
    main_hwnd: Optional[int] = None
    status: str = "INITIALIZING"
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    error_count: int = 0
    last_window_check: datetime = field(default_factory=datetime.now)
    window_title_history: List[str] = field(default_factory=list)
    state_change_count: int = 0
    simulation_detected: bool = False
    
    def is_active(self) -> bool:
        """프로세스가 활성 상태인지 확인"""
        try:
            return self.process.poll() is None and psutil.Process(self.process.pid).is_running()
        except:
            return False
    
    def cleanup(self):
        """리소스 정리"""
        with window_handles_lock:
            if self.login_hwnd and self.login_hwnd in active_windows:
                del active_windows[self.login_hwnd]
            if self.main_hwnd and self.main_hwnd in active_windows:
                del active_windows[self.main_hwnd]
            with used_handles_lock:
                if self.login_hwnd in used_handles:
                    used_handles.remove(self.login_hwnd)
                if self.main_hwnd in used_handles:
                    used_handles.remove(self.main_hwnd)

# 창 관리자 클래스
class WindowManager:
    """창 관리를 위한 헬퍼 클래스"""
    
    @staticmethod
    def is_window_valid(hwnd: int) -> bool:
        """창 핸들이 유효한지 확인"""
        try:
            return bool(hwnd) and win32gui.IsWindow(hwnd) and win32gui.IsWindowVisible(hwnd)
        except:
            return False
    
    @staticmethod
    def safe_get_window_text(hwnd: int) -> str:
        """안전하게 창 제목 가져오기"""
        try:
            if WindowManager.is_window_valid(hwnd):
                return win32gui.GetWindowText(hwnd)
        except:
            pass
        return ""
    
    @staticmethod
    def activate_window(hwnd: int, max_retries: int = 5, delay: float = 0.5) -> bool:
        """창 활성화 (재시도 로직 포함)"""
        if not WindowManager.is_window_valid(hwnd):
            return False
        
        for attempt in range(max_retries):
            try:
                # 창이 최소화되어 있으면 복원
                if win32gui.IsIconic(hwnd):
                    win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
                    time.sleep(0.1)
                
                # 다양한 방법으로 창 활성화 시도
                # 방법 1: SetForegroundWindow
                win32gui.SetForegroundWindow(hwnd)
                
                # 방법 2: BringWindowToTop
                win32gui.BringWindowToTop(hwnd)
                
                # 방법 3: SetWindowPos로 Z-Order 변경
                win32gui.SetWindowPos(hwnd, win32con.HWND_TOPMOST, 0, 0, 0, 0,
                                    win32con.SWP_NOMOVE | win32con.SWP_NOSIZE)
                win32gui.SetWindowPos(hwnd, win32con.HWND_NOTOPMOST, 0, 0, 0, 0,
                                    win32con.SWP_NOMOVE | win32con.SWP_NOSIZE)
                
                # 활성화 확인
                time.sleep(0.1)
                if win32gui.GetForegroundWindow() == hwnd:
                    return True
                
                # Alt 키를 눌렀다 떼기 (Windows 정책 우회)
                if attempt > 0:
                    win32api.keybd_event(win32con.VK_MENU, 0, 0, 0)
                    win32api.keybd_event(win32con.VK_MENU, 0, win32con.KEYEVENTF_KEYUP, 0)
                    time.sleep(0.1)
                
            except Exception as e:
                logging.debug(f"창 활성화 시도 {attempt + 1} 실패: {str(e)}")
            
            if attempt < max_retries - 1:
                time.sleep(delay)
        
        return False
    
    @staticmethod
    def arrange_window(hwnd: int, x: int, y: int, width: int, height: int) -> bool:
        """창 위치 및 크기 조정"""
        if not WindowManager.is_window_valid(hwnd):
            return False
        
        try:
            # 현재 창 상태 확인
            placement = win32gui.GetWindowPlacement(hwnd)
            if placement[1] == win32con.SW_SHOWMINIMIZED:
                win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
                time.sleep(0.1)
            
            # 창 이동
            win32gui.MoveWindow(hwnd, x, y, width, height, True)
            return True
        except Exception as e:
            logging.error(f"창 배치 실패: {str(e)}")
            return False

# DPI 인식 설정
def set_dpi_awareness():
    """Windows DPI 인식 설정"""
    try:
        # Windows 10 1703 이상
        ctypes.windll.shcore.SetProcessDpiAwarenessContext(2)  # DPI_AWARENESS_CONTEXT_PER_MONITOR_AWARE_V2
    except:
        try:
            # Windows 8.1 이상
            ctypes.windll.shcore.SetProcessDpiAwareness(2)  # PROCESS_PER_MONITOR_DPI_AWARE
        except:
            try:
                # Windows Vista 이상
                ctypes.windll.user32.SetProcessDPIAware()
            except:
                pass

# 로깅 설정
def setup_logging(log_file):
    log_format = '%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

# 설정 로드 또는 생성
def load_config():
    config = configparser.ConfigParser()
    
    if os.path.exists(CONFIG_FILE):
        config.read(CONFIG_FILE, encoding='utf-8')
    else:
        for section, options in DEFAULT_CONFIG.items():
            if not config.has_section(section):
                config.add_section(section)
            for key, value in options.items():
                config.set(section, key, value)
        
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            config.write(f)
    
    # 누락된 설정 확인 및 추가
    for section, options in DEFAULT_CONFIG.items():
        if not config.has_section(section):
            config.add_section(section)
        for key, value in options.items():
            if not config.has_option(section, key):
                config.set(section, key, value)
    
    with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
        config.write(f)
    
    return config

# 모니터 정보 가져오기
def get_monitor_info():
    """사용 가능한 모니터 정보 가져오기"""
    monitors = []
    
    def callback(hMonitor, hdcMonitor, lprcMonitor, dwData):
        info = {}
        info['handle'] = hMonitor
        info['rect'] = (lprcMonitor.contents.left, lprcMonitor.contents.top,
                        lprcMonitor.contents.right, lprcMonitor.contents.bottom)
        info['width'] = lprcMonitor.contents.right - lprcMonitor.contents.left
        info['height'] = lprcMonitor.contents.bottom - lprcMonitor.contents.top
        info['work_area'] = info['rect']  # 작업 영역 (작업 표시줄 제외)
        monitors.append(info)
        return True
    
    # MONITORENUMPROC 타입 정의
    MonitorEnumProc = ctypes.WINFUNCTYPE(
        ctypes.c_bool, 
        ctypes.c_void_p, 
        ctypes.c_void_p, 
        ctypes.POINTER(wintypes.RECT), 
        ctypes.c_void_p
    )
    
    # 콜백 함수 생성
    enum_proc = MonitorEnumProc(callback)
    
    # 모든 모니터 열거
    ctypes.windll.user32.EnumDisplayMonitors(None, None, enum_proc, 0)
    
    return monitors

# 개선된 창 위치 계산 함수
def calculate_window_positions(instance_count, monitor_index=0):
    """모니터 해상도를 고려한 창 위치 계산"""
    monitors = get_monitor_info()
    
    if not monitors:
        # 모니터 정보를 가져올 수 없는 경우 기본값 사용
        screen_width = win32api.GetSystemMetrics(0)
        screen_height = win32api.GetSystemMetrics(1)
        monitor_rect = (0, 0, screen_width, screen_height)
    else:
        # 지정된 모니터 사용 (없으면 주 모니터)
        monitor_index = min(monitor_index, len(monitors) - 1)
        monitor = monitors[monitor_index]
        monitor_rect = monitor['rect']
    
    screen_left = monitor_rect[0]
    screen_top = monitor_rect[1]
    screen_width = monitor_rect[2] - monitor_rect[0]
    screen_height = monitor_rect[3] - monitor_rect[1]
    
    # 작업 표시줄을 고려한 여백
    margin_top = 30
    margin_bottom = 50
    margin_left = 10
    margin_right = 10
    
    usable_width = screen_width - margin_left - margin_right
    usable_height = screen_height - margin_top - margin_bottom
    
    positions = []
    
    # 인스턴스 수에 따른 최적 그리드 계산
    if instance_count <= 1:
        cols = 1
        rows = 1
    elif instance_count <= 4:
        cols = 2
        rows = 2
    elif instance_count <= 9:
        cols = 3
        rows = 3
    elif instance_count <= 16:
        cols = 4
        rows = 4
    elif instance_count <= 25:
        cols = 5
        rows = 5
    else:
        cols = 6
        rows = (instance_count + 5) // 6
    
    # 창 크기 계산
    window_spacing = 5  # 창 사이 간격
    window_width = (usable_width - (cols - 1) * window_spacing) // cols
    window_height = (usable_height - (rows - 1) * window_spacing) // rows
    
    # 최소 창 크기 보장
    min_width = 400
    min_height = 300
    window_width = max(window_width, min_width)
    window_height = max(window_height, min_height)
    
    # 위치 계산
    for i in range(instance_count):
        col = i % cols
        row = i // cols
        
        x = screen_left + margin_left + col * (window_width + window_spacing)
        y = screen_top + margin_top + row * (window_height + window_spacing)
        
        # 화면 경계 체크
        if x + window_width > screen_left + screen_width - margin_right:
            x = screen_left + screen_width - margin_right - window_width
        if y + window_height > screen_top + screen_height - margin_bottom:
            y = screen_top + screen_height - margin_bottom - window_height
        
        positions.append((x, y, window_width, window_height))
    
    return positions

# 사용자에게 인스턴스 수 입력받기
def get_instance_count_from_user(default_count):
    while True:
        try:
            user_input = input(f"\n실행할 PES 인스턴스 수를 입력하세요 (기본값: {default_count}, 권장: 1-30): ").strip()
            
            if not user_input:
                return default_count
            
            count = int(user_input)
            
            if count < 1:
                print("최소 1개 이상의 인스턴스를 실행해야 합니다.")
                continue
            
            if count > 30:
                print(f"경고: {count}개의 인스턴스는 시스템 리소스를 많이 사용할 수 있습니다.")
                confirm = input("계속하시겠습니까? (y/n): ").strip().lower()
                if confirm != 'y':
                    continue
            
            return count
            
        except ValueError:
            print("올바른 숫자를 입력해주세요.")
        except KeyboardInterrupt:
            print("\n프로그램을 종료합니다.")
            sys.exit(0)

# 실행 중인 PES 프로세스 모두 종료
def cleanup_all_pes_processes(process_name="PES.exe", logger=None):
    if not logger:
        logger = logging.getLogger(__name__)
        
    logger.info("모든 PES 프로세스 정리 시작")
    
    terminated_count = 0
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            proc_name = proc.info['name'].lower()
            if proc_name == process_name.lower():
                process = psutil.Process(proc.info['pid'])
                logger.info(f"PES 프로세스 종료 중: PID {proc.info['pid']}")
                process.terminate()
                terminated_count += 1
                try:
                    process.wait(timeout=3)
                except psutil.TimeoutExpired:
                    process.kill()
                    logger.info(f"PES 프로세스 강제 종료: PID {proc.info['pid']}")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
            
    if terminated_count > 0:
        time.sleep(3)
        
    remaining = 0
    for proc in psutil.process_iter(['pid', 'name']):
        try:
            if proc.info['name'].lower() == process_name.lower():
                remaining += 1
                try:
                    process = psutil.Process(proc.info['pid'])
                    process.kill()
                    logger.info(f"남은 PES 프로세스 강제 종료: PID {proc.info['pid']}")
                except:
                    pass
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
            
    logger.info(f"PES 프로세스 정리 완료: {terminated_count}개 종료, {remaining}개 남음")
    
    time.sleep(1)
    return terminated_count

# 특정 PES 프로세스 종료
def terminate_pes_process(process_pid, unique_id, logger, status="정상 종료"):
    global simulation_results
    
    try:
        process = psutil.Process(process_pid)
        logger.info(f"PES 프로세스 종료 중: ID {unique_id}, PID {process_pid}")
        
        with results_lock:
            for result in simulation_results:
                if result["unique_id"] == unique_id:
                    result["end_time"] = datetime.now()
                    result["status"] = status
                    result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                    break
        
        process.terminate()
        
        try:
            process.wait(timeout=3)
        except psutil.TimeoutExpired:
            process.kill()
            logger.info(f"PES 프로세스 강제 종료: ID {unique_id}, PID {process_pid}")
            
        save_results_to_excel()
        return True
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
        logger.error(f"프로세스 종료 오류 (ID {unique_id}, PID {process_pid}): {str(e)}")
        with results_lock:
            for result in simulation_results:
                if result["unique_id"] == unique_id:
                    result["end_time"] = datetime.now()
                    result["status"] = f"오류: {str(e)}"
                    result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                    break
        
        save_results_to_excel()
        return False
    except Exception as e:
        logger.error(f"프로세스 종료 중 예상치 못한 오류 (ID {unique_id}): {str(e)}")
        with results_lock:
            for result in simulation_results:
                if result["unique_id"] == unique_id:
                    result["end_time"] = datetime.now()
                    result["status"] = f"오류: {str(e)}"
                    result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                    break
        
        save_results_to_excel()
        return False

# 결과를 엑셀 파일에 저장
def save_results_to_excel(file_path=None):
    global simulation_results, results_file_path
    
    if file_path is None:
        file_path = results_file_path
    
    try:
        with results_lock:
            df = pd.DataFrame(simulation_results)
            
            if df.empty:
                return False
                
            columns = [
                "instance_id", "unique_id", "project_id", "start_time", "end_time", 
                "duration", "status", "pid"
            ]
            
            available_columns = [col for col in columns if col in df.columns]
            df = df[available_columns]
            
            for col in ['start_time', 'end_time']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if x else "")
            
            df.to_excel(file_path, index=False, sheet_name="PES 실행 결과")
        
        return True
    except Exception as e:
        logging.error(f"결과 저장 중 오류: {str(e)}")
        return False

# 창 핸들을 찾는 함수
def find_window_by_title(title_pattern, already_used_handles=None):
    result = []
    
    if already_used_handles is None:
        already_used_handles = set()
    
    def callback(hwnd, param):
        if win32gui.IsWindowVisible(hwnd) and hwnd not in already_used_handles:
            try:
                window_title = win32gui.GetWindowText(hwnd)
                if title_pattern in window_title:
                    with window_handles_lock:
                        if hwnd not in active_windows:
                            param.append((hwnd, window_title))
            except:
                pass
        return True
    
    try:
        win32gui.EnumWindows(callback, result)
    except:
        pass
        
    return result

# 창 제목 가져오기
def get_window_title(hwnd):
    try:
        return win32gui.GetWindowText(hwnd)
    except:
        return ""

# 특정 좌표의 픽셀 색상 가져오기
def get_pixel_color(x, y):
    try:
        screenshot = ImageGrab.grab(bbox=(x, y, x+1, y+1))
        pixel_color = screenshot.getpixel((0, 0))
        return pixel_color
    except Exception as e:
        logging.error(f"픽셀 색상 가져오기 오류: {str(e)}")
        return None

# 특정 영역의 이미지 캡처
def capture_area(hwnd, x_offset, y_offset, width, height):
    try:
        rect = win32gui.GetWindowRect(hwnd)
        x = rect[0] + x_offset
        y = rect[1] + y_offset
        screenshot = ImageGrab.grab(bbox=(x, y, x+width, y+height))
        return screenshot
    except Exception as e:
        logging.error(f"영역 캡처 오류: {str(e)}")
        return None

# 특정 컨트롤 찾기
def find_control_by_text(hwnd, text):
    result = []
    
    def callback(child_hwnd, param):
        try:
            title = win32gui.GetWindowText(child_hwnd)
            if text in title:
                param.append(child_hwnd)
        except:
            pass
        return True
    
    try:
        win32gui.EnumChildWindows(hwnd, callback, result)
        return result[0] if result else None
    except:
        return None

# 시뮬레이션 결과 탭 찾고 상태 확인
def is_simulation_result_tab_active(hwnd, logger):
    try:
        # 1. "Simulation Result" 텍스트를 가진 컨트롤 찾기
        tab_hwnd = find_control_by_text(hwnd, "Simulation Result")
        
        if not tab_hwnd:
            return False
        
        # 2. 창 좌표 가져오기
        rect = win32gui.GetWindowRect(hwnd)
        tab_rect = win32gui.GetWindowRect(tab_hwnd)
        
        # 3. 탭 주변 영역 캡처
        tab_area = capture_area(
            hwnd, 
            tab_rect[0] - rect[0], 
            tab_rect[1] - rect[1], 
            tab_rect[2] - tab_rect[0], 
            tab_rect[3] - tab_rect[1]
        )
        
        if tab_area:
            # 4. 이미지에서 파란색 픽셀 탐지
            width, height = tab_area.size
            blue_count = 0
            pixel_count = 0
            
            for x in range(0, width, 2):
                for y in range(0, height, 2):
                    pixel = tab_area.getpixel((x, y))
                    pixel_count += 1
                    
                    # 파란색 픽셀인지 확인
                    if len(pixel) >= 3 and pixel[2] > pixel[0] + 30 and pixel[2] > pixel[1] + 30:
                        blue_count += 1
            
            blue_ratio = blue_count / pixel_count if pixel_count > 0 else 0
            if blue_ratio > 0.15:
                logger.info(f"파란색 Simulation Result 탭 감지됨: 파란색 비율 {blue_ratio:.2f}")
                return True
        
        # 5. 결과 항목 확인
        result_items = [
            "Outlet Air/Inlet Water Temp", "Cooling/Heating Capacity",
            "Power Input", "EER", "Refrigerant Mass Flow", 
            "Proper Ref. Charge"
        ]
        
        child_windows = []
        win32gui.EnumChildWindows(hwnd, lambda child, lst: lst.append(child), child_windows)
        
        found_items = 0
        for child in child_windows:
            text = win32gui.GetWindowText(child)
            if any(item in text for item in result_items):
                found_items += 1
                
        if found_items >= 3:
            logger.info(f"Simulation Result 내용 감지됨: {found_items}개 항목 발견")
            return True
            
    except Exception as e:
        logger.warning(f"Simulation Result 탭 확인 중 오류: {str(e)}")
    
    return False

# PES 창을 특정 위치로 이동하는 함수
def arrange_window(hwnd, x, y, width=None, height=None, logger=None):
    if not logger:
        logger = logging.getLogger(__name__)
        
    if not hwnd or not win32gui.IsWindow(hwnd):
        return False
        
    try:
        current_rect = win32gui.GetWindowRect(hwnd)
        current_width = current_rect[2] - current_rect[0]
        current_height = current_rect[3] - current_rect[1]
        
        window_width = width if width else current_width
        window_height = height if height else current_height
        
        try:
            if win32gui.IsIconic(hwnd):
                win32gui.ShowWindow(hwnd, win32con.SW_RESTORE)
                time.sleep(0.2)
                
            win32gui.SetForegroundWindow(hwnd)
            time.sleep(0.2)
            
            win32gui.MoveWindow(hwnd, x, y, window_width, window_height, True)
            logger.debug(f"창 이동됨: 핸들={hwnd}, 위치=({x}, {y}), 크기=({window_width}x{window_height})")
            return True
        except Exception as e:
            logger.warning(f"창 이동 실패: {str(e)}")
            return False
    except Exception as e:
        logger.error(f"창 이동 중 오류: {str(e)}")
        return False

# 특정 창에 텍스트 전송 (개선된 버전)
def send_text_to_edit_control(hwnd, text, logger=None):
    if not logger:
        logger = logging.getLogger(__name__)
        
    try:
        # 창 활성화
        parent_hwnd = win32gui.GetParent(hwnd) or hwnd
        WindowManager.activate_window(parent_hwnd)
        time.sleep(0.1)
        
        # 포커스 설정
        win32gui.SetFocus(hwnd)
        time.sleep(0.1)
        
        # 텍스트 설정
        win32gui.SendMessage(hwnd, win32con.EM_SETSEL, 0, -1)
        win32gui.SendMessage(hwnd, win32con.WM_CLEAR, 0, 0)
        win32gui.SendMessage(hwnd, win32con.WM_SETTEXT, 0, text)
        time.sleep(0.1)
        return True
    except Exception as e:
        logger.warning(f"텍스트 전송 중 오류: {str(e)}")
        return False

# 버튼 클릭 함수
def click_button(hwnd, logger=None):
    if not logger:
        logger = logging.getLogger(__name__)
        
    if not hwnd or not win32gui.IsWindow(hwnd):
        return False
        
    try:
        win32gui.SendMessage(hwnd, win32con.BM_CLICK, 0, 0)
        time.sleep(0.2)
        return True
    except Exception as e:
        logger.warning(f"버튼 클릭 중 오류: {str(e)}")
        return False

# 자식 윈도우 찾기
def find_child_windows(parent_hwnd, class_name=None):
    result = []
    
    if not parent_hwnd or not win32gui.IsWindow(parent_hwnd):
        return result
        
    def callback(hwnd, param):
        try:
            if class_name is None or win32gui.GetClassName(hwnd) == class_name:
                param.append(hwnd)
        except:
            pass
        return True
        
    try:
        win32gui.EnumChildWindows(parent_hwnd, callback, result)
        return result
    except:
        return []

# DB 연결 확인 함수 추가
def check_db_connection(logger):
    """PES DB 연결 확인"""
    try:
        # 데이터베이스 연결 함수 (프로젝트 ID 모드용)
        def get_db_connection():
            try:
                import cx_Oracle
                connection = cx_Oracle.connect('acpes_app/acpes48app@CWRNDPTP')
                return connection
            except Exception as e:
                logger.error(f"데이터베이스 연결 실패: {e}")
                return None
        
        con = get_db_connection()
        if con:
            cursor = con.cursor()
            cursor.execute("SELECT 1 FROM DUAL")
            cursor.close()
            con.close()
            logger.info("PES DB 연결 확인 성공")
            return True
        else:
            logger.error("PES DB 연결 실패")
            return False
    except Exception as e:
        logger.error(f"DB 연결 확인 중 오류: {str(e)}")
        return False

# 오류 패턴 감지 함수
def detect_error_patterns(hwnd, logger):
    """창에서 오류 패턴 감지"""
    error_patterns = [
        "오류", "Error", "실패", "Failed", "Exception",
        "문제", "Problem", "Cannot", "Unable", "Invalid",
        "Connection failed", "Database error", "연결 실패"
    ]
    
    try:
        # 창 제목 확인
        title = get_window_title(hwnd)
        for pattern in error_patterns:
            if pattern.lower() in title.lower():
                logger.warning(f"창 제목에서 오류 패턴 감지: '{title}'")
                return True
        
        # 자식 창 텍스트 확인
        child_windows = find_child_windows(hwnd)
        for child in child_windows[:20]:  # 성능을 위해 처음 20개만 확인
            try:
                child_text = win32gui.GetWindowText(child)
                if child_text:
                    for pattern in error_patterns:
                        if pattern.lower() in child_text.lower():
                            logger.warning(f"자식 창에서 오류 패턴 감지: '{child_text}'")
                            return True
            except:
                pass
                
    except Exception as e:
        logger.error(f"오류 패턴 감지 중 예외: {str(e)}")
    
    return False

# 데이터베이스 연결 함수 (프로젝트 ID 모드용)
def get_db_connection():
    try:
        import cx_Oracle
        connection = cx_Oracle.connect('acpes_app/acpes48app@CWRNDPTP')
        return connection
    except Exception as e:
        logging.error(f"데이터베이스 연결 실패: {e}")
        return None

# 프로젝트 데이터 가져오기 함수 (프로젝트 ID 모드용)
def get_project_data(project_id, logger):
    try:
        con = get_db_connection()
        if con is None:
            logger.error(f"프로젝트 ID {project_id}의 데이터를 가져오기 위한 DB 연결 실패")
            return None
            
        cursor = con.cursor()
        query = f"""
        SELECT * FROM PES_EDX_INPUT_SINGLE_TBL_Test5 
        WHERE ID = '{project_id}' AND STATUS IN ('WAIT', 'RUN')
        """
            
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        result = cursor.fetchone()
            
        if result:
            project_data = dict(zip(columns, result))
            logger.info(f"프로젝트 ID {project_id}의 데이터를 성공적으로 가져왔습니다.")
            return project_data
        else:
            logger.warning(f"프로젝트 ID {project_id}의 데이터를 찾을 수 없거나 STATUS가 적절하지 않습니다.")
            return None
    except Exception as e:
        logger.error(f"프로젝트 데이터 가져오기 실패: {str(e)}")
        return None
    finally:
        if 'con' in locals() and con:
            con.close()

# 프로젝트 상태 업데이트 함수 (수정됨 - 동시 처리 지원)
def update_project_status_db(project_id, status, logger):
    """DB의 프로젝트 상태를 업데이트하는 함수"""
    # API 서버가 상태 관리를 하므로 여기서는 로그만 남김
    logger.info(f"프로젝트 ID {project_id}의 상태를 '{status}'로 업데이트 요청 (API 서버에서 처리)")
    
    # 기존 DB 직접 업데이트는 주석 처리
    """
    try:
        con = get_db_connection()
        if con is None:
            logger.error(f"프로젝트 ID {project_id} 상태 업데이트를 위한 DB 연결 실패")
            return False
        
        cursor = con.cursor()
        query = f"UPDATE PES_EDX_INPUT_SINGLE_TBL_Test5 SET STATUS = '{status}' WHERE ID = '{project_id}'"
        
        cursor.execute(query)
        rows_updated = cursor.rowcount
        con.commit()
        cursor.close()
        con.close()
        
        if rows_updated > 0:
            logger.info(f"프로젝트 ID {project_id}의 상태가 '{status}'로 업데이트되었습니다.")
            return True
        else:
            logger.warning(f"프로젝트 ID {project_id} 상태 업데이트 실패: 대상 레코드가 없습니다.")
            return False
    except Exception as e:
        logger.error(f"상태 업데이트 중 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())
        return False
    """
    return True  # API 서버에서 처리하므로 항상 True 반환

# 이미지에 보이는 결과 화면 탐지 기능 개선
def detect_simulation_result_screen(hwnd, logger):
    try:
        if is_simulation_result_tab_active(hwnd, logger):
            logger.info(f"파란색 Simulation Result 탭 감지됨")
            return True
            
        result_items = [
            "Outlet Air/Inlet Water Temp", "Cooling/Heating Capacity",
            "Power Input", "EER", "Refrigerant Mass Flow", 
            "Proper Ref. Charge", "Latent Heat", "Sensible Heat", "SHF", 
            "Super Heating", "Point Result", "Temperature", 
            "Pressure", "Enthalpy", "Quality", "Export Result",
            "Outdoor HEX Path Result", "Indoor HEX Path Result"
        ]
        
        child_windows = find_child_windows(hwnd)
        found_result_items = []

        for child in child_windows:
            try:
                child_text = win32gui.GetWindowText(child)
                if not child_text:
                    continue
                    
                for item in result_items:
                    if item.lower() in child_text.lower():
                        found_result_items.append(item)
                        logger.debug(f"결과 항목 발견: '{item}' (실제 텍스트: '{child_text}')")
                        break
            except:
                continue
        
        unique_items = set(found_result_items)
        if len(unique_items) >= 3:
            logger.info(f"결과 화면 감지 성공: {len(unique_items)}개 항목 발견 - {', '.join(list(unique_items)[:3])}...")
            return True
            
        number_with_unit_patterns = ["Hz", "℃", "Btu/h", "kJ/kg", "kPa", "W", "kg/h"]
        unit_count = 0
        
        for child in child_windows:
            try:
                child_text = win32gui.GetWindowText(child)
                if not child_text:
                    continue
                    
                for unit in number_with_unit_patterns:
                    if unit in child_text:
                        unit_count += 1
                        logger.debug(f"단위 패턴 발견: '{unit}' (전체 텍스트: '{child_text}')")
                        break
            except:
                continue
                
        if unit_count >= 3:
            logger.info(f"결과 단위 패턴 감지: {unit_count}개 단위 패턴 발견")
            return True
            
        static_controls = find_child_windows(hwnd, "Static")
        edit_controls = find_child_windows(hwnd, "Edit")
        
        if len(static_controls) > 15 and len(edit_controls) > 0:
            logger.info(f"결과 화면 구조 감지: Static 컨트롤 {len(static_controls)}개, Edit 컨트롤 {len(edit_controls)}개")
            return True
    
        return False
        
    except Exception as e:
        logger.warning(f"결과 화면 감지 중 오류: {str(e)}")
        return False

# 창의 특정 컨트롤/텍스트 패턴을 찾는 함수
def detect_final_simulation_screen(hwnd, logger):
    if is_simulation_result_tab_active(hwnd, logger):
        return True
    
    if detect_simulation_result_screen(hwnd, logger):
        return True
        
    try:
        final_screen_indicators = [
            "Simulation Result", "시뮬레이션 결과", "Results", "결과", 
            "Simulation Completed", "시뮬레이션 완료", "Final", "최종",
            "Point Result", "Temperature", "Pressure", "Enthalpy", "Quality",
            "Outdoor HEX Path Result", "Indoor HEX Path Result", "Export Result"
        ]
        
        title = get_window_title(hwnd)
        for indicator in final_screen_indicators:
            if indicator.lower() in title.lower():
                logger.info(f"창 제목에서 최종 화면 감지: '{title}'")
                return True
                
        child_windows = find_child_windows(hwnd)
        for child in child_windows:
            try:
                child_text = win32gui.GetWindowText(child)
                if child_text:
                    for indicator in final_screen_indicators:
                        if indicator.lower() in child_text.lower():
                            logger.info(f"자식 창에서 최종 화면 감지: '{child_text}'")
                            return True
            except:
                pass
                
        simulation_result_items = [
            "Refrigerant Mass Flow", "Proper Ref. Charge", "Latent Heat", "Sensible Heat", "SHF", "Super Heating",
            "Outlet Air/Inlet Water Temp", "Cooling/Heating Capacity", "Power Input", "EER"
        ]
        
        found_items = 0
        for child in child_windows:
            try:
                child_text = win32gui.GetWindowText(child)
                if child_text:
                    for item in simulation_result_items:
                        if item.lower() in child_text.lower():
                            found_items += 1
                            break
            except:
                pass
        
        if found_items >= 3:
            logger.info(f"결과 항목 {found_items}개가 감지됨 - 최종 화면으로 판단")
            return True
                
        static_controls = find_child_windows(hwnd, "Static")
        button_controls = find_child_windows(hwnd, "Button")
        
        if len(static_controls) > 10:
            logger.info(f"많은 Static 컨트롤 감지됨 ({len(static_controls)}개) - 결과 화면으로 판단")
            return True
            
        for button in button_controls:
            try:
                btn_text = win32gui.GetWindowText(button)
                if btn_text.lower() in ["ok", "확인", "close", "닫기", "exit", "종료"]:
                    logger.info(f"특정 버튼 감지됨: '{btn_text}' - 결과 화면으로 판단")
                    return True
            except:
                pass
    except Exception as e:
        logger.warning(f"최종 화면 감지 중 오류: {str(e)}")
    
    return False

# 화면 캡처 및 픽셀 비교로 결과 화면 감지
def detect_result_screen_by_visual(hwnd, logger):
    try:
        if is_simulation_result_tab_active(hwnd, logger):
            return True
            
        rect = win32gui.GetWindowRect(hwnd)
        width = rect[2] - rect[0]
        height = rect[3] - rect[1]
        
        if width < 400 or height < 300:
            return False
            
        tab_area = capture_area(hwnd, 10, 30, 150, 30)
        if tab_area:
            width, height = tab_area.size
            blue_count = 0
            pixel_count = 0
            
            for x in range(0, width, 2):
                for y in range(0, height, 2):
                    pixel = tab_area.getpixel((x, y))
                    pixel_count += 1
                    
                    if len(pixel) >= 3 and pixel[2] > pixel[0] + 30 and pixel[2] > pixel[1] + 30:
                        blue_count += 1
            
            blue_ratio = blue_count / pixel_count if pixel_count > 0 else 0
            if blue_ratio > 0.15:
                logger.info(f"결과 화면 탭에서 파란색 픽셀 감지: 비율 {blue_ratio:.2f}")
                return True
        
        return False
        
    except Exception as e:
        logger.warning(f"시각적 결과 화면 감지 중 오류: {str(e)}")
        return False

# 단일 PES 인스턴스 처리 함수 (개선된 버전)
def process_single_instance(instance_id, process, position, username, password, logger, login_delay=1, project_id=None):
    global simulation_results, used_handles
    
    x, y, width, height = position
    unique_id = str(uuid.uuid4())
    start_time = datetime.now()
    retry_count = 0
    max_retries = 3
    
    # PES 인스턴스 생성
    instance = PESInstance(
        instance_id=instance_id,
        unique_id=unique_id,
        process=process,
        position=position,
        project_id=project_id
    )
    
    # 활성 인스턴스에 추가
    active_instances[unique_id] = instance
    
    result_entry = {
        "instance_id": instance_id,
        "unique_id": unique_id,
        "pid": process.pid,
        "start_time": start_time,
        "end_time": None,
        "duration": None,
        "status": "실행 중"
    }
    
    if project_id:
        result_entry["project_id"] = project_id
    
    with results_lock:
        simulation_results.append(result_entry)
    
    logger.info(f"인스턴스 {instance_id} (고유 ID: {unique_id}) 처리 시작...")
    
    while retry_count < max_retries:
        try:
            # 로그인 창 찾기
            login_hwnd = None
            login_window_titles = ["Air Conditioner PES", "LG Electronics", "PES Login", "PES User Login"]
            
            start_search_time = time.time()
            while time.time() - start_search_time < 15 and not login_hwnd:
                for title in login_window_titles:
                    windows = find_window_by_title(title, used_handles)
                    if windows:
                        login_hwnd, window_title = windows[0]
                        with used_handles_lock:
                            used_handles.add(login_hwnd)
                        with window_handles_lock:
                            active_windows[login_hwnd] = unique_id
                        instance.login_hwnd = login_hwnd
                        logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 로그인 창 찾음 '{window_title}' (핸들: {login_hwnd})")
                        break
                
                if not login_hwnd:
                    time.sleep(0.5)
            
            if not login_hwnd:
                if retry_count < max_retries - 1:
                    logger.warning(f"인스턴스 {instance_id}: 로그인 창을 찾지 못했습니다. 재시도 중... ({retry_count + 1}/{max_retries})")
                    retry_count += 1
                    time.sleep(2)
                    continue
                else:
                    logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 로그인 창을 찾지 못했습니다.")
                    with results_lock:
                        for result in simulation_results:
                            if result["unique_id"] == unique_id:
                                result["end_time"] = datetime.now()
                                result["status"] = "로그인 창 찾기 실패"
                                result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                                break
                    
                    if project_id:
                        update_project_status_db(project_id, "ERROR", logger)
                        
                    return None
            
            # 오류 패턴 확인
            if error_detection_enabled and detect_error_patterns(login_hwnd, logger):
                logger.error(f"인스턴스 {instance_id}: 로그인 창에서 오류 감지됨")
                with results_lock:
                    for result in simulation_results:
                        if result["unique_id"] == unique_id:
                            result["end_time"] = datetime.now()
                            result["status"] = "로그인 창 오류"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                            break
                
                if project_id:
                    update_project_status_db(project_id, "ERROR", logger)
                    
                return None
            
            # 로그인 창 위치 설정
            arrange_window(login_hwnd, x, y, width, height, logger)
            time.sleep(0.5)
            
            # 로그인 처리
            logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 로그인 진행 중...")
            
            # 창 활성화
            if not WindowManager.activate_window(login_hwnd):
                logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 로그인 창 활성화 실패")
            
            # 자식 컨트롤 찾기
            edit_controls = find_child_windows(login_hwnd, "Edit")
            if len(edit_controls) < 2:
                logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): Edit 컨트롤을 충분히 찾지 못했습니다. 발견된 컨트롤 수: {len(edit_controls)}")
                with results_lock:
                    for result in simulation_results:
                        if result["unique_id"] == unique_id:
                            result["end_time"] = datetime.now()
                            result["status"] = "로그인 컨트롤 찾기 실패"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                            break
                
                if project_id:
                    update_project_status_db(project_id, "ERROR", logger)
                    
                return None
            
            # 사용자 이름 입력
            for i in range(3):
                if send_text_to_edit_control(edit_controls[0], username, logger):
                    break
                else:
                    logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 사용자 이름 입력 재시도 {i+1}/3")
                    time.sleep(0.3)
            else:
                logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 사용자 이름 입력 실패")
                with results_lock:
                    for result in simulation_results:
                        if result["unique_id"] == unique_id:
                            result["end_time"] = datetime.now()
                            result["status"] = "사용자 이름 입력 실패"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                            break
                
                if project_id:
                    update_project_status_db(project_id, "ERROR", logger)
                    
                return None
            
            # 비밀번호 입력
            for i in range(3):
                if send_text_to_edit_control(edit_controls[1], password, logger):
                    break
                else:
                    logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 비밀번호 입력 재시도 {i+1}/3")
                    time.sleep(0.3)
            else:
                logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 비밀번호 입력 실패")
                with results_lock:
                    for result in simulation_results:
                        if result["unique_id"] == unique_id:
                            result["end_time"] = datetime.now()
                            result["status"] = "비밀번호 입력 실패"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                            break
                
                if project_id:
                    update_project_status_db(project_id, "ERROR", logger)
                    
                return None
            
            # 로그인 버튼 찾기
            buttons = find_child_windows(login_hwnd, "Button")
            login_button = None
            
            for btn in buttons:
                try:
                    btn_text = win32gui.GetWindowText(btn)
                    if btn_text.lower() in ["login", "로그인", ""]:
                        login_button = btn
                        break
                except:
                    continue
            
            if not login_button and buttons:
                login_button = buttons[0]
            
            if not login_button:
                logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 로그인 버튼을 찾지 못했습니다.")
                with results_lock:
                    for result in simulation_results:
                        if result["unique_id"] == unique_id:
                            result["end_time"] = datetime.now()
                            result["status"] = "로그인 버튼 찾기 실패"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                            break
                        
                if project_id:
                    update_project_status_db(project_id, "ERROR", logger)
                    
                return None
            
            # 로그인 버튼 클릭
            for i in range(3):
                if click_button(login_button, logger):
                    break
                else:
                    logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 로그인 버튼 클릭 재시도 {i+1}/3")
                    time.sleep(0.3)
            else:
                logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 로그인 버튼 클릭 실패")
                with results_lock:
                    for result in simulation_results:
                        if result["unique_id"] == unique_id:
                            result["end_time"] = datetime.now()
                            result["status"] = "로그인 버튼 클릭 실패"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                            break
                        
                if project_id:
                    update_project_status_db(project_id, "ERROR", logger)
                    
                return None
            
            logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 로그인 완료")
            instance.status = "LOGGED_IN"
            
            time.sleep(login_delay)
            
            return instance
            
        except Exception as e:
            logger.error(f"인스턴스 {instance_id} 처리 중 예외 발생: {str(e)}")
            if retry_count < max_retries - 1:
                retry_count += 1
                logger.info(f"재시도 중... ({retry_count}/{max_retries})")
                time.sleep(2)
            else:
                with results_lock:
                    for result in simulation_results:
                        if result["unique_id"] == unique_id:
                            result["end_time"] = datetime.now()
                            result["status"] = f"예외 발생: {str(e)}"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                            break
                
                if project_id:
                    update_project_status_db(project_id, "ERROR", logger)
                    
                return None

# PES 메인 창 찾기 및 위치 조정
def find_and_arrange_main_window(instance, logger, window_stable_delay=2):
    instance_id = instance.instance_id
    unique_id = instance.unique_id
    position = instance.position
    x, y, width, height = position
    project_id = instance.project_id
    
    # 메인 창 찾기
    main_window_titles = ["PES - [제목 없음]", "PES -", "PES", "PES2", "PES mult test", "Air Conditioner PES"]
    main_hwnd = None
    
    start_time = time.time()
    while time.time() - start_time < 20 and not main_hwnd:
        for title in main_window_titles:
            windows = find_window_by_title(title, used_handles)
            if windows:
                main_hwnd, window_title = windows[0]
                with used_handles_lock:
                    used_handles.add(main_hwnd)
                with window_handles_lock:
                    active_windows[main_hwnd] = unique_id
                instance.main_hwnd = main_hwnd
                logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 메인 창 찾음 '{window_title}' (핸들: {main_hwnd})")
                break
        
        if not main_hwnd:
            time.sleep(0.5)
    
    if not main_hwnd:
        logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 메인 창을 찾지 못했습니다.")
        with results_lock:
            for result in simulation_results:
                if result["unique_id"] == unique_id:
                    result["end_time"] = datetime.now()
                    result["status"] = "메인 창 찾기 실패"
                    result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                    break
                
        if project_id:
            update_project_status_db(project_id, "ERROR", logger)
            
        return None
    
    # 오류 패턴 확인
    if error_detection_enabled and detect_error_patterns(main_hwnd, logger):
        logger.error(f"인스턴스 {instance_id}: 메인 창에서 오류 감지됨")
        with results_lock:
            for result in simulation_results:
                if result["unique_id"] == unique_id:
                    result["end_time"] = datetime.now()
                    result["status"] = "메인 창 오류"
                    result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                    break
        
        if project_id:
            update_project_status_db(project_id, "ERROR", logger)
            
        return None
    
    # 메인 창 위치 설정
    arrange_success = False
    for i in range(3):
        if arrange_window(main_hwnd, x, y, width, height, logger):
            arrange_success = True
            break
        else:
            logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 메인 창 위치 설정 재시도 {i+1}/3")
            time.sleep(0.5)
    
    if not arrange_success:
        logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 메인 창 위치 설정 실패, 기본 위치 사용")
    
    time.sleep(window_stable_delay)
    
    # 메인 창 활성화
    if not WindowManager.activate_window(main_hwnd):
        logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 메인 창 활성화 실패")
    
    instance.status = "READY"
    instance.window_title_history.append(get_window_title(main_hwnd))
    return instance

# Run Simulation 버튼 클릭 함수
def run_simulation(instance, logger):
    instance_id = instance.instance_id
    unique_id = instance.unique_id
    main_hwnd = instance.main_hwnd
    project_id = instance.project_id
    
    logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 시뮬레이션 실행 시도...")
    
    if not main_hwnd or not win32gui.IsWindow(main_hwnd):
        logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 유효한 메인 창 핸들이 없습니다.")
        with results_lock:
            for result in simulation_results:
                if result["unique_id"] == unique_id:
                    result["status"] = "시뮬레이션 실행 실패 - 창 핸들 없음"
                    break
                
        if project_id:
            update_project_status_db(project_id, "ERROR", logger)
            
        return False
    
    # 창 활성화
    if not WindowManager.activate_window(main_hwnd):
        logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 시뮬레이션 실행 전 창 활성화 실패")
    
    # 시뮬레이션 실행 방법 시도
    
    # 1. 메뉴 사용
    try:
        menu_bar = win32gui.GetMenu(main_hwnd)
        if menu_bar:
            menu_count = win32gui.GetMenuItemCount(menu_bar)
            for i in range(menu_count):
                try:
                    menu_text = win32gui.GetMenuString(menu_bar, i, 0x1000)[0]
                    if "simulation" in menu_text.lower() or "시뮬레이션" in menu_text.lower():
                        submenu = win32gui.GetSubMenu(menu_bar, i)
                        submenu_count = win32gui.GetMenuItemCount(submenu)
                        
                        for j in range(submenu_count):
                            item_text = win32gui.GetMenuString(submenu, j, 0x1000)[0]
                            if "run" in item_text.lower() or "실행" in item_text.lower():
                                command_id = win32gui.GetMenuItemID(submenu, j)
                                win32gui.SendMessage(main_hwnd, win32con.WM_COMMAND, command_id, 0)
                                logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 메뉴에서 시뮬레이션 실행 명령 전송")
                                time.sleep(0.5)
                                instance.status = "SIMULATING"
                                return True
                except:
                    continue
    except Exception as e:
        logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 메뉴 방식 시뮬레이션 실행 시도 중 오류: {str(e)}")
    
    # 2. 단축키 사용 (Alt+S, R)
    try:
        if WindowManager.activate_window(main_hwnd):
            time.sleep(0.3)
        
        win32api.keybd_event(win32con.VK_MENU, 0, 0, 0)
        time.sleep(0.1)
        
        win32api.keybd_event(0x53, 0, 0, 0)  # 'S' 키
        time.sleep(0.1)
        win32api.keybd_event(0x53, 0, win32con.KEYEVENTF_KEYUP, 0)
        time.sleep(0.3)
        
        win32api.keybd_event(0x52, 0, 0, 0)  # 'R' 키
        time.sleep(0.1)
        win32api.keybd_event(0x52, 0, win32con.KEYEVENTF_KEYUP, 0)
        
        win32api.keybd_event(win32con.VK_MENU, 0, win32con.KEYEVENTF_KEYUP, 0)
        
        logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): Alt+S, R 단축키로 시뮬레이션 실행 시도")
        time.sleep(0.5)
        instance.status = "SIMULATING"
        return True
    except Exception as e:
        logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 단축키 방식 시뮬레이션 실행 시도 중 오류: {str(e)}")
    
    # 3. 버튼 찾기
    try:
        all_children = find_child_windows(main_hwnd)
        for child in all_children:
            try:
                if win32gui.GetClassName(child) == "Button":
                    btn_text = win32gui.GetWindowText(child)
                    if ("simulation" in btn_text.lower() and "run" in btn_text.lower()):
                        click_button(child, logger)
                        logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 'Run Simulation' 버튼 클릭됨")
                        time.sleep(0.5)
                        instance.status = "SIMULATING"
                        return True
            except:
                continue
    except Exception as e:
        logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 버튼 찾기 방식 시뮬레이션 실행 시도 중 오류: {str(e)}")
    
    # 4. 도구 모음 버튼 찾기 시도
    try:
        rect = win32gui.GetWindowRect(main_hwnd)
        positions = [
            (rect[0] + 100, rect[1] + 40),
            (rect[0] + 150, rect[1] + 40),
            (rect[0] + 200, rect[1] + 40)
        ]
        
        for pos in positions:
            win32api.SetCursorPos(pos)
            time.sleep(0.2)
            win32api.mouse_event(win32con.MOUSEEVENTF_LEFTDOWN, 0, 0, 0, 0)
            time.sleep(0.1)
            win32api.mouse_event(win32con.MOUSEEVENTF_LEFTUP, 0, 0, 0, 0)
            time.sleep(0.3)
            
            logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 도구 모음 버튼 클릭 시도 위치: {pos}")
        
        instance.status = "SIMULATING"
        return True
    except Exception as e:
        logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 도구 모음 버튼 클릭 시도 중 오류: {str(e)}")
    
    logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 시뮬레이션 실행에 실패했습니다.")
    with results_lock:
        for result in simulation_results:
            if result["unique_id"] == unique_id:
                result["status"] = "시뮬레이션 실행 실패"
                break
            
    if project_id:
        update_project_status_db(project_id, "ERROR", logger)
            
    return False

# 창 변경 감지 및 프로그램 종료 함수 (개선된 버전)
def monitor_window_changes(instance, logger, check_interval=1, timeout=120, result_wait_time=3):
    instance_id = instance.instance_id
    unique_id = instance.unique_id
    main_hwnd = instance.main_hwnd
    process = instance.process
    project_id = instance.project_id
    
    logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 창 변경 모니터링 시작")
    
    start_time = time.time()
    last_title = get_window_title(main_hwnd) if main_hwnd else ""
    consecutive_result_screens = 0
    last_check_time = time.time()
    error_check_interval = 5  # 5초마다 오류 확인
    last_error_check = time.time()
    
    min_wait_after_simulation = 5
    
    while time.time() - start_time < timeout:
        try:
            # 프로세스가 아직 실행 중인지 확인
            if process.poll() is not None:
                logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 프로세스가 예기치 않게 종료됨")
                with results_lock:
                    for result in simulation_results:
                        if result["unique_id"] == unique_id:
                            result["end_time"] = datetime.now()
                            result["status"] = "프로세스 비정상 종료"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                            break
                
                if project_id:
                    update_project_status_db(project_id, "ERROR", logger)
                    
                save_results_to_excel()
                return
            
            if not win32gui.IsWindow(main_hwnd):
                logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 창이 이미 닫혔습니다. 프로세스 종료 중...")
                if project_id:
                    update_project_status_db(project_id, "END", logger)
                terminate_pes_process(process.pid, unique_id, logger, "창 닫힘")
                return
            
            current_title = get_window_title(main_hwnd)
            
            # 오류 패턴 주기적 확인
            if error_detection_enabled and time.time() - last_error_check > error_check_interval:
                last_error_check = time.time()
                if detect_error_patterns(main_hwnd, logger):
                    logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 오류 패턴 감지됨")
                    if project_id:
                        update_project_status_db(project_id, "ERROR", logger)
                    terminate_pes_process(process.pid, unique_id, logger, "오류 감지")
                    return
            
            # 1. 창 제목 변경 감지
            if current_title != last_title:
                logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 창 제목 변경 감지됨 '{last_title}' -> '{current_title}'")
                instance.state_change_count += 1
                instance.simulation_detected = True
                instance.window_title_history.append(current_title)
                last_title = current_title
                
                if "simulation result" in current_title.lower() or "시뮬레이션 결과" in current_title.lower():
                    logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 제목에서 시뮬레이션 결과 감지됨")
                    time.sleep(result_wait_time)
                    if project_id:
                        update_project_status_db(project_id, "END", logger)
                    terminate_pes_process(process.pid, unique_id, logger, "결과 화면 감지 (제목)")
                    return
            
            # 2. 빨간색 박스 내부의 Simulation Result 탭 확인
            if is_simulation_result_tab_active(main_hwnd, logger):
                logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 파란색 Simulation Result 탭 감지됨. 종료 중...")
                time.sleep(result_wait_time)
                if project_id:
                    update_project_status_db(project_id, "END", logger)
                terminate_pes_process(process.pid, unique_id, logger, "Simulation Result 탭 변경 감지")
                return
            
            # 3. 시뮬레이션 시작 후 일정 시간이 지났으면 강제 검사 수행
            if instance.simulation_detected and (time.time() - start_time) > min_wait_after_simulation:
                if time.time() - last_check_time >= 2:
                    last_check_time = time.time()
                    
                    if detect_final_simulation_screen(main_hwnd, logger):
                        consecutive_result_screens += 1
                        logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 결과 화면 감지 ({consecutive_result_screens}회 연속)")
                        
                        if consecutive_result_screens >= 1:
                            logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 결과 화면 확인됨. {result_wait_time}초 대기 후 PES 종료...")
                            time.sleep(result_wait_time)
                            if project_id:
                                update_project_status_db(project_id, "END", logger)
                            terminate_pes_process(process.pid, unique_id, logger, "결과 화면 감지 (내용)")
                            return
                    
                    elif detect_result_screen_by_visual(main_hwnd, logger):
                        logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 시각적 패턴으로 결과 화면 감지됨")
                        logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): {result_wait_time}초 대기 후 PES 종료...")
                        time.sleep(result_wait_time)
                        if project_id:
                            update_project_status_db(project_id, "END", logger)
                        terminate_pes_process(process.pid, unique_id, logger, "결과 화면 감지 (시각적)")
                        return
                    
                    else:
                        consecutive_result_screens = 0
                        
                    if instance.state_change_count >= 2 and time.time() - start_time > 30:
                        logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 다중 상태 변화 감지됨 (횟수: {instance.state_change_count}), 시간 경과: {time.time() - start_time:.1f}초")
                        
                        result_detected = detect_final_simulation_screen(main_hwnd, logger)
                        if result_detected:
                            logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 강제 검사에서 결과 화면 감지됨")
                            time.sleep(result_wait_time)
                            if project_id:
                                update_project_status_db(project_id, "END", logger)
                            terminate_pes_process(process.pid, unique_id, logger, "결과 화면 감지 (강제 검사)")
                            return
            
            # 시뮬레이션 시작 후 30초 이상 경과하면 결과 화면 검사 강화
            if instance.simulation_detected and (time.time() - start_time) > 30:
                if detect_final_simulation_screen(main_hwnd, logger) or detect_simulation_result_screen(main_hwnd, logger):
                    logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 30초 이상 경과 후 결과 화면 감지됨")
                    time.sleep(result_wait_time)
                    if project_id:
                        update_project_status_db(project_id, "END", logger)
                    terminate_pes_process(process.pid, unique_id, logger, "결과 화면 감지 (시간 경과)")
                    return
            
            # 시뮬레이션 시작 후 60초 이상 경과하면 강제 종료
            if instance.simulation_detected and (time.time() - start_time) > 60:
                logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 시간 초과로 종료 (60초)")
                if project_id:
                    update_project_status_db(project_id, "END", logger)
                terminate_pes_process(process.pid, unique_id, logger, "시간 초과 (60초)")
                return
            
            time.sleep(check_interval)
            
        except Exception as e:
            logger.error(f"인스턴스 {instance_id} (ID: {unique_id}): 모니터링 중 예외 발생: {str(e)}")
            # 예외 발생 시에도 계속 진행
    
    # 시간 초과
    logger.warning(f"인스턴스 {instance_id} (ID: {unique_id}): 창 변경 감지 시간 초과 ({timeout}초)")
    logger.info(f"인스턴스 {instance_id} (ID: {unique_id}): 시간 초과로 PES 종료 중...")
    if project_id:
        update_project_status_db(project_id, "END", logger)
    terminate_pes_process(process.pid, unique_id, logger, "시간 초과 (모니터링)")
    return

# 모든 프로세스 강제 종료 함수
def force_terminate_all_processes(process_name="PES.exe", logger=None):
    global TERMINATE_ALL
    TERMINATE_ALL = True
    
    if not logger:
        logger = logging.getLogger(__name__)
    
    logger.info("모든 PES 프로세스 강제 종료 시작")
    cleanup_all_pes_processes(process_name, logger)

# 키보드 인터럽트 핸들러
def signal_handler(sig, frame):
    print("\n프로그램을 종료합니다. 모든 PES 프로세스를 정리합니다...")
    force_terminate_all_processes()
    sys.exit(0)

# 프로젝트 ID 모드로 실행 함수 (수정됨 - API 서버와 협업)
def run_project_mode(project_id, logger):
    """
    특정 프로젝트 ID로 PES를 실행하는 함수
    """
    config = None
    
    config = load_config()
    
    # Oracle 클라이언트 초기화
    try:
        import cx_Oracle
        try:
            cx_Oracle.init_oracle_client(lib_dir=r"D:\instantclient_21_6")
            logger.info("Oracle 클라이언트 초기화 성공")
        except Exception as e:
            if "already been initialized" in str(e):
                logger.info("Oracle 클라이언트가 이미 초기화되어 있습니다.")
            else:
                logger.error(f"Oracle 클라이언트 초기화 실패: {e}")
                return False
        
        # DB 연결 확인
        if not check_db_connection(logger):
            logger.error("PES DB 연결 실패")
            return False
            
        # 프로젝트 데이터 가져오기
        project_data = get_project_data(project_id, logger)
        if not project_data:
            logger.error(f"프로젝트 ID {project_id}의 데이터를 가져오지 못했습니다.")
            return False
            
        # 단일 인스턴스 실행
        config = load_config()
        pes_path = config.get('Paths', 'PES_EXECUTABLE_PATH')
        working_dir = config.get('Paths', 'PES_WORKING_DIR')
        username = config.get('Credentials', 'PES_USERNAME')
        password = config.get('Credentials', 'PES_PASSWORD')
        login_delay = config.getfloat('Settings', 'LOGIN_DELAY', fallback=1)
        post_login_delay = config.getfloat('Settings', 'POST_LOGIN_DELAY', fallback=2)
        window_stable_delay = config.getfloat('Settings', 'WINDOW_STABLE_DELAY', fallback=2)
        monitor_interval = config.getfloat('Settings', 'SIMULATION_MONITOR_INTERVAL', fallback=1)
        window_change_timeout = config.getfloat('Settings', 'WINDOW_CHANGE_CHECK_TIMEOUT', fallback=120)
        result_wait_time = config.getfloat('Settings', 'RESULT_WAIT_TIME', fallback=3)
        monitor_index = config.getint('Settings', 'MONITOR_INDEX', fallback=0)
        
        # PES 프로세스는 API 서버에서 관리하므로 정리하지 않음
        logger.info(f"프로젝트 ID {project_id} 모드 - 동시 실행 지원")
        
        # 창 위치 계산 (단일 인스턴스)
        positions = calculate_window_positions(1, monitor_index)
        
        # PES 실행
        try:
            # 프로세스 시작
            process = subprocess.Popen(
                pes_path,
                cwd=working_dir,
                creationflags=subprocess.CREATE_NEW_CONSOLE
            )
            logger.info(f"PES 프로세스 시작됨 (PID: {process.pid})")
            
            # 프로세스 시작 대기
            time.sleep(2)
            
            # 로그인 처리
            instance = process_single_instance(
                1,  # 인스턴스 ID
                process, 
                positions[0], 
                username, 
                password, 
                logger,
                login_delay,
                project_id  # 프로젝트 ID 전달
            )
            
            if not instance:
                logger.error(f"프로젝트 ID {project_id}: 로그인 처리 실패")
                update_project_status_db(project_id, "ERROR", logger)
                return False
                
            # 로그인 후 대기
            time.sleep(post_login_delay)
            
            # 메인 창 찾기 및 위치 조정
            instance = find_and_arrange_main_window(
                instance, 
                logger,
                window_stable_delay
            )
            
            if not instance:
                logger.error(f"프로젝트 ID {project_id}: 메인 창 찾기 실패")
                update_project_status_db(project_id, "ERROR", logger)
                return False
                
            # 시뮬레이션 실행
            if not run_simulation(instance, logger):
                logger.error(f"프로젝트 ID {project_id}: 시뮬레이션 실행 실패")
                update_project_status_db(project_id, "ERROR", logger)
                return False
                
            # 창 변경 모니터링
            monitor_window_changes(
                instance, 
                logger, 
                monitor_interval, 
                window_change_timeout, 
                result_wait_time
            )
            
            # 실행 완료
            logger.info(f"프로젝트 ID {project_id}: 처리 완료")
            return True
            
        except Exception as e:
            logger.error(f"프로젝트 ID {project_id} 처리 중 오류 발생: {str(e)}")
            logger.error(traceback.format_exc())
            update_project_status_db(project_id, "ERROR", logger)
            return False
            
    except ImportError:
        logger.error("cx_Oracle 라이브러리를 찾을 수 없습니다. 프로젝트 ID 모드에는 필수 라이브러리입니다.")
        return False
    except Exception as e:
        logger.error(f"프로젝트 ID 모드 실행 중 예상치 못한 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            update_project_status_db(project_id, "ERROR", logger)
        except:
            pass
        return False
    finally:
        # 프로젝트별 PES 프로세스만 종료 (다른 프로젝트에 영향 없음)
        if 'process' in locals() and process:
            try:
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                    logger.info(f"프로젝트 ID {project_id}의 PES 프로세스 종료됨")
            except:
                pass

# 배치 처리를 위한 프로젝트 처리 워커
def process_project_worker(project_queue, config, logger):
    """큐에서 프로젝트를 가져와 처리하는 워커 스레드"""
    while True:
        try:
            project_id = project_queue.get()
            if project_id is None:  # 종료 신호
                break
                
            logger.info(f"워커 스레드: 프로젝트 {project_id} 처리 시작")
            
            # 프로젝트 처리
            success = run_project_mode(project_id, logger)
            
            if success:
                logger.info(f"워커 스레드: 프로젝트 {project_id} 처리 완료")
            else:
                logger.error(f"워커 스레드: 프로젝트 {project_id} 처리 실패")
                
            project_queue.task_done()
            
        except Exception as e:
            logger.error(f"워커 스레드 오류: {str(e)}")
            logger.error(traceback.format_exc())

# 배치 처리 함수
def run_batch_mode(project_ids, max_concurrent=5, logger=None):
    """여러 프로젝트를 동시에 처리하는 배치 모드"""
    if not logger:
        logger = logging.getLogger(__name__)
        
    logger.info(f"배치 모드 시작: {len(project_ids)}개 프로젝트, 최대 동시 실행: {max_concurrent}")
    
    # 프로젝트 큐 생성
    project_queue = queue.Queue()
    
    # 모든 프로젝트를 큐에 추가
    for project_id in project_ids:
        project_queue.put(project_id)
    
    # 워커 스레드 생성
    config = load_config()
    workers = []
    for i in range(min(max_concurrent, len(project_ids))):
        worker = threading.Thread(
            target=process_project_worker,
            args=(project_queue, config, logger),
            name=f"ProjectWorker-{i+1}"
        )
        worker.start()
        workers.append(worker)
        logger.info(f"워커 스레드 {i+1} 시작됨")
    
    # 모든 프로젝트 처리 완료 대기
    project_queue.join()
    
    # 워커 스레드 종료
    for _ in workers:
        project_queue.put(None)  # 종료 신호
    
    for worker in workers:
        worker.join()
        
    logger.info("배치 모드 처리 완료")

# 메인 함수
def main(project_id=None):
    global TERMINATE_ALL, simulation_results, results_file_path
    
    simulation_results = []
    
    # DPI 인식 설정
    set_dpi_awareness()
    
    config = load_config()
    
    pes_path = config.get('Paths', 'PES_EXECUTABLE_PATH')
    working_dir = config.get('Paths', 'PES_WORKING_DIR')
    username = config.get('Credentials', 'PES_USERNAME')
    password = config.get('Credentials', 'PES_PASSWORD')
    instance_count = config.getint('Settings', 'INSTANCE_COUNT')
    log_file = config.get('Settings', 'LOG_FILE')
    login_delay = config.getfloat('Settings', 'LOGIN_DELAY', fallback=1)
    post_login_delay = config.getfloat('Settings', 'POST_LOGIN_DELAY', fallback=2)
    window_stable_delay = config.getfloat('Settings', 'WINDOW_STABLE_DELAY', fallback=2)
    monitor_interval = config.getfloat('Settings', 'SIMULATION_MONITOR_INTERVAL', fallback=1)
    window_change_timeout = config.getfloat('Settings', 'WINDOW_CHANGE_CHECK_TIMEOUT', fallback=120)
    force_quit = config.getboolean('Settings', 'FORCE_QUIT_AFTER_SIMULATION', fallback=True)
    result_wait_time = config.getfloat('Settings', 'RESULT_WAIT_TIME', fallback=3)
    results_file = config.get('Settings', 'RESULTS_EXCEL_FILE', fallback=results_file_path)
    ask_instance_count = config.getboolean('Settings', 'ASK_INSTANCE_COUNT', fallback=True)
    monitor_index = config.getint('Settings', 'MONITOR_INDEX', fallback=0)
    
    results_file_path = results_file
    
    logger = setup_logging(log_file)
    
    # 프로젝트 ID 모드인 경우 별도 처리
    if project_id:
        logger.info(f"프로젝트 ID '{project_id}' 모드로 실행합니다.")
        return run_project_mode(project_id, logger)
    
    # 일반 모드 실행 (기존 코드와 동일)
    if ask_instance_count:
        instance_count = get_instance_count_from_user(instance_count)
        config.set('Settings', 'INSTANCE_COUNT', str(instance_count))
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            config.write(f)
    
    logger.info("=== PES 동시 실행기 시작 (개선된 버전) ===")
    logger.info(f"PES 실행 파일: {pes_path}")
    logger.info(f"작업 디렉토리: {working_dir}")
    logger.info(f"실행할 인스턴스 수: {instance_count}")
    logger.info(f"모니터링 간격: {monitor_interval}초")
    logger.info(f"자동 종료 시간: {window_change_timeout}초")
    logger.info(f"강제 종료 사용: {'예' if force_quit else '아니오'}")
    logger.info(f"결과 화면 대기 시간: {result_wait_time}초")
    logger.info(f"결과 저장 파일: {results_file}")
    logger.info(f"사용 모니터: {monitor_index}")
    
    cleanup_all_pes_processes(config.get('Settings', 'PES_PROCESS_NAME', fallback='PES.exe'), logger)
    
    positions = calculate_window_positions(instance_count, monitor_index)
    
    monitor_threads = []
    
    try:
        all_processes = []
        for i in range(instance_count):
            try:
                process = subprocess.Popen(
                    pes_path,
                    cwd=working_dir,
                    creationflags=subprocess.CREATE_NEW_CONSOLE
                )
                all_processes.append((i+1, process, positions[i]))
                logger.info(f"인스턴스 {i+1} 시작됨 (PID: {process.pid})")
            except Exception as e:
                logger.error(f"인스턴스 {i+1} 시작 실패: {str(e)}")
        
        logger.info("모든 프로세스 시작됨, 창 초기화 대기 중...")
        time.sleep(2)
        
        instances = []
        for instance_id, process, position in all_processes:
            instance = process_single_instance(
                instance_id, 
                process, 
                position, 
                username, 
                password, 
                logger,
                login_delay
            )
            if instance:
                instances.append(instance)
                
        logger.info(f"모든 로그인 완료, {post_login_delay}초 대기 중...")
        time.sleep(post_login_delay)
        
        updated_instances = []
        for instance in instances:
            updated_instance = find_and_arrange_main_window(
                instance, 
                logger,
                window_stable_delay
            )
            if updated_instance:
                updated_instances.append(updated_instance)
        
        if updated_instances:
            logger.info("모든 인스턴스에 시뮬레이션 실행 시작...")
            for instance in updated_instances:
                if run_simulation(instance, logger):
                    monitor_thread = threading.Thread(
                        target=monitor_window_changes,
                        args=(instance, logger, monitor_interval, window_change_timeout, result_wait_time),
                        daemon=True
                    )
                    monitor_thread.start()
                    monitor_threads.append((instance.unique_id, monitor_thread))
                    logger.info(f"인스턴스 {instance.instance_id} (ID: {instance.unique_id}): 창 모니터링 스레드 시작됨")
                
                time.sleep(0.5)
        
        save_results_to_excel()
        
        logger.info(f"총 {len(updated_instances)}개 인스턴스가 성공적으로 실행됨")
        
        logger.info("모든 PES 인스턴스가 실행되었습니다. 창 변경 감지를 모니터링 중...")
        
        check_count = 0
        force_quit_timeout = window_change_timeout // 2
        force_quit_start_time = time.time()
        
        while any(thread.is_alive() for _, thread in monitor_threads):
            if TERMINATE_ALL:
                break
                
            time.sleep(1)
            check_count += 1
            
            if check_count >= 60:
                active_count = sum(1 for _, thread in monitor_threads if thread.is_alive())
                logger.info(f"상태 확인: {active_count}개 인스턴스 모니터링 중")
                check_count = 0
                
                save_results_to_excel()
            
            if force_quit and time.time() - force_quit_start_time > force_quit_timeout:
                logger.warning(f"강제 종료 타임아웃 ({force_quit_timeout}초) 도달. 모든 PES 프로세스 종료 중...")
                process_name = config.get('Settings', 'PES_PROCESS_NAME', fallback='PES.exe')
                cleanup_all_pes_processes(process_name, logger)
                
                with results_lock:
                    for result in simulation_results:
                        if result["end_time"] is None:
                            result["end_time"] = datetime.now()
                            result["status"] = "강제 종료 (타임아웃)"
                            result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
                
                save_results_to_excel()
                break
                
        logger.info("모든 인스턴스 모니터링이 완료되었습니다.")
        
        save_results_to_excel()
        
        return True
            
    except KeyboardInterrupt:
        logger.info("사용자에 의해 프로그램이 종료되었습니다.")
        
        with results_lock:
            for result in simulation_results:
                if result["end_time"] is None:
                    result["end_time"] = datetime.now()
                    result["status"] = "사용자 중단"
                    result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
        
        save_results_to_excel()
        return False
        
    except Exception as e:
        logger.error(f"실행 중 오류 발생: {str(e)}")
        logger.error(traceback.format_exc())
        
        with results_lock:
            for result in simulation_results:
                if result["end_time"] is None:
                    result["end_time"] = datetime.now()
                    result["status"] = f"오류: {str(e)}"
                    result["duration"] = (result["end_time"] - result["start_time"]).total_seconds()
        
        save_results_to_excel()
        return False
        
    finally:
        cleanup_all_pes_processes(config.get('Settings', 'PES_PROCESS_NAME', fallback='PES.exe'), logger)
        
        save_results_to_excel()
        
        logger.info("=== PES 동시 실행기 종료 ===")

# 명령줄 인터페이스
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='PES 동시 실행기 (개선된 버전)')
    parser.add_argument('--count', type=int, help='실행할 인스턴스 수')
    parser.add_argument('--cleanup', action='store_true', help='모든 PES 프로세스 정리 후 종료')
    parser.add_argument('--force-quit', action='store_true', help='시뮬레이션 완료 후 강제 종료 사용')
    parser.add_argument('--result-wait', type=float, help='결과 화면 대기 시간(초)')
    parser.add_argument('--results-file', type=str, help='결과 저장 엑셀 파일 경로')
    parser.add_argument('--no-ask', action='store_true', help='실행 시 인스턴스 수를 묻지 않음')
    parser.add_argument('--project-id', type=str, help='PES API와 연동하기 위한 프로젝트 ID')
    parser.add_argument('--batch', type=str, help='배치 처리를 위한 프로젝트 ID 파일 경로')
    parser.add_argument('--max-concurrent', type=int, default=5, help='배치 처리 시 최대 동시 실행 수')
    parser.add_argument('--monitor', type=int, help='사용할 모니터 인덱스 (0=주 모니터)')
    parser.add_argument('--disable-error-detection', action='store_true', help='오류 감지 비활성화')
    
    args = parser.parse_args()
    
    config = load_config()
    log_file = config.get('Settings', 'LOG_FILE')
    logger = setup_logging(log_file)
    
    # 오류 감지 설정
    if args.disable_error_detection:
        error_detection_enabled = False
        logger.info("오류 감지가 비활성화되었습니다.")
    
    import signal
    signal.signal(signal.SIGINT, signal_handler)
    
    if args.cleanup:
        process_name = config.get('Settings', 'PES_PROCESS_NAME', fallback='PES.exe')
        count = cleanup_all_pes_processes(process_name, logger)
        print(f"{count}개의 PES 프로세스가 정리되었습니다.")
    elif args.batch:
        # 배치 모드
        logger.info(f"배치 파일: {args.batch}")
        try:
            # 파일에서 프로젝트 ID 목록 읽기
            project_ids = []
            with open(args.batch, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        project_ids.append(line)
            
            if project_ids:
                logger.info(f"{len(project_ids)}개의 프로젝트를 배치 처리합니다.")
                run_batch_mode(project_ids, args.max_concurrent, logger)
            else:
                logger.error("배치 파일에 유효한 프로젝트 ID가 없습니다.")
                
        except FileNotFoundError:
            logger.error(f"배치 파일을 찾을 수 없습니다: {args.batch}")
        except Exception as e:
            logger.error(f"배치 처리 중 오류: {str(e)}")
            logger.error(traceback.format_exc())
            
    elif args.project_id:
        logger.info(f"프로젝트 ID '{args.project_id}' 모드로 실행합니다.")
        try:
            try:
                import cx_Oracle
            except ImportError:
                logger.error("cx_Oracle 라이브러리를 찾을 수 없습니다. 프로젝트 ID 모드에는 필수 라이브러리입니다.")
                sys.exit(1)
                
            try:
                cx_Oracle.init_oracle_client(lib_dir=r"D:\instantclient_21_6")
                logger.info("Oracle 클라이언트 초기화 성공")
            except Exception as e:
                if "already been initialized" not in str(e):
                    logger.error(f"Oracle 클라이언트 초기화 실패: {e}")
                    sys.exit(1)
            
            config.set('Settings', 'INSTANCE_COUNT', '1')
            config.set('Settings', 'ASK_INSTANCE_COUNT', 'False')
            
            with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
                config.write(f)
            
            success = main(args.project_id)
            
            if not success:
                update_project_status_db(args.project_id, "ERROR", logger)
            
        except Exception as e:
            logger.error(f"프로젝트 ID 모드 실행 중 오류 발생: {e}")
            logger.error(traceback.format_exc())
            
            try:
                update_project_status_db(args.project_id, "ERROR", logger)
            except:
                pass
                
            sys.exit(1)
    else:
        if args.count:
            config.set('Settings', 'INSTANCE_COUNT', str(args.count))
            
        if args.no_ask:
            config.set('Settings', 'ASK_INSTANCE_COUNT', 'False')
            
        if args.force_quit:
            config.set('Settings', 'FORCE_QUIT_AFTER_SIMULATION', 'True')
            
        if args.result_wait:
            config.set('Settings', 'RESULT_WAIT_TIME', str(args.result_wait))
            
        if args.results_file:
            config.set('Settings', 'RESULTS_EXCEL_FILE', args.results_file)
            
        if args.monitor is not None:
            config.set('Settings', 'MONITOR_INDEX', str(args.monitor))
            
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            config.write(f)
        
        main()
