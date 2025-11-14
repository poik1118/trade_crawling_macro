# pip install -r requirements.txt

import os, sys, csv, json, time, shutil, traceback
from pathlib import Path
from datetime import datetime
from sshtunnel import SSHTunnelForwarder
import pymysql
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# 환경설정 / 환경변수 로드
SSH_HOST = os.environ.get("SSH_HOST", "")
SSH_PORT = int(os.environ.get("SSH_PORT", ""))
SSH_USER = os.environ.get("SSH_USER", "")
SSH_PASS = os.environ.get("SSH_PASS", "")

LOCAL_BIND_HOST = os.environ.get("LOCAL_BIND_HOST", "")
LOCAL_BIND_PORT = int(os.environ.get("LOCAL_BIND_PORT", ""))

DB_REMOTE_HOST = os.environ.get("DB_REMOTE_HOST", "")
DB_REMOTE_PORT = int(os.environ.get("DB_REMOTE_PORT", ""))

DB_USER = os.environ.get("DB_USER", "")
DB_PASS = os.environ.get("DB_PASS", "")
DB_NAME = os.environ.get("DB_NAME", "")

COOKIES_FILE = os.environ.get("COOKIES_FILE", "tradingview_cookies.json")
DOWNLOAD_ROOT = Path(os.environ.get("TV_DOWNLOAD_ROOT", "./data")).resolve()
USER_PROFILE_DIR = Path(os.environ.get("TV_CHROME_PROFILE", "./chrome_profile")).resolve()

INDICATORS = json.loads(os.environ.get("TV_INDICATORS", '["Relative Strength Index", "Moving Average Convergence Divergence"]'))
TV_TICKERS = json.loads(os.environ.get("TV_100_TICKERS_JSON", '["MMM"]'))  # TV_100_TICKERS_JSON(상위 100개) 또는 TV_ALL_TICKERS_JSON(전체 505개) 선택 TV_100_TICKERS_JSON

TIMEFRAMES = [
    ('12M', '12 months', '12M', False),
    ('M', '1 month', '1M', False),
    ('W', '1 week', '1W', False),
    ('D', '1 day', '1D', True),
    ('1h', '1 hour', '60', True),
    ('10m', '10 minutes', '10', True),
]


def ensure_dir(p: Path):
    if p.exists() and p.is_file():
        p.parent.mkdir(parents=True, exist_ok=True)
        return
    if p.suffix:
        p.parent.mkdir(parents=True, exist_ok=True)
        return
    p.mkdir(parents=True, exist_ok=True)

def create_ssh_tunnel():
    print("SSH 터널 연결 중")
    tunnel = SSHTunnelForwarder(
        (SSH_HOST, SSH_PORT),
        ssh_username=SSH_USER, ssh_password=SSH_PASS,
        local_bind_address=(LOCAL_BIND_HOST, LOCAL_BIND_PORT),
        remote_bind_address=(DB_REMOTE_HOST, DB_REMOTE_PORT)
    )
    tunnel.start()
    print("SSH 터널 연결 완료")
    return tunnel

def db_connect():
    print("DB 연결 중")
    conn = pymysql.connect(
        host=LOCAL_BIND_HOST, port=LOCAL_BIND_PORT,
        user=DB_USER, password=DB_PASS, database=DB_NAME,
        autocommit=False, charset="utf8mb4"
    )
    print("DB 연결 완료")
    return conn

def process_csv_for_db(csv_path: Path, symbol: str, timeframe: str):
    print(f"데이터 처리: {csv_path.name}")
    rows = []

    def get_val(row, names, default=None):
        for n in names:
            if n in row and row[n]:
                return row[n]
        lower_map = {k.lower(): v for k, v in row.items()}
        for n in names:
            if n.lower() in lower_map and lower_map[n.lower()]:
                return lower_map[n.lower()]
        return default

    def parse_float(x):
        if not x: return None
        s_val = str(x).replace(',', '').strip()

        if s_val.lower() == 'nan':
            return None  # 'nan' 문자열이면 None 반환

        try:    return float(s_val)
        except: return None

    def parse_int(x):
        try: return int(float(str(x).replace(',', '')))
        except: return None

    def parse_time(s):
        if not s: raise ValueError('time is None')
        s = str(s).strip().replace('Z', '')
        for fmt in ['%Y-%m-%dT%H:%M:%S', '%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M']:
            try: return datetime.strptime(s, fmt).strftime('%Y-%m-%d %H:%M:%S')
            except: pass
        try: return datetime.fromisoformat(s).strftime('%Y-%m-%d %H:%M:%S')
        except: raise ValueError(f'날짜 형식 파싱 실패: {s}')

    with csv_path.open(encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            try:
                time_raw = get_val(row, ['time', 'timestamp'])
                if not time_raw: continue
                time_parsed = parse_time(time_raw)

                open_v = parse_float(get_val(row, ['open', 'Open']))
                high_v = parse_float(get_val(row, ['high', 'High']))
                low_v = parse_float(get_val(row, ['low', 'Low']))
                close_v = parse_float(get_val(row, ['close', 'Close']))
                vol_v = parse_int(get_val(row, ['Volume', 'volume'])) or 0
                rsi_v = parse_float(get_val(row, ['RSI', 'rsi'])) or 0.0
                macd_v = parse_float(get_val(row, ['MACD', 'macd'])) or 0.0

                if None in (open_v, high_v, low_v, close_v): continue

                rows.append({
                    'symbol': symbol, 'timeframe': timeframe, 'time': time_parsed,
                    'open': open_v, 'high': high_v, 'low': low_v, 'close': close_v,
                    'volume': vol_v, 'rsi': rsi_v, 'macd': macd_v
                })
            except Exception as e:
                continue

    print(f"처리 완료: {len(rows)}행")
    return rows

def save_to_db(conn, data, symbol, timeframe):
    cur = conn.cursor()
    table = f"{symbol}_{timeframe}"

    try:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        print(f"기존 개별 테이블(`{table}`) 삭제")

        cur.execute(f"""CREATE TABLE IF NOT EXISTS `{table}` (
            symbol VARCHAR(32) NOT NULL, timeframe VARCHAR(16) NOT NULL,
            time DATETIME NOT NULL, open DECIMAL(18,8) NOT NULL,
            high DECIMAL(18,8) NOT NULL, low DECIMAL(18,8) NOT NULL,
            close DECIMAL(18,8) NOT NULL, volume BIGINT NOT NULL,
            rsi DECIMAL(10,5), macd DECIMAL(10,5),
            PRIMARY KEY (symbol, timeframe, time))""")

        sql = f"""INSERT INTO `{table}` (symbol, timeframe, time, open, high, low, close, volume, rsi, macd)
                  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                  ON DUPLICATE KEY UPDATE open=VALUES(open), high=VALUES(high), low=VALUES(low),
                  close=VALUES(close), volume=VALUES(volume), rsi=VALUES(rsi), macd=VALUES(macd)"""

        total = 0
        for i in range(0, len(data), 1000):
            batch = data[i:i+1000]
            values = [(r['symbol'], r['timeframe'], r['time'], r['open'], r['high'],
                      r['low'], r['close'], r['volume'], r['rsi'], r['macd']) for r in batch]
            cur.executemany(sql, values)
            total += len(batch)
        conn.commit()
        print(f"DB 저장 완료: {table}, {total}행")
        return total
    except Exception as e:
        print(f"DB 저장 오류: {e}")
        conn.rollback()
        raise
    finally:
        cur.close()

def setup_driver(download_dir: Path):
    print("Chrome 드라이버 설정 중")
    ensure_dir(USER_PROFILE_DIR)
    ensure_dir(download_dir)

    opts = Options()
    opts.add_argument(f"--user-data-dir={str(USER_PROFILE_DIR)}")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option('useAutomationExtension', False)
    opts.add_experimental_option("prefs", {
        "download.default_directory": str(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
        "safebrowsing.disable_download_protection": True
    })

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    driver.set_window_size(1600, 1000)
    print("드라이버 설정 완료")
    return driver

def save_cookies(driver):
    try:
        with open(COOKIES_FILE, "w", encoding="utf-8") as f:
            json.dump(driver.get_cookies(), f)
        print("쿠키 저장 완료")
    except Exception as e:
        print(f"쿠키 저장 실패: {e}")

def load_cookies(driver):
    if not Path(COOKIES_FILE).exists(): return False
    try:
        driver.get("https://www.tradingview.com/")
        time.sleep(2)
        with open(COOKIES_FILE, "r", encoding="utf-8") as f:
            for cookie in json.load(f):
                if 'sameSite' in cookie and cookie['sameSite'] is None:
                    cookie.pop('sameSite', None)
                try: driver.add_cookie(cookie)
                except: pass
        print("쿠키 로드 완료")
        return True
    except Exception as e:
        print(f"쿠키 로드 실패: {e}")
        return False

def manual_login(driver):
    print("쿠키 없음, 수동 로그인 필요")
    driver.get("https://www.tradingview.com/")
    input("> TradingView 로그인 후 Enter")
    save_cookies(driver)

def go_chart(driver, symbol, interval=None):
    url = f"https://www.tradingview.com/chart/?symbol={symbol}"
    if interval: url += f"&interval={interval}"
    print(f"차트 이동: {url}")
    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'canvas[data-name="pane-top-canvas"]')))

    except TimeoutException:
        print("!    차트 로드 지연 (1차 실패).")
        driver.refresh()
        timeout = 60
        print(f"차트 로드 재시도 {timeout}초 대기")
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'canvas[data-name="pane-top-canvas"]')))
            print("차트 로드 재시도 성공.")
        except TimeoutException:
            print(f"X    [{symbol}] 차트 로드 2차 실패: {url}")

def lazy_load_short_tf(driver: webdriver.Chrome, tf_short: str, tf_label: str) -> None:
    print(f"과거 데이터 로딩 시작 ({tf_short} - {tf_label})...")
    canvas = driver.find_element(By.CSS_SELECTOR, 'canvas[data-name="pane-top-canvas"]')
    size = canvas.size
    center_x = size["width"] // 2
    center_y = size["height"] // 2

    zoom_out_count = 15
    print(f"차트 축소 (휠 스크롤 {zoom_out_count}회)")
    for i in range(zoom_out_count):
        wheel_script = f"""
        var canvas = arguments[0];
        var rect = canvas.getBoundingClientRect();
        var clientX = rect.left + {center_x};
        var clientY = rect.top  + {center_y};

        canvas.dispatchEvent(new PointerEvent('pointermove', {{
            clientX: clientX, clientY: clientY, pointerId: 1, pointerType: 'mouse', bubbles: true
        }}));
        var e = new WheelEvent('wheel', {{
            clientX: clientX, clientY: clientY, deltaX: 0, deltaY: 200, deltaMode: 0, bubbles: true
        }});
        canvas.dispatchEvent(e);
        """
        driver.execute_script(wheel_script, canvas)
        time.sleep(0.05)

    # 프레임별 드래그 횟수
    if tf_short == "10m":   drag_count = 8
    elif tf_short == "1h":  drag_count = 12
    elif tf_short == "D":   drag_count = 6
    else:   drag_count = 8

    print(f"과거 데이터 로딩 (드래그 {drag_count}회)")
    for i in range(drag_count):
        start_x, end_x = 100, size["width"] - 100
        drag_script = f"""
        var canvas = arguments[0];
        var rect = canvas.getBoundingClientRect();

        var down = new MouseEvent('mousedown', {{
            clientX: rect.left + {start_x}, clientY: rect.top + {center_y},
            button: 0, buttons: 1, bubbles: true
        }});
        canvas.dispatchEvent(down);

        var steps = 10;
        var stepX = ({end_x} - {start_x}) / steps;
        for (var s = 1; s <= steps; s++) {{
            var move = new MouseEvent('mousemove', {{
                clientX: rect.left + {start_x} + stepX*s,
                clientY: rect.top + {center_y},
                button: 0, buttons: 1, bubbles: true
            }});
            canvas.dispatchEvent(move);
        }}

        var up = new MouseEvent('mouseup', {{
            clientX: rect.left + {end_x}, clientY: rect.top + {center_y},
            button: 0, buttons: 0, bubbles: true
        }});
        canvas.dispatchEvent(up);
        """
        driver.execute_script(drag_script, canvas)
        time.sleep(0.7)
    print(f"과거 데이터 로딩 완료.")


def add_indicator(driver, keyword):
    print(f"\n지표 추가: {keyword}")
    WebDriverWait(driver, 3).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "canvas[data-name='pane-top-canvas']")))
    time.sleep(2)

    for attempt in range(1):
        try:
            btn = None
            for sel in ['//*[@id="header-toolbar-indicators"]',
                       '//*[@id="header-toolbar-indicators"]/button']:
                try:
                    btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.XPATH, sel)))
                    if btn: break
                except: continue
            if not btn: continue
            btn.click()
            time.sleep(1)

            search = None
            for sel in ["//div[@role='dialog']//input[@type='text']",
                       "//input[contains(@placeholder, 'Search')]"]:
                try:
                    search = WebDriverWait(driver, 2).until(EC.presence_of_element_located((By.XPATH, sel)))
                    if search: break
                except: continue
            if not search: continue
            search.clear()
            search.send_keys(keyword)
            time.sleep(1)

            clicked = False
            for sel in [f'//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]/div[2]/div/div/div/div[2]',
                        "//div[@role='dialog']//span[contains(text(), '{keyword}')]"]:
                try:
                    WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.XPATH, sel))).click()
                    clicked = True
                    break
                except: continue
            if clicked: return
        except Exception as e:
            try: driver.find_element(By.XPATH, '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[1]/button').click()
            except: pass
            time.sleep(2)

def export_csv(driver):
    print("CSV 내보내기")
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div/div[3]/div/div/div[3]/div[1]/div/div/div/div/div[14]/div/div/div/button"))).click()    # Management Layout 버튼 클릭
    time.sleep(1)
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable((By.XPATH,'//*[@id="overlap-manager-root"]/div[2]/span/div[1]/div/div/div[4]'))).click() # Export chart data 버튼 클릭
    time.sleep(1)
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="time-format-select"]/span[2]'))).click()    # Time Format 버튼 클릭
    time.sleep(1)
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="time-format-iso"]'))).click()   # ISO 지정
    time.sleep(1)
    WebDriverWait(driver, 1).until(
        EC.element_to_be_clickable((By.XPATH, '//*[@id="overlap-manager-root"]/div[2]/div/div[1]/div/div[3]/div/span/button'))).click() # Export 버튼


def wait_for_download(download_dir: Path, timeout=5):
    print(f"다운로드 대기 중 {timeout}초")
    end_time = time.time() + timeout

    while time.time() < end_time:
        csvs = list(download_dir.glob("*.csv"))
        if csvs:
            latest_file = max(csvs, key=lambda x: x.stat().st_mtime)
            if latest_file.stat().st_size > 0:
                print(f"다운로드 확인 완료: {latest_file.name}")
                return latest_file
        time.sleep(0.5)
    raise TimeoutException("시간 초과. 파일을 찾을 수 없음")

def process_symbol(driver, symbol, download_dir, db_conn):
    print(f"{'='*18} [{symbol}] 처리 시작 {'='*18}")
    inserted_total = 0
    symbol_dir = download_dir / symbol
    ensure_dir(symbol_dir)

    for tf_short, tf_label, url_interval, requires_lazy in TIMEFRAMES:
        print("-" * 60)
        print(f"[{symbol}] TF: {tf_short}")
        tf_dir = symbol_dir / tf_short
        ensure_dir(tf_dir)

        go_chart(driver, symbol, url_interval)
        time.sleep(2)

        for indicator in INDICATORS:
            try:
                add_indicator(driver, indicator)
                time.sleep(1)
            except Exception as e:
                print(f"지표 추가 실패({indicator}): {e}")

        if requires_lazy:
            try: lazy_load_short_tf(driver, tf_short, tf_label)
            except Exception as e: print(f"과거 로딩 실패: {e}")

        try:
            export_csv(driver)
            csv_file = wait_for_download(download_dir)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            new_filename = f"{symbol}_{tf_short}_{timestamp}.csv"
            target = tf_dir / new_filename

            shutil.move(str(csv_file), str(target))
            data = process_csv_for_db(target, symbol, tf_short)
            if data:
                n = save_to_db(db_conn, data, symbol, tf_short)
                inserted_total += n
            print(f"[{symbol}] {tf_short} 완료: {len(data)}행")
        except Exception as e:
            print(f"[{symbol}] {tf_short} 오류: {e}")
            continue

    print(f"{'='*18} [{symbol}] 완료 (총 {inserted_total}행) {'='*18}\n")
    return {"inserted_rows": inserted_total}


def main():
    print("\n" + "="*60)
    print(" TradingView 데이터 수집 + DB 저장 자동화 ")
    print("="*60 + "\n")

    load_dotenv()
    ensure_dir(DOWNLOAD_ROOT)
    pending = TV_TICKERS

    print(f"대상: {len(pending)} / {len(TV_TICKERS)}")
    driver = tunnel = db_conn = None
    summary_rows = []

    try:
        driver = setup_driver(DOWNLOAD_ROOT)
        if not load_cookies(driver): manual_login(driver)
        tunnel = create_ssh_tunnel()
        db_conn = db_connect()

        total_count = len(pending)
        print(f"\n--- 총 {total_count}개 종목 처리 시작 ---")

        for i, symbol in enumerate(pending):
            print(f"\n--- [진행: {i + 1}/{total_count}] ---")

            result = process_symbol(driver, symbol, DOWNLOAD_ROOT, db_conn)
            row = {
                "symbol": symbol, "status": result.get("status", "fail"),
                "attempt": result.get("attempt", 0),
                "inserted_rows": result.get("inserted_rows", 0),
                "error": result.get("error", "")
            }
            summary_rows.append(row)
            time.sleep(3)

    except Exception as e:
        print(f"오류: {e}\n{traceback.format_exc()}")
        raise
    finally:
        db_conn.close(); print("DB 종료")
        tunnel.stop(); print("SSH 종료")
        driver.quit(); print("드라이버 종료")
        print("\n프로그램 수행 완료")

if __name__ == "__main__":
    main()