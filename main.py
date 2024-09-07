import websocket
import json
import time
from datetime import datetime, timedelta

# Bitquery API Key (EAP에 참여해 발급받은 키를 여기에 넣습니다)
# Bitquery API Key (Insert the key issued after joining EAP)
API_KEY = "your_bitquery_api_key"  # 이곳에 실제 API Key를 입력 / Insert your actual API Key here

# 로그 파일에 기록하는 함수 (utf-8 인코딩 사용)
# Function to log messages to a file (using utf-8 encoding)
def log_message(message):
    with open("log.txt", "a", encoding='utf-8') as log_file:
        log_file.write(f"{datetime.now()}: {message}\n")

# 서버로부터 받은 메시지를 처리하는 함수
# Function to handle messages received from the server
def on_message(ws, message):
    print(f"받은 메시지: {message}")  # Received message
    log_message(f"Received message: {message}")
    
    try:
        data = json.loads(message)
        # 메시지의 구조에 따른 처리
        # Process the message structure
        if 'errors' in data:
            error_message = f"서버 오류: {data['errors']}"  # Server error
            print(error_message)
            log_message(error_message)
        elif 'DEXTrades' in data.get('Solana', {}):
            for trade in data['Solana']['DEXTrades']:
                analyze_trade(trade)  # 새로운 DEX 거래 분석 / Analyze new DEX trade
        else:
            print("알 수 없는 메시지 형식")  # Unknown message format
            log_message("Unknown message format received")
    except json.JSONDecodeError:
        error_message = "JSON 디코딩 오류. 받은 메시지가 JSON 형식이 아닙니다."  # JSON decoding error
        print(error_message)
        log_message(error_message)

# 에러가 발생할 때 호출되는 함수
# Function called when an error occurs
def on_error(ws, error):
    print(f"에러 발생: {error}")  # Error occurred
    log_message(f"Error occurred: {error}")

# 웹소켓 연결이 닫힐 때 호출되는 함수
# Function called when the WebSocket connection closes
def on_close(ws, close_status_code, close_msg):
    print(f"웹소켓 연결 종료. 상태 코드: {close_status_code}, 메시지: {close_msg}")  # WebSocket connection closed
    log_message(f"Websocket connection closed. Status Code: {close_status_code}, Message: {close_msg}")

# 웹소켓 연결이 성공적으로 열렸을 때 호출되는 함수
# Function called when the WebSocket connection is successfully opened
def on_open(ws):
    print("웹소켓 연결 성공")  # WebSocket connection opened
    log_message("Websocket connection opened")
    
    # Solana 네트워크의 실시간 DEX 거래 구독
    # Subscribe to real-time DEX trades on the Solana network
    subscribe_message = json.dumps({
        "query": """
        subscription {
          Solana {
            DEXTrades {
              Transaction {
                Signature
              }
              Trade {
                Buy {
                  Account {
                    Address
                  }
                  Amount
                  AmountInUSD
                  PriceInUSD
                  Price
                  Currency {
                    Name
                    MintAddress
                  }
                }
                Dex {
                  ProgramAddress
                  ProtocolName
                }
                Sell {
                  Account {
                    Address
                  }
                  Amount
                  AmountInUSD
                  Currency {
                    MintAddress
                    Name
                  }
                  Price
                  PriceInUSD
                }
              }
            }
          }
        }
        """
    })
    ws.send(subscribe_message)
    log_message(f"Sent subscription message: {subscribe_message}")

# 거래 데이터를 분석하는 함수 (Solana DEX 데이터에 맞춰 수정됨)
# Function to analyze trade data (modified to fit Solana DEX data)
def analyze_trade(trade_data):
    buy_account = trade_data['Trade']['Buy']['Account']['Address']
    sell_account = trade_data['Trade']['Sell']['Account']['Address']
    buy_amount = trade_data['Trade']['Buy']['Amount']
    sell_amount = trade_data['Trade']['Sell']['Amount']
    buy_currency = trade_data['Trade']['Buy']['Currency']['Name']
    sell_currency = trade_data['Trade']['Sell']['Currency']['Name']

    if not buy_account or not sell_account:
        print("필요한 데이터가 없습니다.")  # Missing required data
        log_message("Missing required data in trade")
        return

    log_message(f"DEX 거래 발생: {buy_account}가 {buy_amount} {buy_currency} 구매, {sell_account}가 {sell_amount} {sell_currency} 판매")  
    # DEX trade occurred: {buy_account} bought {buy_amount} {buy_currency}, {sell_account} sold {sell_amount} {sell_currency}

    # 특정 지갑 통계 갱신 (Solana DEX 거래 데이터를 반영)
    # Update wallet stats (reflecting Solana DEX trade data)
    if buy_account not in wallet_stats:
        wallet_stats[buy_account] = {'trades': [], 'wins': 0, 'total_profit': 0}
    if sell_account not in wallet_stats:
        wallet_stats[sell_account] = {'trades': [], 'wins': 0, 'total_profit': 0}

    # 거래 데이터를 각 계정에 추가
    # Add trade data to each account
    current_time = datetime.now()
    wallet_stats[buy_account]['trades'].append({'timestamp': current_time, 'profit': buy_amount})
    wallet_stats[sell_account]['trades'].append({'timestamp': current_time, 'profit': sell_amount})

    # 30일 이상 된 데이터 제거
    # Remove data older than 30 days
    for account in [buy_account, sell_account]:
        wallet_stats[account]['trades'] = [t for t in wallet_stats[account]['trades'] if current_time - t['timestamp'] <= timedelta(days=30)]

    # 스마트 월렛 조건 확인
    # Check for smart wallet condition
    for account in [buy_account, sell_account]:
        if is_smart_wallet(account):
            smart_wallet_message = f"스마트 월렛 발견: {account}"  # Smart wallet detected
            print(smart_wallet_message)
            log_message(smart_wallet_message)
            send_alert(smart_wallet_message)

# 스마트 월렛을 확인하는 함수
# Function to check if the wallet is smart
def is_smart_wallet(wallet_address):
    stats = wallet_stats[wallet_address]
    if len(stats['trades']) < 10:  # 최소 거래 횟수 설정 / Minimum number of trades required
        return False
    
    win_rate = stats['wins'] / len(stats['trades'])
    avg_profit = stats['total_profit'] / len(stats['trades'])
    
    return win_rate >= 0.9 and avg_profit > 0.5  # 승률 90% 이상, 평균 수익률 50% 이상 / Win rate ≥ 90%, Avg profit > 50%

# 알림을 기록하는 함수 (utf-8 인코딩 사용)
# Function to log alerts (using utf-8 encoding)
def send_alert(message):
    alert_message = f"ALERT: {message}"
    print(alert_message)
    with open("alert.txt", "a", encoding='utf-8') as alert_file:
        alert_file.write(f"{datetime.now()}: {alert_message}\n")

# 지갑 통계를 저장할 딕셔너리
# Dictionary to store wallet statistics
wallet_stats = {}

if __name__ == "__main__":
    websocket.enableTrace(True)
    
    # WebSocket 헤더에 API 키 추가
    # Add API Key to WebSocket headers
    headers = {
        'X-API-KEY': API_KEY
    }
    
    # Bitquery의 스트리밍 API에 연결
    # Connect to Bitquery's streaming API
    ws = websocket.WebSocketApp("wss://streaming.bitquery.io/eap",
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open,
                                header=headers)

    ws.run_forever()
