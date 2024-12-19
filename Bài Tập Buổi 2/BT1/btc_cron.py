import os
import requests
import datetime

# Tạo folder để lưu file nếu chưa tồn tại
output_dir = "btc_data"
os.makedirs(output_dir, exist_ok=True)

# Hàm lấy giá BTC/USD
def fetch_btc_price():
    url = "https://api.coindesk.com/v1/bpi/currentprice.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        price = data['bpi']['USD']['rate']
        return price
    else:
        print("Failed to fetch data:", response.status_code)
        return None

# Lưu giá BTC/USD vào file txt
def save_btc_price():
    current_time = datetime.datetime.now()
    filename = current_time.strftime("%H_%M_%d_%m_%Y") + ".txt"
    filepath = os.path.join(output_dir, filename)

    price = fetch_btc_price()
    if price:
        with open(filepath, "w") as f:
            f.write(f"BTC/USD: {price}\n")
        print(f"Saved BTC/USD price to {filepath}")

# Xóa các file txt không thuộc ngày hiện tại
def cleanup_old_files():
    today = datetime.datetime.now().date()
    for file in os.listdir(output_dir):
        filepath = os.path.join(output_dir, file)
        if os.path.isfile(filepath):
            create_time = datetime.datetime.fromtimestamp(os.path.getctime(filepath)).date()
            if create_time != today:
                os.remove(filepath)
                print(f"Deleted old file: {filepath}")

# Chạy script
if __name__ == "__main__":
    save_btc_price()
   # cleanup_old_files()
