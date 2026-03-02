import streamlit as st
import pandas as pd
from curl_cffi import requests
import random
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="Advanced Scraper", layout="wide")
st.title("🚀 Advanced Proxy + Captcha Aware Scraper → Excel")

# -----------------------------
# Helper Functions
# -----------------------------

def check_for_captcha(html):
    indicators = [
        "captcha","recaptcha","cf-challenge","cf-ray",
        "access denied","robot check","cloudflare",
        "attention required","security check"
    ]
    html = html.lower()
    return any(word in html for word in indicators)

def check_for_gone(html):
    indicators = [
        "410 gone","404 not found","no longer available",
        "has been removed","does not exist"
    ]
    html = html.lower()
    return any(word in html for word in indicators)

def parse_proxies(proxy_text):
    proxies = []
    for line in proxy_text.splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            if "://" in line:
                proxies.append(line)
            else:
                parts = line.split(":")
                if len(parts) == 4:
                    host, port, user, pwd = parts
                    proxies.append(f"http://{user}:{pwd}@{host}:{port}")
                elif len(parts) == 2:
                    host, port = parts
                    proxies.append(f"http://{host}:{port}")
    return proxies

# -----------------------------
# UI INPUTS
# -----------------------------

uploaded_urls = st.file_uploader("Upload URLs.txt", type=["txt"])
uploaded_proxies = st.file_uploader("Upload proxies.txt (optional)", type=["txt"])

col1, col2, col3 = st.columns(3)
max_workers = col1.number_input("Threads", 1, 50, 5)
max_retries = col2.number_input("Max Retries", 1, 5, 2)
proxy_rotate = col3.checkbox("Enable Proxy Rotation", True)

start_btn = st.button("Start Scraping")

# -----------------------------
# MAIN LOGIC
# -----------------------------

if uploaded_urls and start_btn:

    urls = uploaded_urls.read().decode("utf-8").splitlines()
    urls = [u.strip() for u in urls if u.strip()]
    total_urls = len(urls)

    proxies = []
    if uploaded_proxies:
        proxy_text = uploaded_proxies.read().decode("utf-8")
        proxies = parse_proxies(proxy_text)

    proxy_index = 0
    proxy_lock = threading.Lock()
    bad_proxies = set()
    proxy_fail_count = {}
    max_fail_per_proxy = 3

    stats = {
        "success": 0,
        "failed": 0,
        "gone": 0,
        "captcha": 0,
        "rate_limited": 0,
        "proxy_switches": 0
    }

    results = []
    lock = threading.Lock()

    progress_bar = st.progress(0)
    status_box = st.empty()

    def get_next_proxy():
        nonlocal proxy_index
        if not proxies:
            return None
        with proxy_lock:
            proxy = proxies[proxy_index % len(proxies)]
            proxy_index += 1
            stats["proxy_switches"] += 1
            return proxy

    def scrape(url):

        local_result = {
            "URL": url,
            "Status": "Failed",
            "Status Code": None,
            "Proxy Used": None,
            "Content Length": 0,
            "Timestamp": datetime.now()
        }

        for attempt in range(max_retries):

            proxy_url = get_next_proxy() if proxy_rotate else None
            proxy_dict = {"http": proxy_url, "https": proxy_url} if proxy_url else None

            try:
                headers = {
                    "User-Agent": random.choice([
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
                        "Mozilla/5.0 (X11; Linux x86_64)"
                    ])
                }

                time.sleep(random.uniform(1,2))

                response = requests.get(
                    url,
                    impersonate="chrome120",
                    timeout=30,
                    headers=headers,
                    proxies=proxy_dict,
                    verify=False
                )

                local_result["Status Code"] = response.status_code
                local_result["Proxy Used"] = proxy_url

                if response.status_code == 200:

                    if check_for_gone(response.text):
                        local_result["Status"] = "Content Gone"
                        stats["gone"] += 1
                        break

                    if check_for_captcha(response.text):
                        local_result["Status"] = "Captcha"
                        stats["captcha"] += 1
                        continue

                    local_result["Status"] = "Success"
                    local_result["Content Length"] = len(response.text)
                    stats["success"] += 1
                    break

                elif response.status_code == 410:
                    local_result["Status"] = "Gone (410)"
                    stats["gone"] += 1
                    break

                elif response.status_code in [403,429,503]:
                    stats["rate_limited"] += 1
                    continue

                else:
                    local_result["Status"] = f"HTTP {response.status_code}"

            except Exception as e:
                local_result["Status"] = str(e)

        if local_result["Status"] not in ["Success","Gone (410)","Content Gone"]:
            stats["failed"] += 1

        return local_result

    # Threaded Execution
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape, url): url for url in urls}

        completed = 0
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1

            progress_bar.progress(completed / total_urls)

            status_box.write(
                f"""
                ✅ Success: {stats['success']}  
                ❌ Failed: {stats['failed']}  
                📪 Gone: {stats['gone']}  
                ⚠️ Captcha: {stats['captcha']}  
                🚫 Rate Limited: {stats['rate_limited']}  
                🔄 Proxy Switches: {stats['proxy_switches']}  
                """
            )

    # -----------------------------
    # CREATE EXCEL
    # -----------------------------

    df = pd.DataFrame(results)

    output = BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    st.success("🎉 Scraping Completed!")

    st.download_button(
        label="Download Final Excel File",
        data=output,
        file_name="full_scraper_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
