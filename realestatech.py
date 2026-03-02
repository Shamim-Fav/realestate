import streamlit as st
import pandas as pd
from curl_cffi import requests
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from io import BytesIO
from datetime import datetime

st.set_page_config(page_title="Advanced Scraper", layout="wide")
st.title("🚀 Fast Scraper → Excel (No Proxy)")

# -----------------------------
# Helper Functions
# -----------------------------

def check_for_captcha(html):
    indicators = [
        "captcha","recaptcha","cf-challenge",
        "access denied","robot check",
        "cloudflare","attention required"
    ]
    html = html.lower()
    return any(word in html for word in indicators)

def check_for_gone(html):
    indicators = [
        "410 gone","404 not found",
        "no longer available",
        "has been removed","does not exist"
    ]
    html = html.lower()
    return any(word in html for word in indicators)

# -----------------------------
# UI
# -----------------------------

uploaded_urls = st.file_uploader("Upload URLs.txt", type=["txt"])

col1, col2 = st.columns(2)
max_workers = col1.number_input("Threads", 1, 50, 5)
max_retries = col2.number_input("Max Retries", 1, 5, 2)

start_btn = st.button("Start Scraping")

# -----------------------------
# MAIN LOGIC
# -----------------------------

if uploaded_urls and start_btn:

    urls = uploaded_urls.read().decode("utf-8").splitlines()
    urls = [u.strip() for u in urls if u.strip()]
    total_urls = len(urls)

    stats = {
        "success": 0,
        "failed": 0,
        "gone": 0,
        "captcha": 0,
        "rate_limited": 0
    }

    results = []

    progress_bar = st.progress(0)
    status_box = st.empty()

    def scrape(url):

        result = {
            "URL": url,
            "Status": "Failed",
            "Status Code": None,
            "Content Length": 0,
            "Timestamp": datetime.now()
        }

        for attempt in range(max_retries):
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
                    verify=False
                )

                result["Status Code"] = response.status_code

                if response.status_code == 200:

                    if check_for_gone(response.text):
                        result["Status"] = "Content Gone"
                        stats["gone"] += 1
                        break

                    if check_for_captcha(response.text):
                        result["Status"] = "Captcha"
                        stats["captcha"] += 1
                        continue

                    result["Status"] = "Success"
                    result["Content Length"] = len(response.text)
                    stats["success"] += 1
                    break

                elif response.status_code == 410:
                    result["Status"] = "Gone (410)"
                    stats["gone"] += 1
                    break

                elif response.status_code in [403,429,503]:
                    stats["rate_limited"] += 1
                    continue

                else:
                    result["Status"] = f"HTTP {response.status_code}"

            except Exception as e:
                result["Status"] = str(e)

        if result["Status"] not in ["Success","Gone (410)","Content Gone"]:
            stats["failed"] += 1

        return result

    # Threaded Execution
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scrape, url): url for url in urls}

        completed = 0
        for future in as_completed(futures):
            results.append(future.result())
            completed += 1

            progress_bar.progress(completed / total_urls)

            status_box.write(
                f"""
                ✅ Success: {stats['success']}  
                ❌ Failed: {stats['failed']}  
                📪 Gone: {stats['gone']}  
                ⚠️ Captcha: {stats['captcha']}  
                🚫 Rate Limited: {stats['rate_limited']}  
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
        file_name="scraper_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
