import streamlit as st
import time
import random
from curl_cffi import requests
from datetime import datetime
import pandas as pd

st.title("Homegate URL Checker (Safe Mode)")

uploaded_file = st.file_uploader("Upload URL file (.txt)", type=["txt"])

if uploaded_file:
    urls = uploaded_file.read().decode("utf-8").splitlines()
    urls = [u.strip() for u in urls if u.strip()]

    if st.button("Start Checking"):

        results = []
        progress = st.progress(0)

        stats = {
            "processed": 0,
            "success": 0,
            "failed": 0,
            "captcha": 0
        }

        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }

        for i, url in enumerate(urls):

            max_retries = 3
            result = None

            for attempt in range(max_retries):
                try:
                    # Human-like delay
                    time.sleep(random.uniform(1, 3))

                    response = requests.get(
                        url,
                        headers=headers,
                        impersonate="chrome120",
                        timeout=30,
                        verify=False
                    )

                    status_code = response.status_code
                    content_length = len(response.text)

                    if "captcha" in response.text.lower():
                        status = "Captcha"
                        stats["captcha"] += 1
                    elif status_code == 200:
                        status = "Success"
                        stats["success"] += 1
                    else:
                        status = "Failed"
                        stats["failed"] += 1

                    result = {
                        "URL": url,
                        "Status": status,
                        "Status Code": status_code,
                        "Content Length": content_length,
                        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }

                    break

                except Exception:
                    time.sleep(2)

            if result is None:
                stats["failed"] += 1
                result = {
                    "URL": url,
                    "Status": "Error",
                    "Status Code": 0,
                    "Content Length": 0,
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

            results.append(result)
            stats["processed"] += 1

            progress.progress((i + 1) / len(urls))

        df = pd.DataFrame(results)

        st.success("Finished!")
        st.write(stats)
        st.dataframe(df)

        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            csv,
            "results.csv",
            "text/csv"
        )
