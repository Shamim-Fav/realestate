import streamlit as st
from curl_cffi import requests
import time
import threading
import random
from pathlib import Path
import os
import json
from datetime import datetime

# ----------------------------
# SCRAPER CLASS
# ----------------------------

class FastHTMLScraper:

    def __init__(self, urls, use_proxies=False, proxy_file="proxies.txt"):

        self.urls = urls
        self.use_proxies = use_proxies
        self.proxy_file = proxy_file

        self.html_folder = Path("html_files")
        self.html_folder.mkdir(exist_ok=True)

        self.progress_file = "progress.txt"
        self.processed_urls = self.load_progress()

        self.proxies = self.load_proxies() if use_proxies else []
        self.proxy_index = 0

        self.running = True

        # Stats
        self.total = len(urls)
        self.processed = len(self.processed_urls)
        self.success = 0
        self.failed = 0
        self.gone = 0
        self.captcha = 0

    # ----------------------------

    def load_proxies(self):
        proxies = []
        if os.path.exists(self.proxy_file):
            with open(self.proxy_file, "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        proxies.append(line)
        return proxies

    # ----------------------------

    def load_progress(self):
        processed = set()
        if os.path.exists(self.progress_file):
            with open(self.progress_file, "r") as f:
                for line in f:
                    processed.add(line.strip())
        return processed

    # ----------------------------

    def save_progress(self, url):
        with open(self.progress_file, "a") as f:
            f.write(url + "\n")

    # ----------------------------

    def get_proxy(self):
        if not self.proxies:
            return None
        proxy = self.proxies[self.proxy_index]
        self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
        return {"http": proxy, "https": proxy}

    # ----------------------------

    def check_captcha(self, text):
        keywords = ["captcha", "cloudflare", "access denied", "robot"]
        lower = text.lower()
        return any(k in lower for k in keywords)

    # ----------------------------

    def download(self, url):

        if not self.running:
            return

        filename = url.rstrip("/").split("/")[-1]
        if not filename:
            filename = "index.html"

        filepath = self.html_folder / filename

        if filepath.exists():
            return

        proxy = self.get_proxy() if self.use_proxies else None

        try:
            time.sleep(random.uniform(1, 2))

            response = requests.get(
                url,
                timeout=30,
                proxies=proxy,
                impersonate="chrome120"
            )

            if response.status_code == 410:
                self.gone += 1
                self.save_progress(url)
                return

            if response.status_code != 200:
                self.failed += 1
                return

            if self.check_captcha(response.text):
                self.captcha += 1
                return

            with open(filepath, "w", encoding="utf-8") as f:
                f.write(response.text)

            self.success += 1
            self.save_progress(url)

        except:
            self.failed += 1

    # ----------------------------

    def run(self, progress_bar, status_box):

        for url in self.urls:

            if not self.running:
                break

            if url in self.processed_urls:
                continue

            self.download(url)

            self.processed += 1

            progress = self.processed / self.total
            progress_bar.progress(progress)

            status_box.write(
                f"""
                Total: {self.total}
                Processed: {self.processed}
                Success: {self.success}
                Failed: {self.failed}
                Gone: {self.gone}
                Captcha: {self.captcha}
                """
            )

        status_box.write("✅ Finished!")

# ----------------------------
# STREAMLIT UI
# ----------------------------

st.title("🚀 Fast HTML Scraper")

url_input = st.text_area("Paste URLs (one per line)")

use_proxy = st.checkbox("Use Proxies")

start_button = st.button("Start")
stop_button = st.button("Stop")

progress_bar = st.progress(0)
status_box = st.empty()

if "scraper" not in st.session_state:
    st.session_state.scraper = None

if start_button:

    if not url_input.strip():
        st.warning("Please enter URLs.")
    else:
        urls = [u.strip() for u in url_input.split("\n") if u.strip()]

        scraper = FastHTMLScraper(urls, use_proxies=use_proxy)
        st.session_state.scraper = scraper

        thread = threading.Thread(
            target=scraper.run,
            args=(progress_bar, status_box),
            daemon=True
        )
        thread.start()

if stop_button:
    if st.session_state.scraper:
        st.session_state.scraper.running = False
        st.warning("Stopping...")
