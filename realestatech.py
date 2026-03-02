import streamlit as st
import pandas as pd
from curl_cffi import requests
import time
import random
import json
from io import BytesIO
from datetime import datetime

# --- Configuration & UI ---
st.set_page_config(page_title="Swiss Real Estate Scraper", layout="wide")

st.title("🏘️ Swiss Real Estate HTML to Excel")
st.markdown("""
- **No Proxy Mode**: Uses local IP with Chrome 120 impersonation.
- **Excel Output**: All data (including 'anti-captcha' pages) saved in one file.
- **Easy Download**: Get your results in one click.
""")

# --- Scraper Logic ---
class StreamlitScraper:
    def __init__(self, urls, delay_min, delay_max):
        self.urls = urls
        self.delay_min = delay_min
        self.delay_max = delay_max
        self.results = []

    def check_for_captcha(self, html):
        indicators = ['captcha', 'recaptcha', 'cf-challenge', 'turnstile', 'robot check', 'access denied']
        return any(x in html.lower() for x in indicators)

    def run(self):
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Use a session for better performance
        with requests.Session() as s:
            for i, url in enumerate(self.urls):
                status_text.text(f"Processing {i+1}/{len(self.urls)}: {url}")
                
                row = {
                    "URL": url,
                    "Status": "Pending",
                    "Status_Code": None,
                    "Timestamp": datetime.now().strftime("%H:%M:%S"),
                    "Is_Captcha_Page": False,
                    "Full_HTML": ""
                }

                try:
                    # Chrome 120 Impersonation
                    resp = s.get(
                        url, 
                        impersonate="chrome120", 
                        timeout=30,
                        verify=False
                    )
                    
                    row["Status_Code"] = resp.status_code
                    row["Full_HTML"] = resp.text
                    
                    if resp.status_code == 200:
                        row["Status"] = "Success"
                        if self.check_for_captcha(resp.text):
                            row["Is_Captcha_Page"] = True
                    elif resp.status_code == 410:
                        row["Status"] = "Gone (410)"
                    else:
                        row["Status"] = f"HTTP Error {resp.status_code}"

                except Exception as e:
                    row["Status"] = "Failed"
                    row["Full_HTML"] = str(e)

                self.results.append(row)
                progress_bar.progress((i + 1) / len(self.urls))
                
                # Human-like delay (Since no proxy is used)
                if i < len(self.urls) - 1:
                    time.sleep(random.uniform(self.delay_min, self.delay_max))
                    
        return pd.DataFrame(self.results)

# --- Sidebar Controls ---
st.sidebar.header("Scraper Settings")
d_min = st.sidebar.slider("Min Delay (sec)", 0.5, 3.0, 1.0)
d_max = st.sidebar.slider("Max Delay (sec)", 1.5, 10.0, 3.0)

# --- Main App ---
uploaded_file = st.file_uploader("Upload URLs (.txt)", type=['txt'])

if uploaded_file:
    content = uploaded_file.read().decode("utf-8")
    url_list = [line.strip() for line in content.split('\n') if line.strip()]
    st.success(f"Loaded {len(url_list)} URLs")

    if st.button("🚀 Start Scraping"):
        scraper = StreamlitScraper(url_list, d_min, d_max)
        df_final = scraper.run()
        
        st.divider()
        st.subheader("Results Preview")
        # Don't show the massive HTML column in the browser preview
        st.dataframe(df_final.drop(columns=["Full_HTML"]).head(20))

        # --- Excel Export Logic ---
        output = BytesIO()
        # Use 'xlsxwriter' to handle large HTML strings in cells
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_final.to_excel(writer, index=False, sheet_name='Data')
            
            # Auto-adjust column width for readability
            workbook  = writer.book
            worksheet = writer.sheets['Data']
            worksheet.set_column('A:A', 40) # URLs
            worksheet.set_column('F:F', 50) # HTML Content

        st.download_button(
            label="📥 Download Excel File",
            data=output.getvalue(),
            file_name=f"homegate_results_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
