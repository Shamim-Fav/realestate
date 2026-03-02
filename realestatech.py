import streamlit as st
import pandas as pd
from curl_cffi import requests
import time
import random
from io import BytesIO
from datetime import datetime

# --- Functions ---

def check_for_captcha(html):
    """Checks if the page looks like a challenge, but doesn't stop the crawl."""
    indicators = ['captcha', 'recaptcha', 'cf-challenge', 'robot check', 'turnstile']
    html_l = html.lower()
    return any(x in html_l for x in indicators)

def scrape_urls(urls, wait_min=1, wait_max=3):
    results = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    # Use a session for better performance/connection pooling
    with requests.Session() as s:
        for i, url in enumerate(urls):
            status_text.text(f"🔍 Processing {i+1}/{len(urls)}: {url}")
            
            row = {
                "URL": url,
                "Status Code": None,
                "Fetch Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Is Captcha Page": False,
                "HTML Content": ""
            }

            try:
                # Impersonate Chrome 120 to bypass TLS fingerprinting
                resp = s.get(
                    url, 
                    impersonate="chrome120", 
                    timeout=30,
                    verify=False
                )
                
                row["Status Code"] = resp.status_code
                row["HTML Content"] = resp.text
                
                if resp.status_code == 200:
                    if check_for_captcha(resp.text):
                        row["Is Captcha Page"] = True
                
            except Exception as e:
                row["Status Code"] = "ERROR"
                row["HTML Content"] = str(e)

            results.append(row)
            
            # Update UI
            progress_bar.progress((i + 1) / len(urls))
            
            # Random delay to prevent IP blocking since not using proxies
            if i < len(urls) - 1:
                time.sleep(random.uniform(wait_min, wait_max))
                
    return pd.DataFrame(results)

# --- Streamlit UI ---

st.set_page_config(page_title="Fast HTML to Excel Scraper", layout="wide")

st.title("📊 Web Content to Excel Scraper")
st.markdown("""
This tool uses **Chrome TLS Impersonation** to fetch pages without a proxy. 
Even if a 'Captcha' is detected, the full HTML is saved to your Excel file.
""")

# Sidebar settings
st.sidebar.header("Settings")
w_min = st.sidebar.slider("Min Delay (sec)", 0.5, 5.0, 1.0)
w_max = st.sidebar.slider("Max Delay (sec)", 1.0, 10.0, 3.0)

# File Upload
uploaded_file = st.file_uploader("Upload your urls.txt file", type=['txt'])

if uploaded_file:
    # Process text file
    content = uploaded_file.read().decode("utf-8")
    urls = [line.strip() for line in content.split('\n') if line.strip()]
    
    st.info(f"📋 Found {len(urls)} URLs to process.")
    
    if st.button("🚀 Start Download"):
        # Run Scraper
        df_results = scrape_urls(urls, w_min, w_max)
        
        st.success("✅ Finished!")
        
        # Show data preview
        st.subheader("Data Preview")
        st.write(df_results.drop(columns=["HTML Content"]).head(10)) # Hide heavy HTML in preview
        
        # Create Excel File in Memory
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_results.to_excel(writer, index=False, sheet_name='Scraped Data')
            
            # Formatting the Excel for better readability
            workbook  = writer.book
            worksheet = writer.sheets['Scraped Data']
            header_format = workbook.add_format({'bold': True, 'bg_color': '#D7E4BC'})
            worksheet.set_column('A:A', 50) # URL column width
            worksheet.set_column('E:E', 100) # HTML column width
            
        excel_data = output.getvalue()
        
        # Download Button
        st.download_button(
            label="📥 Download Results as Excel",
            data=excel_data,
            file_name=f"scrape_results_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
