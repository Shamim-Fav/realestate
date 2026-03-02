import streamlit as st
from curl_cffi import requests
import time
import random
import json
import re
import pandas as pd
from datetime import datetime
import io

# Set page config
st.set_page_config(
    page_title="Homegate Agency Scraper",
    page_icon="🏢",
    layout="wide"
)

class HomegateScraper:
    def __init__(self, urls, proxies=None, max_retries=2):
        self.urls = urls
        self.proxies = proxies if proxies else []
        self.max_retries = max_retries
        self.total_urls = len(urls)
        self.proxy_index = 0
        self.results = []
        self.failed_urls = []
        self.success = 0
        self.failed = 0
        
    def get_next_proxy(self):
        if not self.proxies:
            return None
        proxy = self.proxies[self.proxy_index]
        self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
        return proxy
    
    def extract_company_data(self, html_content, url):
        try:
            # Try JSON extraction
            pattern = r'window\.__INITIAL_STATE__\s*=\s*({.*?});'
            match = re.search(pattern, html_content, re.DOTALL)
            
            if match:
                json_str = match.group(1)
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
                
                # Find valid JSON end
                stack = []
                valid_end = 0
                for i, char in enumerate(json_str):
                    if char == '{':
                        stack.append(char)
                    elif char == '}':
                        if stack:
                            stack.pop()
                            if not stack:
                                valid_end = i + 1
                                break
                
                if valid_end > 0:
                    json_str = json_str[:valid_end]
                
                data = json.loads(json_str)
                agency = data.get('agencyProfile', {}).get('agencyProfileFetch', {}).get('result', {})
                
                if agency:
                    contact = agency.get('contact', {})
                    address = agency.get('address', {})
                    
                    address_parts = []
                    if address.get('street'):
                        address_parts.append(address['street'])
                    if address.get('postalCode') or address.get('city'):
                        addr = f"{address.get('postalCode', '')} {address.get('city', '')}".strip()
                        if addr:
                            address_parts.append(addr)
                    
                    return {
                        'company_name': agency.get('agencyName'),
                        'address': ', '.join(address_parts) if address_parts else None,
                        'phone': contact.get('phone') or agency.get('customPhoneNumber'),
                        'email': contact.get('email'),
                        'website': contact.get('website'),
                        'logo_url': agency.get('logo'),
                        'agency_id': agency.get('agencyId'),
                        'source_url': url,
                        'extract_method': 'json'
                    }
            
            # Fallback extraction
            name_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html_content)
            phone_match = re.search(r'tel:([^"]+)"', html_content) or re.search(r'phone":\s*"([^"]+)"', html_content)
            email_match = re.search(r'mailto:([^"]+)"', html_content) or re.search(r'email":\s*"([^"]+)"', html_content)
            
            return {
                'company_name': name_match.group(1).strip() if name_match else "Unknown",
                'address': None,
                'phone': phone_match.group(1) if phone_match else None,
                'email': email_match.group(1) if email_match else None,
                'website': None,
                'logo_url': None,
                'agency_id': None,
                'source_url': url,
                'extract_method': 'fallback'
            }
            
        except Exception as e:
            return None
    
    def download_page(self, url):
        for attempt in range(self.max_retries):
            proxy_url = self.get_next_proxy() if self.proxies else None
            proxy_dict = {"http": proxy_url, "https": proxy_url} if proxy_url else None
            
            try:
                headers = {
                    'User-Agent': random.choice([
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    ])
                }
                
                response = requests.get(
                    url, 
                    impersonate="chrome120", 
                    timeout=30, 
                    headers=headers,
                    proxies=proxy_dict,
                    verify=False
                )
                
                if response.status_code == 200:
                    company_data = self.extract_company_data(response.text, url)
                    if company_data:
                        return company_data, None
                    else:
                        return None, "Failed to extract data"
                else:
                    return None, f"HTTP {response.status_code}"
                    
            except Exception as e:
                if attempt == self.max_retries - 1:
                    return None, str(e)[:100]
                time.sleep(random.uniform(1, 2))
        
        return None, "Max retries exceeded"
    
    def run(self, progress_callback=None):
        for i, url in enumerate(self.urls):
            data, error = self.download_page(url)
            
            if data:
                self.results.append(data)
                self.success += 1
            else:
                self.failed_urls.append({'url': url, 'error': error})
                self.failed += 1
            
            if progress_callback:
                progress_callback(i + 1, self.total_urls, url, data is not None)
        
        return self.results, self.failed_urls

# Streamlit UI
st.title("🏢 Homegate Agency Scraper")

# Sidebar
with st.sidebar:
    st.header("⚙️ Settings")
    
    input_method = st.radio("Choose input method:", ["📝 Paste URLs", "📁 Upload file"])
    
    urls = []
    if input_method == "📝 Paste URLs":
        urls_text = st.text_area("Paste URLs (one per line):", height=200)
        if urls_text:
            urls = [u.strip() for u in urls_text.split('\n') if u.strip()]
    else:
        uploaded_file = st.file_uploader("Upload a text file with URLs", type=['txt'])
        if uploaded_file:
            content = uploaded_file.getvalue().decode('utf-8')
            urls = [u.strip() for u in content.split('\n') if u.strip()]
    
    proxy_option = st.radio("Proxy option:", ["No proxy", "Use proxies"])
    
    proxies = []
    if proxy_option == "Use proxies":
        proxy_input = st.text_area("Enter proxies (one per line):", height=150)
        if proxy_input:
            for line in proxy_input.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    proxies.append(line)
    
    with st.expander("🔧 Advanced"):
        max_retries = st.slider("Max retries per URL:", 1, 5, 2)

# Main content
if urls:
    st.success(f"✅ Loaded {len(urls)} URLs")
    
    if st.button("🚀 Start Scraping", type="primary"):
        scraper = HomegateScraper(
            urls=urls,
            proxies=proxies if proxy_option == "Use proxies" else [],
            max_retries=max_retries
        )
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        stats_text = st.empty()
        log_container = st.container()
        
        def update_progress(current, total, current_url, success):
            progress_bar.progress(current / total)
            status_text.text(f"Processing: {current}/{total}")
            stats_text.markdown(f"✅ Success: {len(scraper.results)} | ❌ Failed: {len(scraper.failed_urls)}")
            with log_container:
                if success:
                    st.success(f"✅ {current_url[:80]}...")
                else:
                    st.error(f"❌ {current_url[:80]}...")
        
        with st.spinner("Scraping in progress..."):
            results, failed = scraper.run(progress_callback=update_progress)
        
        progress_bar.progress(1.0)
        status_text.text("✅ Complete!")
        
        if results:
            df = pd.DataFrame(results)
            
            # Create Excel file
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Agencies')
            output.seek(0)
            
            st.download_button(
                label="📊 Download Excel",
                data=output,
                file_name=f"homegate_agencies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            
            st.dataframe(df.head(10))
            
        else:
            st.error("❌ No data was extracted successfully!")
            
else:
    # Simple welcome message - FIXED: properly closed triple quotes
    welcome_message = """
    ### 👋 Welcome!
    
    Enter your Homegate agency URLs to extract data.
    
    **Example:**
