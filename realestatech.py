import streamlit as st
from curl_cffi import requests
import time
import random
import json
import re
import pandas as pd
from datetime import datetime
import io
import concurrent.futures
import threading

# Set page config
st.set_page_config(
    page_title="Homegate Agency Data Extractor",
    page_icon="🏢",
    layout="wide"
)

class HomegateExtractor:
    def __init__(self, urls, proxies=None, max_workers=10, max_retries=2):
        self.urls = urls
        self.proxies = proxies if proxies else []
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.total_urls = len(urls)
        self.proxy_index = 0
        self.proxy_lock = threading.Lock()
        self.results = []
        self.failed_urls = []
        self.results_lock = threading.Lock()
        self.failed_lock = threading.Lock()
        self.stats = {
            'success': 0,
            'failed': 0,
            'captcha_detected': 0,
            'gone_count': 0,
            'rate_limited': 0
        }
        self.stats_lock = threading.Lock()
        
    def get_next_proxy(self):
        if not self.proxies:
            return None
        with self.proxy_lock:
            proxy = self.proxies[self.proxy_index]
            self.proxy_index = (self.proxy_index + 1) % len(self.proxies)
            return proxy
    
    def check_for_captcha(self, html_content):
        captcha_indicators = [
            'captcha', 'recaptcha', 'cf-challenge', 'cf-browser-verification',
            'cf-ray', 'access denied', 'please verify', 'robot check',
            'security check', 'captcha-delivery', 'turnstile', 'cloudflare',
            'ddos-guard', 'bot detection', 'your request has been blocked',
            'attention required', 'please complete the security check'
        ]
        
        html_lower = html_content.lower()
        for indicator in captcha_indicators:
            if indicator in html_lower:
                return True
        return False
    
    def check_for_gone(self, html_content):
        gone_indicators = [
            '410 gone', 'page not found', 'no longer available',
            'has been removed', 'does not exist', '404 not found',
            'error 404', 'error 410'
        ]
        
        html_lower = html_content.lower()
        for indicator in gone_indicators:
            if indicator in html_lower:
                return True
        return False
    
    def extract_from_html(self, html_content, url):
        try:
            pattern = r'window\.__INITIAL_STATE__\s*=\s*({.*?});'
            match = re.search(pattern, html_content, re.DOTALL)
            
            if match:
                json_str = match.group(1)
                json_str = re.sub(r',\s*}', '}', json_str)
                json_str = re.sub(r',\s*]', ']', json_str)
                
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
            
            name_match = re.search(r'<h1[^>]*>([^<]+)</h1>', html_content)
            phone_match = re.search(r'tel:([^"]+)"', html_content) or re.search(r'phone":\s*"([^"]+)"', html_content)
            email_match = re.search(r'mailto:([^"]+)"', html_content) or re.search(r'email":\s*"([^"]+)"', html_content)
            address_match = re.search(r'<address[^>]*>([^<]+)</address>', html_content)
            website_match = re.search(r'href="(https?://[^"]+)"[^>]*>Website', html_content)
            logo_match = (re.search(r'<img[^>]*data-test="agencyLogoImage"[^>]*src="([^"]+)"', html_content) or 
                         re.search(r'<img[^>]*class="[^"]*agency-logo[^"]*"[^>]*src="([^"]+)"', html_content))
            
            filename = url.split('/')[-1] if url.split('/')[-1] else 'index'
            
            return {
                'company_name': name_match.group(1).strip() if name_match else filename.replace('-', ' ').title(),
                'address': address_match.group(1).strip() if address_match else None,
                'phone': phone_match.group(1) if phone_match else None,
                'email': email_match.group(1) if email_match else None,
                'website': website_match.group(1) if website_match else None,
                'logo_url': logo_match.group(1) if logo_match else None,
                'agency_id': filename.split('_')[0] if '_' in filename else None,
                'source_url': url,
                'extract_method': 'fallback' if (name_match or phone_match or email_match) else 'minimal'
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
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    ]),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'DNT': '1',
                    'Connection': 'keep-alive',
                    'Upgrade-Insecure-Requests': '1',
                }
                
                time.sleep(random.uniform(0.5, 1.5))
                
                response = requests.get(
                    url, 
                    impersonate="chrome120", 
                    timeout=30, 
                    headers=headers,
                    proxies=proxy_dict,
                    verify=False
                )
                
                if response.status_code == 410:
                    with self.stats_lock:
                        self.stats['gone_count'] += 1
                    return None, "URL is gone (410)"
                
                if response.status_code != 200:
                    if response.status_code in [403, 429, 503]:
                        with self.stats_lock:
                            self.stats['rate_limited'] += 1
                    return None, f"HTTP {response.status_code}"
                
                if response.status_code == 200:
                    if self.check_for_gone(response.text):
                        with self.stats_lock:
                            self.stats['gone_count'] += 1
                        return None, "Content appears to be gone"
                    
                    if self.check_for_captcha(response.text):
                        with self.stats_lock:
                            self.stats['captcha_detected'] += 1
                    
                    company_data = self.extract_from_html(response.text, url)
                    if company_data:
                        return company_data, None
                    else:
                        return None, "Failed to extract data"
                    
            except Exception as e:
                if attempt == self.max_retries - 1:
                    return None, str(e)[:100]
                time.sleep(random.uniform(1, 2))
        
        return None, "Max retries exceeded"
    
    def process_url(self, url):
        data, error = self.download_page(url)
        
        if data:
            with self.results_lock:
                self.results.append(data)
            with self.stats_lock:
                self.stats['success'] += 1
            return True, url
        else:
            with self.failed_lock:
                self.failed_urls.append({'url': url, 'error': error})
            with self.stats_lock:
                self.stats['failed'] += 1
            return False, url
    
    def run(self, progress_callback=None):
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_url = {executor.submit(self.process_url, url): url for url in self.urls}
            completed = 0
            
            for future in concurrent.futures.as_completed(future_to_url):
                completed += 1
                success, url = future.result()
                if progress_callback:
                    progress_callback(completed, self.total_urls, url, success)
        
        return self.results, self.failed_urls, self.stats

# Streamlit UI
st.title("🏢 Homegate Agency Data Extractor")
st.markdown("Extract agency data directly to Excel - no HTML files saved")

# Sidebar
with st.sidebar:
    st.header("⚙️ Settings")
    
    # Worker threads
    max_workers = st.slider("Number of threads:", 1, 20, 10)
    
    # Input method
    input_method = st.radio(
        "Choose input method:",
        ["📝 Paste URLs", "📁 Upload file"]
    )
    
    urls = []
    if input_method == "📝 Paste URLs":
        urls_text = st.text_area(
            "Paste URLs (one per line):",
            height=200,
            placeholder="https://www.homegate.ch/agency/abc123\nhttps://www.homegate.ch/agency/xyz789"
        )
        if urls_text:
            urls = [u.strip() for u in urls_text.split('\n') if u.strip()]
    else:
        uploaded_file = st.file_uploader(
            "Upload a text file with URLs",
            type=['txt']
        )
        if uploaded_file:
            content = uploaded_file.getvalue().decode('utf-8')
            urls = [u.strip() for u in content.split('\n') if u.strip()]
    
    st.header("🔌 Proxy Settings")
    st.markdown("Example: `http://okbqhrtv-rotate:aa0kiwxlrvqk@p.webshare.io:80/`")
    proxy_option = st.radio("Proxy option:", ["No proxy", "Use proxies"])
    
    proxies = []
    if proxy_option == "Use proxies":
        proxy_input = st.text_area(
            "Enter proxies (one per line):", 
            height=100,
            placeholder="http://user:pass@host:port"
        )
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
    
    with st.expander("📋 View URLs"):
        for url in urls[:5]:
            st.code(url)
        if len(urls) > 5:
            st.info(f"... and {len(urls) - 5} more")
    
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        start_button = st.button("🚀 Start Extraction", type="primary", use_container_width=True)
    
    if start_button:
        extractor = HomegateExtractor(
            urls=urls,
            proxies=proxies if proxy_option == "Use proxies" else [],
            max_workers=max_workers,
            max_retries=max_retries
        )
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        stats_text = st.empty()
        log_container = st.container()
        
        def update_progress(current, total, current_url, success):
            progress_bar.progress(current / total)
            status_text.text(f"Processing: {current}/{total}")
            stats_text.markdown(f"""
            ✅ Success: {extractor.stats['success']} | ❌ Failed: {extractor.stats['failed']} | 📪 Gone: {extractor.stats['gone_count']} | ⚠️ Captcha: {extractor.stats['captcha_detected']}
            """)
            with log_container:
                if success:
                    st.success(f"✅ {current_url[:80]}...")
                else:
                    st.error(f"❌ {current_url[:80]}...")
        
        with st.spinner("Extracting data..."):
            results, failed, stats = extractor.run(progress_callback=update_progress)
        
        progress_bar.progress(1.0)
        status_text.text("✅ Complete!")
        
        st.markdown("---")
        col1, col2, col3, col4, col5 = st.columns(5)
        with col1:
            st.metric("Total URLs", len(urls))
        with col2:
            st.metric("Success", stats['success'])
        with col3:
            st.metric("Failed", stats['failed'])
        with col4:
            st.metric("Gone (410)", stats['gone_count'])
        with col5:
            st.metric("Captcha", stats['captcha_detected'])
        
        if results:
            df = pd.DataFrame(results)
            
            # Create Excel file
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Agencies')
                
                # Auto-adjust column widths
                worksheet = writer.sheets['Agencies']
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            output.seek(0)
            
            st.markdown("### 📥 Download Results")
            st.download_button(
                label="📊 Download Excel File",
                data=output,
                file_name=f"homegate_agencies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
            
            with st.expander("👁️ Preview Extracted Data"):
                st.dataframe(df[['company_name', 'phone', 'email', 'website']].head(10), use_container_width=True)
            
            if failed:
                with st.expander("❌ Failed URLs"):
                    failed_df = pd.DataFrame(failed)
                    st.dataframe(failed_df, use_container_width=True)
            
        else:
            st.error("❌ No data was extracted successfully!")
            
else:
    st.markdown("### 👋 Welcome!")
    st.markdown("")
    st.markdown("This tool extracts agency data directly from Homegate.ch URLs and provides an Excel file.")
    st.markdown("")
    st.markdown("**Features:**")
    st.markdown("- Multi-threaded extraction (up to 20 threads)")
    st.markdown("- Proxy support with rotation")
    st.markdown("- Tracks captcha, gone URLs, and rate limiting")
    st.markdown("- Direct Excel download - no files saved on server")
    st.markdown("")
    st.markdown("**How to use:**")
    st.markdown("1. Paste your URLs or upload a text file")
    st.markdown("2. Configure proxy settings (optional)")
    st.markdown("3. Adjust thread count for speed")
    st.markdown("4. Click 'Start Extraction'")
    st.markdown("5. Download your Excel file")
    st.markdown("")
    st.markdown("**Example URL:**")
    st.code("https://www.homegate.ch/agency/abc123")
    st.code("https://www.homegate.ch/agency/xyz789/company-name")
    st.markdown("")
    st.markdown("**Proxy example:**")
    st.code("http://okbqhrtv-rotate:aa0kiwxlrvqk@p.webshare.io:80/")

st.markdown("---")
st.markdown("⚡ Powered by multi-threaded extraction")
