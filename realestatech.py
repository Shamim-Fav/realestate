import streamlit as st
from curl_cffi import requests
import time
import random
from pathlib import Path
import tempfile
import os
import json
import re
import pandas as pd
import zipfile
from datetime import datetime
import io

# Set page config
st.set_page_config(
    page_title="Homegate Agency Scraper",
    page_icon="🏢",
    layout="wide"
)

# Custom CSS
st.markdown("""
<style>
    .stProgress > div > div > div > div {
        background-color: #00cc66;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .warning-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #fff3cd;
        border: 1px solid #ffeeba;
        color: #856404;
    }
</style>
""", unsafe_allow_html=True)

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
    
    def get_filename(self, url):
        parts = url.rstrip('/').split('/')
        filename = parts[-1] if parts[-1] else 'index'
        return filename
    
    def extract_company_data(self, html_content, url):
        """Extract company data from HTML"""
        try:
            # Try JSON extraction first
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
            address_match = re.search(r'<address[^>]*>([^<]+)</address>', html_content)
            website_match = re.search(r'href="(https?://[^"]+)"[^>]*>Website', html_content)
            logo_match = (re.search(r'<img[^>]*data-test="agencyLogoImage"[^>]*src="([^"]+)"', html_content) or 
                         re.search(r'<img[^>]*class="[^"]*agency-logo[^"]*"[^>]*src="([^"]+)"', html_content))
            
            # Extract agency ID from URL
            agency_id = None
            id_match = re.search(r'/agency/([^/]+)', url)
            if id_match:
                agency_id = id_match.group(1)
            
            return {
                'company_name': name_match.group(1).strip() if name_match else self.get_filename(url).replace('-', ' ').title(),
                'address': address_match.group(1).strip() if address_match else None,
                'phone': phone_match.group(1) if phone_match else None,
                'email': email_match.group(1) if email_match else None,
                'website': website_match.group(1) if website_match else None,
                'logo_url': logo_match.group(1) if logo_match else None,
                'agency_id': agency_id,
                'source_url': url,
                'extract_method': 'fallback'
            }
            
        except Exception as e:
            return None
    
    def download_page(self, url):
        """Download and extract data from a single page"""
        for attempt in range(self.max_retries):
            proxy_url = self.get_next_proxy() if self.proxies else None
            proxy_dict = {"http": proxy_url, "https": proxy_url} if proxy_url else None
            
            try:
                headers = {
                    'User-Agent': random.choice([
                        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                    ]),
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
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
                    # Extract data directly
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
        """Run the scraper"""
        for i, url in enumerate(self.urls):
            # Download and extract
            data, error = self.download_page(url)
            
            if data:
                self.results.append(data)
                self.success += 1
            else:
                self.failed_urls.append({
                    'url': url,
                    'error': error
                })
                self.failed += 1
            
            # Update progress
            if progress_callback:
                progress_callback(i + 1, self.total_urls, url, data is not None)
        
        return self.results, self.failed_urls

# Streamlit UI
st.title("🏢 Homegate Agency Scraper")
st.markdown("Extract agency data from Homegate.ch URLs and get an Excel file instantly")

# Sidebar
with st.sidebar:
    st.header("⚙️ Settings")
    
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
            type=['txt'],
            help="Text file with one URL per line"
        )
        if uploaded_file:
            content = uploaded_file.getvalue().decode('utf-8')
            urls = [u.strip() for u in content.split('\n') if u.strip()]
    
    # Proxy settings
    st.header("🔌 Proxy Settings")
    proxy_option = st.radio(
        "Proxy option:",
        ["No proxy", "Use proxies"]
    )
    
    proxies = []
    if proxy_option == "Use proxies":
        proxy_input = st.text_area(
            "Enter proxies (one per line):",
            height=150,
            placeholder="http://user:pass@host:port\nhost:port:user:pass\nhost:port",
            help="Supported formats:\n- http://user:pass@host:port\n- host:port:user:pass\n- host:port"
        )
        if proxy_input:
            for line in proxy_input.split('\n'):
                line = line.strip()
                if line and not line.startswith('#'):
                    proxies.append(line)
    
    # Advanced settings
    with st.expander("🔧 Advanced"):
        max_retries = st.slider("Max retries per URL:", 1, 5, 2)
        delay = st.slider("Delay between requests (seconds):", 0.5, 3.0, 1.0, 0.5)

# Main content
if urls:
    st.success(f"✅ Loaded {len(urls)} URLs")
    
    # Sample of URLs
    with st.expander("📋 View URLs"):
        for url in urls[:5]:
            st.code(url)
        if len(urls) > 5:
            st.info(f"... and {len(urls) - 5} more")
    
    # Start button
    col1, col2, col3 = st.columns([1, 1, 1])
    with col2:
        start_button = st.button("🚀 Start Scraping", type="primary", use_container_width=True)
    
    if start_button:
        # Initialize scraper
        scraper = HomegateScraper(
            urls=urls,
            proxies=proxies if proxy_option == "Use proxies" else [],
            max_retries=max_retries
        )
        
        # Progress tracking
        progress_bar = st.progress(0)
        status_text = st.empty()
        stats_text = st.empty()
        
        # Results containers
        col1, col2 = st.columns(2)
        with col1:
            success_container = st.empty()
        with col2:
            failed_container = st.empty()
        
        # Live log
        log_container = st.container()
        
        def update_progress(current, total, current_url, success):
            # Update progress bar
            progress_bar.progress(current / total)
            
            # Update status
            status_text.text(f"Processing: {current}/{total}")
            
            # Update stats
            stats_text.markdown(f"""
            <div style='display: flex; gap: 2rem; margin: 1rem 0;'>
                <span style='color: #00cc66;'>✅ Success: {len(scraper.results)}</span>
                <span style='color: #ff4444;'>❌ Failed: {len(scraper.failed_urls)}</span>
            </div>
            """, unsafe_allow_html=True)
            
            # Show current URL
            with log_container:
                if success:
                    st.success(f"✅ {current_url[:80]}...")
                else:
                    st.error(f"❌ {current_url[:80]}...")
        
        # Run scraper
        with st.spinner("Scraping in progress..."):
            results, failed = scraper.run(progress_callback=update_progress)
        
        # Complete
        progress_bar.progress(1.0)
        status_text.text("✅ Complete!")
        
        # Show summary
        st.markdown("---")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total URLs", len(urls))
        with col2:
            st.metric("Success", len(results), delta_color="normal")
        with col3:
            st.metric("Failed", len(failed), delta_color="inverse")
        
        # Create Excel file
        if results:
            df = pd.DataFrame(results)
            
            # Reorder columns
            columns = ['company_name', 'source_url', 'logo_url', 'phone', 'email', 'website',
                      'address', 'agency_id', 'extract_method']
            existing_cols = [col for col in columns if col in df.columns]
            other_cols = [col for col in df.columns if col not in columns]
            df = df[existing_cols + other_cols]
            
            # Create Excel file in memory
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
            
            # Download buttons
            st.markdown("### 📥 Download Results")
            
            col1, col2 = st.columns(2)
            
            # Excel download
            with col1:
                st.download_button(
                    label="📊 Download Excel",
                    data=output,
                    file_name=f"homegate_agencies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            
            # CSV download
            with col2:
                csv = df.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📄 Download CSV",
                    data=csv,
                    file_name=f"homegate_agencies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            # Show data preview
            st.markdown("### 👁️ Data Preview")
            st.dataframe(
                df[['company_name', 'phone', 'email', 'website', 'extract_method']].head(10),
                use_container_width=True
            )
            
            # Show failed URLs if any
            if failed:
                with st.expander("❌ Failed URLs"):
                    failed_df = pd.DataFrame(failed)
                    st.dataframe(failed_df, use_container_width=True)
            
            # Summary statistics
            st.markdown("### 📊 Summary")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric("Companies with Phone", df['phone'].notna().sum())
            with col2:
                st.metric("Companies with Email", df['email'].notna().sum())
            with col3:
                st.metric("Companies with Website", df['website'].notna().sum())
            with col4:
                st.metric("Companies with Logo", df['logo_url'].notna().sum())
            
            # Extraction method breakdown
            st.markdown("**Extraction Method:**")
            method_counts = df['extract_method'].value_counts()
            for method, count in method_counts.items():
                st.markdown(f"- {method}: {count} ({count/len(df)*100:.1f}%)")
            
        else:
            st.error("❌ No data was extracted successfully!")
            
else:
    # Welcome message - FIXED: properly closed triple quotes
    st.markdown("""
    ### 👋 Welcome!
    
    This tool extracts agency data from Homegate.ch URLs and provides an Excel file with:
    
    - **Company Information**: Name, address, phone, email, website
    - **Media**: Logo URLs
    - **Metadata**: Agency ID, extraction method
    
    ### How to use:
    1. Choose input method (paste URLs or upload file)
    2. Configure proxy settings (optional)
    3. Click "Start Scraping"
    4. Download your Excel file
    
    ### Example URL format:
