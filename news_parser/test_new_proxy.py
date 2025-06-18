import requests
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_proxy():
    test_urls = [
        # 'http://kremlin.ru/events/president/news',
        'http://tass.ru/',
        'http://ria.ru/',
        'http://rbc.ru/',
        'http://kommersant.ru/',
        'http://lenta.ru/',
        'http://gazeta.ru/',
        'http://graininfo.ru/',
        'http://vedomosti.ru/',
        'http://iz.ru/',
        'http://interfax.ru/',
        'http://forbes.ru/',
        'http://rg.ru/',
        'http://pnp.ru/',
        'http://meduza.io/',
        'http://government.ru/news/',
    ]
    
    proxy = {
        'http': 'http://googlecompute:xd23rXPEmq2+23@90.156.202.84:3128',
        'https': 'http://googlecompute:xd23rXPEmq2+23@90.156.202.84:3128'
    }
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive'
    }
    
    try:
        # First test proxy connection
        logger.info("Testing proxy connection...")
        ip_response = requests.get('http://httpbin.org/ip', proxies=proxy, headers=headers, timeout=10)
        logger.info(f"Proxy IP test response: {ip_response.text}")
        
        # Test each URL
        for url in test_urls:
            try:
                logger.info(f"\nTesting URL: {url}")
                response = requests.get(url, proxies=proxy, headers=headers, timeout=15)
                logger.info(f"Response status code: {response.status_code}")
                
                # Save the response
                filename = f'test_response_{url.split("//")[1].split("/")[0]}.html'
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"Saved response to {filename}")
                
            except requests.exceptions.Timeout:
                logger.error(f"Timeout error for {url}")
            except requests.exceptions.RequestException as e:
                logger.error(f"Request error for {url}: {str(e)}")
            except Exception as e:
                logger.error(f"Unexpected error for {url}: {str(e)}")
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")

if __name__ == '__main__':
    test_proxy() 