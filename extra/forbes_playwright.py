import asyncio
from playwright.async_api import async_playwright
from playwright_stealth import Stealth

async def main():
    async with Stealth().use_async(async_playwright()) as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()
        # Your scraping code here
        await page.goto("https://www.forbes.ru/newrss.xml")
        content = await page.content()
        print(content)
        await browser.close()

asyncio.run(main())
