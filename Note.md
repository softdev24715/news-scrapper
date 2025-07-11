
Project Plan: Parsing System for Monitoring Legislation and News
1. Project Goal
The objective of this project is to develop an automated parsing system for the regular collection and storage of data from legislative and news websites. This data will be used for internal analytics and reporting within the scope of Government Relations (GR).
2. Scope of Work
The project includes the creation of parsers for the following sources:
Legislative portals (bills and regulations) – 4 websites:
 • regulation.gov.ru – Public discussion portal
 • pravo.gov.ru – Official legal information portal
 • sozd.duma.gov.ru – State Duma legislative activity portal
 • docs.eaeunion.org – EAEU documentation  // impossible
News portals – 16 websites, implemented in two stages
Stage 1 (8 websites):
1.	iz.ru – Izvestia

2.	kommersant.ru – Kommersant

3.	vedomosti.ru – Vedomosti
4.	rbc.ru – RBC

5.	lenta.ru – Lenta.ru

6.	tass.ru – TASS

7.	interfax.ru – Interfax  //

8.	ria.ru – RIA Novosti  //
Stage 2 (8 websites):
 9. rg.ru – Rossiyskaya Gazeta
 10. gazeta.ru – Gazeta.ru
 11. meduza.io – Meduza //impossible
 12. kremlin.ru – Official website of the President of Russia  //impossible
 13. government.ru – Official website of the Government of Russia  //impossible
 14. forbes.ru – Forbes Russia
 15. pnp.ru – Parliamentary Newspaper
 16. graininfo.ru – Grain Market News
3. System Requirements
• Frequency: Run every hour or on demand via API (cron job or API trigger)
 • Output: Save data to PostgreSQL database including:
 – Full text of the article/news/law
 – Source URL
 – Title
 – First discovery timestamp
• Technology Stack:
 – Language: Python (preferred)
 – Hosting: Google Cloud
 – Database: PostgreSQL
 – Libraries: BeautifulSoup, Scrapy
 – May require use of Russian IP addresses for local access
4. Implementation Plan
• Stage 1: Develop parsers for the first 5 news websites and validate data integrity
 • Stage 2: Add the remaining 6 news websites and all legislative parsers
 • Data processing and storage:
 – Clean and structure data into database tables
 – Implement error handling and logging
 • API development:
 – Create API for on-demand parsing and data access
5. Testing and Validation
• Unit testing of each parser for data accuracy
 • End-to-end testing for system stability
 • Load testing to ensure reliability with frequent execution
6. Deployment and Support
• Deployment to Google Cloud using CI/CD pipelines
 • Scheduled automation via cron
 • Performance monitoring and alert setup
 • Simple web interface for control and monitoring (example will be provided)
 
8. Risks and Mitigation
• Access issues: Use of robust proxy management
 • Website structure changes: Continuous monitoring and parser updates
 • Anti-scraping measures:
 – IP rotation, including use of residential IPs (e.g., via Oxylabs)
 – Cookie handling
 – CAPTCHA bypass
9. Conclusion
This parsing system will enable efficient and automated data collection from legislative and news sources, providing a reliable foundation for an AI-powered GR analytics platform. With stable and accurate data extraction, the system will significantly enhance decision-making and reporting processes.



{
  "id": uuid,
  "text": ,
  "metadata": {
    "source": ,
    "published_at": ,
    "published_at_iso": ,
    "url": ,
    "header": ,
    "parsed_at": ,
  }
}



{
  "id": "2384451b-c8db-4c12-86a4-1eacb9382c3e",
  "text": "Draft text of the regulatory legal act,",
  "lawMetadata": {
    "originalId": "157552",
    "docKind": "bill",
    "title": "On the approval of industry-specific features of categorizing critical information infrastructure objects in the field of nuclear energy",
    "source": "regulation.gov.ru",
    "url": "https://regulation.gov.ru/Regulation/Npa/PublicView?npaID=157552",
    "publishedAt": 1738627200,
    "parsedAt": 1738692000,
    "jurisdiction": "RU",
    "language": "ru",
    "stage": "public_discussion",
    "discussionPeriod": {
      "start": 1738627200,
      "end": 1739654400   // 2025-07-18 T00:00:00Z
    },
    "explanatoryNote": {
      "fileId": "f90c1e…",
      "url": "https://regulation.gov.ru/Files/GetFile?fileid=f90c1e…",
      "mimeType": "application/pdf"
    },
    "summaryReports": [],
    "commentStats": { "total": 137 },
    "files":{}
  }
}

sudo journalctl -u news-scraper -f



https://docs.eaeunion.org/documents/?PAGEN_1=1