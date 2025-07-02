# Real-Time Data Saving

This project now supports **real-time data saving** - data is saved to the database one by one as it's scraped, rather than waiting until the entire scraping process is finished.

## 🚀 Benefits

### ✅ **Fault Tolerance**
- If the scraping process crashes or is interrupted, you don't lose all the data that was already scraped
- Each item is saved immediately after processing

### ✅ **Memory Efficiency**
- No need to hold all scraped data in memory
- Reduces memory usage for large scraping jobs

### ✅ **Real-Time Monitoring**
- See progress in real-time as items are saved
- Better visibility into the scraping process

### ✅ **Better Debugging**
- If there's an issue with one item, it doesn't affect others
- Easier to identify and fix problems

## 🔧 Configuration

The real-time saving is now **enabled by default**. The configuration is in `news_parser/settings.py`:

```python
ITEM_PIPELINES = {
   "news_parser.pipelines.PostgreSQLPipeline": 400,
}
```

## 📊 How It Works

1. **Item Processing**: Each scraped item is processed immediately
2. **Database Save**: Item is saved to the database right away
3. **Progress Tracking**: Real-time statistics are logged
4. **Error Handling**: Individual item failures don't stop the process

## 🧪 Testing the Feature

### Option 1: Run the Test Script
```bash
cd news_parser
python test_realtime_saving.py
```

### Option 2: Run a Spider Manually
```bash
cd news_parser
scrapy crawl forbes -L INFO
```

### Option 3: Monitor Database in Real-Time
In a separate terminal, run:
```bash
cd news_parser
python monitor_database.py
```

## 📈 What You'll See

### In the Spider Logs:
```
✅ Saved news article: forbes - https://www.forbes.ru/news/...
✅ Saved legal document: docs.eaeunion.org - https://docs.eaeunion.org/documents/...
🔄 Duplicate news article: forbes - https://www.forbes.ru/news/...
📊 Spider forbes final stats:
   - Items processed: 15
   - Items saved: 12
   - Items failed: 0
   - Duplicates found: 3
```

### In the Database Monitor:
```
[14:30:15] 🆕 New items detected:
   📰 Articles: +2 (Total: 45)
   📝 Recent items:
      • ARTICLE: forbes - https://www.forbes.ru/news/...
      • LEGAL: docs.eaeunion.org - https://docs.eaeunion.org/documents/...
```

## 🔄 Switching Back to Batch Saving

If you want to go back to the old batch saving method (saving all at the end), change the settings:

```python
# In news_parser/settings.py
ITEM_PIPELINES = {
   "news_parser.pipelines.NewsParserPipeline": 300,  # Batch saving to JSON
   # "news_parser.pipelines.PostgreSQLPipeline": 400,  # Comment out real-time saving
}
```

## 📋 Pipeline Features

### PostgreSQLPipeline (Real-Time)
- ✅ Saves items one by one to database
- ✅ Real-time progress tracking
- ✅ Duplicate detection and handling
- ✅ Comprehensive error handling
- ✅ Detailed logging with emojis
- ✅ Spider status updates

### NewsParserPipeline (Batch)
- ✅ Collects all items in memory
- ✅ Saves to JSON file at the end
- ✅ Duplicate URL tracking
- ✅ File-based output

## 🛠️ Customization

### Adjust Logging Frequency
In `pipelines.py`, you can modify how often progress is logged:

```python
# Log progress every 10 items (default)
if self.items_saved % 10 == 0:
    logging.info(f"Progress update...")

# Update spider status every 5 items (default)
if self.items_saved % 5 == 0:
    # Update spider status
```

### Add Custom Metrics
You can add your own tracking metrics in the pipeline:

```python
def __init__(self, db_url):
    self.db_url = db_url
    self.session = None
    self.items_processed = 0
    self.items_saved = 0
    self.items_failed = 0
    self.duplicates_found = 0
    # Add your custom metrics here
    self.custom_metric = 0
```

## 🚨 Troubleshooting

### Database Connection Issues
- Check your `DATABASE_URL` in `settings.py`
- Ensure the database is running and accessible
- Verify network connectivity

### Performance Issues
- If saving is too slow, consider batch processing
- Monitor database performance
- Check for database locks or connection limits

### Memory Issues
- Real-time saving should reduce memory usage
- If you still have issues, check for memory leaks in spiders

## 📞 Support

If you encounter any issues with the real-time saving feature:

1. Check the logs for detailed error messages
2. Verify database connectivity
3. Test with a small dataset first
4. Check the spider status in the database

---

**Happy Scraping! 🕷️✨** 