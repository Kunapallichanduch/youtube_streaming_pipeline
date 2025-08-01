Added logging + error retry logic
# Change Log – YouTube Live Data Streaming Pipeline

##
- Initial commit: YouTube data extraction script
- Connected to YouTube Data API
- Parsed video metadata (title, views, likes, comments)

##
- Added retry mechanism with exponential backoff for API failures
- Introduced error logging using Python `logging` module
- Prepared structure for future CI/CD pipeline integration
- Planned future integration to Azure Blob / SQL
