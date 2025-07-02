# AI Summary Service

This service generates AI-powered summaries for security clusters using OpenAI's GPT-4-mini model. It creates three tailored briefs for different audiences: executives, technical professionals, and remediation teams.

## Setup

1. **Set OpenAI API Key**:
   ```bash
   export OPENAI_API_KEY="your-api-key-here"
   ```
   Or add to your `.env` file:
   ```
   OPENAI_API_KEY=your-api-key-here
   ```

2. **Run Database Migration**:
   ```bash
   psql $DATABASE_URL -f src/database/sql/add_ai_summary_columns.sql
   ```

3. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Run Once
Process clusters without AI summaries once and exit:
```bash
python -m src.ai_summary.ai_summary_scheduler --once
```

### Run as Daemon
Run continuously, checking every hour for clusters to process:
```bash
python -m src.ai_summary.ai_summary_scheduler
```

### Test Mode
Process just one cluster as a test:
```bash
python -m src.ai_summary.ai_summary_scheduler --test --once
```

### Custom Configuration
```bash
# Process 20 clusters every 30 minutes
python -m src.ai_summary.ai_summary_scheduler --batch-size 20 --interval 1800
```

## How It Works

1. **Hourly Scan**: The scheduler checks for clusters where `has_ai_summary = FALSE`
2. **Article Aggregation**: Collects up to 10 articles per cluster (max 500 chars each)
3. **AI Processing**: Sends content to OpenAI GPT-4-mini with cybersecurity-specific prompt
4. **Three Briefs Generated**:
   - **Executive Brief**: Strategic implications and business risk
   - **Technical Brief**: Vulnerability details and attack vectors
   - **Remediation Brief**: Actionable mitigation steps
5. **Storage**: Saves briefs as JSONB in `ai_summary` column
6. **Flag Update**: Sets `has_ai_summary = TRUE`

## Output Format

The AI summary is stored as JSONB with this structure:
```json
{
    "executive_brief": "High-level summary for leadership...",
    "technical_brief": "Technical details including CVEs...",
    "remediation_brief": "Actionable steps for mitigation..."
}
```

Each brief is limited to 500 characters for concise, focused communication.

## Monitoring

Logs are written to:
- Console output
- `logs/ai_summary_scheduler_YYYYMMDD.log`

Monitor processing with:
```bash
tail -f logs/ai_summary_scheduler_$(date +%Y%m%d).log
```

## Rate Limiting

- Processes up to 10 clusters per run by default
- 1-second delay between OpenAI API calls
- Configurable batch size and interval

## Error Handling

- Retries failed API calls up to 3 times
- Graceful handling of missing articles
- Continues processing even if individual clusters fail
- Detailed error logging for troubleshooting