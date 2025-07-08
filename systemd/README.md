# ThreatCluster Email Service Systemd Setup

## Installation

1. Copy the service file to systemd directory:
```bash
sudo cp threatcluster-email.service /etc/systemd/system/
```

2. Create the threatcluster user (if not already exists):
```bash
sudo useradd -r -s /bin/false threatcluster
```

3. Set proper ownership:
```bash
sudo chown -R threatcluster:threatcluster /opt/threatcluster/cluster-service
```

4. Create log directory:
```bash
sudo mkdir -p /opt/threatcluster/cluster-service/logs
sudo chown threatcluster:threatcluster /opt/threatcluster/cluster-service/logs
```

5. Reload systemd and enable the service:
```bash
sudo systemctl daemon-reload
sudo systemctl enable threatcluster-email.service
```

6. Start the service:
```bash
sudo systemctl start threatcluster-email.service
```

## Management Commands

Check service status:
```bash
sudo systemctl status threatcluster-email.service
```

View logs:
```bash
sudo journalctl -u threatcluster-email -f
```

Stop service:
```bash
sudo systemctl stop threatcluster-email.service
```

Restart service:
```bash
sudo systemctl restart threatcluster-email.service
```

## Configuration

The service expects:
- Python virtual environment at `/opt/threatcluster/cluster-service/venv`
- Application code at `/opt/threatcluster/cluster-service`
- `.env` file with required configuration in `/opt/threatcluster/cluster-service/.env`

Required environment variables in `.env`:
- `POSTMARK_API_TOKEN` - Postmark API token for sending emails
- `EMAIL_FROM_ADDRESS` - From email address
- `DAILY_EMAIL_SEND_TIME` - Time to send daily emails (24-hour format, e.g., "09:00")
- Database connection settings
- OpenAI API key