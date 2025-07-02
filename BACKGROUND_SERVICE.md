# ThreatCluster Background Service

This document explains how to run ThreatCluster as a persistent background service on your DigitalOcean droplet or any Linux server.

## Problem Statement

The default `python -m src.main` command runs an interactive CLI that terminates when your SSH session closes. For continuous operation on a server, you need a background service that:

- Survives SSH disconnections
- Automatically starts on system boot
- Can be managed with standard service commands
- Provides proper logging and monitoring
- Handles graceful shutdowns

## Solution Overview

ThreatCluster now provides multiple ways to run as a background service:

1. **Systemd Service** (Recommended) - Production-ready Linux service
2. **Daemon Mode** - Simple background processing
3. **Screen/Tmux** - Session-based approach
4. **Nohup** - Basic background execution

## Quick Start (Systemd Service)

### 1. Install Dependencies

```bash
cd /path/to/cluster-service
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Configure Environment

```bash
# Copy and edit environment file
cp .env.example .env
nano .env  # Add your database credentials
```

### 3. Install Service

```bash
# Run the installation script
./scripts/install-service.sh
```

### 4. Start Service

```bash
# Start the service
sudo systemctl start threatcluster

# Check status
sudo systemctl status threatcluster

# View logs
./scripts/threatcluster-ctl.sh logs
```

## Detailed Installation

### Prerequisites

- Linux system with systemd (Ubuntu 16.04+, CentOS 7+, Debian 8+)
- Python 3.8+ with virtual environment
- PostgreSQL database configured
- Sudo privileges for service installation

### Step-by-Step Installation

1. **Prepare the Environment**
   ```bash
   cd /home/james/Desktop/Threatcluster-2/cluster-service
   
   # Create virtual environment if not exists
   python3 -m venv venv
   source venv/bin/activate
   
   # Install dependencies
   pip install -r requirements.txt
   
   # Set up configuration
   cp .env.example .env
   nano .env  # Configure database settings
   ```

2. **Test the Installation**
   ```bash
   # Run the installation test
   ./scripts/test-installation.sh
   
   # Test daemon mode (optional)
   python -m src.daemon --once --debug
   ```

3. **Install as System Service**
   ```bash
   # Run the installation script
   ./scripts/install-service.sh
   ```

4. **Start and Enable Service**
   ```bash
   # Start immediately
   sudo systemctl start threatcluster
   
   # Enable for automatic startup
   sudo systemctl enable threatcluster
   
   # Check status
   sudo systemctl status threatcluster
   ```

## Service Management

### Using the Control Script

The `threatcluster-ctl.sh` script provides easy service management:

```bash
# Start the service
./scripts/threatcluster-ctl.sh start

# Stop the service
./scripts/threatcluster-ctl.sh stop

# Restart the service
./scripts/threatcluster-ctl.sh restart

# Check status
./scripts/threatcluster-ctl.sh status

# View logs (last 50 lines)
./scripts/threatcluster-ctl.sh logs

# Follow live logs
./scripts/threatcluster-ctl.sh follow

# Enable automatic startup
./scripts/threatcluster-ctl.sh enable

# Test pipeline once
./scripts/threatcluster-ctl.sh test
```

### Using Systemctl Directly

```bash
# Service management
sudo systemctl start threatcluster
sudo systemctl stop threatcluster
sudo systemctl restart threatcluster
sudo systemctl status threatcluster

# Enable/disable automatic startup
sudo systemctl enable threatcluster
sudo systemctl disable threatcluster

# View logs
sudo journalctl -u threatcluster -f
sudo journalctl -u threatcluster -n 100
```

## Alternative Methods

### Method 1: Daemon Mode (Simple)

For quick setup without systemd:

```bash
cd /home/james/Desktop/Threatcluster-2/cluster-service
source venv/bin/activate

# Run in daemon mode
python -m src.main --daemon

# Run with debug logging
python -m src.main --daemon --debug
```

### Method 2: Using Nohup

For basic background execution:

```bash
cd /home/james/Desktop/Threatcluster-2/cluster-service
source venv/bin/activate

# Run in background with nohup
nohup python -m src.main --daemon > threatcluster.log 2>&1 &

# Check the process
ps aux | grep threatcluster

# View logs
tail -f threatcluster.log
```

### Method 3: Using Screen/Tmux

For session-based management:

```bash
# Using screen
screen -S threatcluster
cd /home/james/Desktop/Threatcluster-2/cluster-service
source venv/bin/activate
python -m src.main --daemon
# Press Ctrl+A, then D to detach

# Reattach later
screen -r threatcluster

# Using tmux
tmux new-session -d -s threatcluster
tmux send-keys -t threatcluster "cd /home/james/Desktop/Threatcluster-2/cluster-service" Enter
tmux send-keys -t threatcluster "source venv/bin/activate" Enter
tmux send-keys -t threatcluster "python -m src.main --daemon" Enter

# Attach later
tmux attach-session -t threatcluster
```

## Logging and Monitoring

### Log Files

The service generates multiple log files:

1. **Daemon Logs**: `logs/threatcluster_daemon_YYYYMMDD.log`
   - Structured JSON logging
   - Pipeline execution details
   - Component-specific results

2. **System Logs**: Available via `journalctl`
   - Service start/stop events
   - System-level errors
   - Service health information

### Viewing Logs

```bash
# View daemon logs
tail -f logs/threatcluster_daemon_$(date +%Y%m%d).log

# View system logs
sudo journalctl -u threatcluster -f

# View logs with the control script
./scripts/threatcluster-ctl.sh follow
./scripts/threatcluster-ctl.sh logs 100
```

### Log Rotation

The daemon automatically creates daily log files. To set up log rotation:

```bash
# Create logrotate configuration
sudo nano /etc/logrotate.d/threatcluster
```

```
/home/james/Desktop/Threatcluster-2/cluster-service/logs/*.log {
    daily
    rotate 30
    compress
    delaycompress
    missingok
    notifempty
    create 644 james james
}
```

## Configuration

### Service Configuration

The service behavior is controlled by `config/config.yaml`:

```yaml
scheduler:
  enabled: true
  components:
    rss_fetcher:
      enabled: true
      interval_minutes: 60
    article_scraper:
      enabled: true
      interval_minutes: 30
    entity_extractor:
      enabled: true
      interval_minutes: 45
    semantic_clusterer:
      enabled: true
      interval_minutes: 120
    article_ranker:
      enabled: true
      interval_minutes: 60
```

### Environment Variables

Key environment variables in `.env`:

```bash
# Database configuration
DB_HOST=your-db-host
DB_PORT=25060
DB_NAME=threatcluster
DB_USER=doadmin
DB_PASSWORD=your-password

# Logging level
LOG_LEVEL=INFO

# Pipeline settings
PIPELINE_BATCH_SIZE=50
```

## Troubleshooting

### Common Issues

1. **Service Won't Start**
   ```bash
   # Check service status
   sudo systemctl status threatcluster
   
   # View detailed logs
   sudo journalctl -u threatcluster -n 50
   
   # Test daemon manually
   cd /home/james/Desktop/Threatcluster-2/cluster-service
   source venv/bin/activate
   python -m src.daemon --once --debug
   ```

2. **Database Connection Errors**
   ```bash
   # Test database connection
   python -m tests.test_db
   
   # Check environment variables
   cat .env | grep DB_
   ```

3. **Permission Issues**
   ```bash
   # Fix file permissions
   chmod +x scripts/*.sh
   
   # Check service file permissions
   ls -la /etc/systemd/system/threatcluster.service
   ```

4. **Memory/Resource Issues**
   ```bash
   # Monitor resource usage
   top -p $(pgrep -f threatcluster)
   
   # Check service resource limits
   systemctl show threatcluster | grep Limit
   ```

### Debug Mode

For detailed troubleshooting:

```bash
# Run in debug mode
python -m src.daemon --debug

# Or with the service
sudo systemctl edit threatcluster
```

Add:
```ini
[Service]
Environment=DEBUG=1
ExecStart=
ExecStart=/home/james/Desktop/Threatcluster-2/cluster-service/venv/bin/python -m src.daemon --debug
```

### Service Health Checks

```bash
# Check if service is running
./scripts/threatcluster-ctl.sh status

# Test pipeline execution
./scripts/threatcluster-ctl.sh test

# Monitor live logs
./scripts/threatcluster-ctl.sh follow
```

## Performance Tuning

### Resource Optimization

1. **Memory Usage**
   - Adjust batch sizes in `config/config.yaml`
   - Monitor memory with `htop` or `free -m`

2. **CPU Usage**
   - Configure component intervals
   - Use nice/ionice for CPU priority

3. **Disk I/O**
   - Use SSD storage for cache directory
   - Configure log rotation

### Scaling Considerations

- **Database Connection Pooling**: Already handled by SQLAlchemy
- **Parallel Processing**: Configure batch sizes appropriately
- **Resource Limits**: Set appropriate systemd limits

## Migration from Interactive Mode

If you're currently using `python -m src.main`:

1. **Stop the interactive process** (Ctrl+C)
2. **Install the service** following the steps above
3. **Start the service**: `sudo systemctl start threatcluster`
4. **Verify operation**: `./scripts/threatcluster-ctl.sh status`

The service will continue from where the interactive process left off.

## Security Considerations

### Service Security

The systemd service includes security hardening:

- Runs as non-root user
- Private temporary directories
- Protected system directories
- Limited file system access

### Network Security

- Database connections use SSL/TLS
- No network services exposed
- Outbound HTTPS only for RSS feeds

### File Permissions

```bash
# Recommended permissions
chmod 600 .env                    # Environment file
chmod 755 scripts/*.sh           # Scripts
chmod 644 systemd/*.service      # Service files
```

## Backup and Recovery

### Important Files to Backup

1. **Configuration**: `.env`, `config/`
2. **Logs**: `logs/` (if needed)
3. **Cache**: `cache/` (optional, can be regenerated)

### Service Recovery

```bash
# If service fails, check logs and restart
sudo systemctl status threatcluster
sudo systemctl restart threatcluster

# If service file is corrupted, reinstall
./scripts/install-service.sh
```

## Support

For issues with the background service:

1. Check the troubleshooting section above
2. Review logs: `./scripts/threatcluster-ctl.sh logs`
3. Test manually: `python -m src.daemon --once --debug`
4. Check system resources: `htop`, `df -h`

## Summary

The ThreatCluster background service provides:

✅ **Persistent operation** - Survives SSH disconnections  
✅ **Automatic startup** - Starts on system boot  
✅ **Service management** - Standard systemctl commands  
✅ **Comprehensive logging** - Structured logs and system logs  
✅ **Resource management** - Configurable limits and monitoring  
✅ **Security hardening** - Non-root execution and restricted access  
✅ **Easy management** - Simple control scripts  

Choose the method that best fits your needs:
- **Production**: Use systemd service
- **Development**: Use daemon mode or screen/tmux
- **Testing**: Use --once flag for single runs