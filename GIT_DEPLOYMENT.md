# Git Deployment Guide for ThreatCluster

This guide explains how to push your local changes to Git and pull them on your DigitalOcean server.

## Local Development Machine (Push Changes)

### 1. Check Current Status

First, check what files have been added/modified:

```bash
cd /home/james/Desktop/Threatcluster-2/cluster-service
git status
```

You should see new/modified files:
- `src/daemon.py` (new)
- `src/main.py` (modified)
- `systemd/` directory (new)
- `scripts/` directory (new)
- `BACKGROUND_SERVICE.md` (new)
- `GIT_DEPLOYMENT.md` (new)
- `CLUSTERING.md` (modified)

### 2. Add Files to Git

Add all the new files:

```bash
# Add all new files
git add src/daemon.py
git add src/main.py
git add systemd/
git add scripts/
git add BACKGROUND_SERVICE.md
git add GIT_DEPLOYMENT.md
git add CLUSTERING.md

# Or add everything at once
git add .
```

### 3. Commit Changes

Create a meaningful commit:

```bash
git commit -m "Add background service support for continuous processing

- Added daemon.py for non-interactive background processing
- Added --daemon flag to main.py
- Created systemd service files for production deployment
- Added installation and management scripts
- Added comprehensive documentation
- Updated CLUSTERING.md with integrated pipeline information"
```

### 4. Push to Remote Repository

Push to your remote repository:

```bash
# If you're on main branch
git push origin main

# Or if you're on a different branch
git push origin <branch-name>
```

## DigitalOcean Server (Pull Changes)

### 1. SSH to Your Server

```bash
ssh root@your-digitalocean-ip
# or
ssh username@your-digitalocean-ip
```

### 2. Navigate to Project Directory

```bash
cd /path/to/threatcluster/cluster-service
# Example: cd /opt/threatcluster/cluster-service
```

### 3. Check Current Status

Before pulling, check if there are any local changes:

```bash
git status
```

If you have local changes you want to keep:
```bash
git stash
```

### 4. Pull Latest Changes

Pull the latest changes from the remote repository:

```bash
# Fetch and merge
git pull origin main

# Or if you want to see what changed first
git fetch origin
git log HEAD..origin/main --oneline
git merge origin/main
```

### 5. Handle Permissions

After pulling, ensure scripts have correct permissions:

```bash
chmod +x scripts/*.sh
```

### 6. Install/Update Dependencies

If requirements.txt was updated:

```bash
source venv/bin/activate
pip install -r requirements.txt
```

### 7. Stop Existing Service (If Running)

If you already have ThreatCluster running:

```bash
# If running in screen/tmux
screen -ls  # List screens
screen -X -S <session-name> quit  # Kill screen session

# If running with nohup
ps aux | grep threatcluster
kill <PID>

# If already installed as service
sudo systemctl stop threatcluster
```

### 8. Install as Background Service

Now install the new background service:

```bash
# Run the installation script
./scripts/install-service.sh

# Start the service
sudo systemctl start threatcluster

# Enable for automatic startup
sudo systemctl enable threatcluster

# Check status
sudo systemctl status threatcluster
```

### 9. Verify Installation

Check that everything is working:

```bash
# Check service status
./scripts/threatcluster-ctl.sh status

# View logs
./scripts/threatcluster-ctl.sh logs

# Follow live logs
./scripts/threatcluster-ctl.sh follow
```

## Quick Deployment Script

You can create a deployment script on your server for easier updates:

```bash
# Create deploy.sh on your DigitalOcean server
cat > /opt/threatcluster/deploy.sh << 'EOF'
#!/bin/bash
set -e

echo "Deploying ThreatCluster updates..."

# Navigate to project directory
cd /opt/threatcluster/cluster-service

# Stop service
echo "Stopping service..."
sudo systemctl stop threatcluster || true

# Pull latest changes
echo "Pulling latest changes..."
git pull origin main

# Update permissions
echo "Updating permissions..."
chmod +x scripts/*.sh

# Update dependencies
echo "Updating dependencies..."
source venv/bin/activate
pip install -r requirements.txt

# Restart service
echo "Starting service..."
sudo systemctl start threatcluster

# Check status
echo "Checking status..."
sudo systemctl status threatcluster --no-pager

echo "Deployment complete!"
EOF

chmod +x /opt/threatcluster/deploy.sh
```

Then deploy with a single command:
```bash
/opt/threatcluster/deploy.sh
```

## Rollback Procedure

If something goes wrong after pulling:

### 1. View Git Log
```bash
git log --oneline -10
```

### 2. Rollback to Previous Commit
```bash
# Rollback to specific commit
git reset --hard <commit-hash>

# Or rollback one commit
git reset --hard HEAD~1
```

### 3. Restart Service
```bash
sudo systemctl restart threatcluster
```

## Best Practices

### 1. Always Test Locally First
Before pushing to production:
```bash
# Test daemon mode
python -m src.daemon --once --debug

# Test service installation
./scripts/test-installation.sh
```

### 2. Use Feature Branches
For major changes:
```bash
# Create feature branch
git checkout -b feature/background-service

# Work on feature
# ... make changes ...

# Push feature branch
git push origin feature/background-service

# On server, checkout and test
git fetch origin
git checkout feature/background-service
# Test thoroughly

# If good, merge to main
git checkout main
git merge feature/background-service
git push origin main
```

### 3. Tag Releases
For production deployments:
```bash
# Tag a release
git tag -a v1.0.0 -m "Initial background service support"
git push origin v1.0.0

# On server, checkout specific tag
git fetch --tags
git checkout v1.0.0
```

### 4. Monitor After Deployment
Always monitor after deploying:
```bash
# Watch logs for errors
./scripts/threatcluster-ctl.sh follow

# Check system resources
htop

# Monitor for 5-10 minutes
# Ensure all components are running
```

## Troubleshooting

### Git Pull Errors

**"Your local changes would be overwritten"**
```bash
# Option 1: Stash changes
git stash
git pull
git stash pop

# Option 2: Discard local changes
git reset --hard origin/main
```

**"Permission denied"**
```bash
# Fix Git permissions
sudo chown -R $(whoami):$(whoami) .git/
```

### Service Won't Start After Pull

1. Check logs:
```bash
sudo journalctl -u threatcluster -n 50
```

2. Test manually:
```bash
source venv/bin/activate
python -m src.daemon --once --debug
```

3. Check dependencies:
```bash
pip install -r requirements.txt
```

### Database Connection Issues
Ensure `.env` file exists and has correct credentials:
```bash
# Check if .env exists
ls -la .env

# Test database connection
python -m tests.test_db
```

## Summary

**On Local Machine:**
```bash
git add .
git commit -m "Add background service support"
git push origin main
```

**On DigitalOcean Server:**
```bash
cd /path/to/cluster-service
git pull origin main
chmod +x scripts/*.sh
./scripts/install-service.sh
./scripts/threatcluster-ctl.sh start
```

This ensures your ThreatCluster service continues running even after SSH disconnections!