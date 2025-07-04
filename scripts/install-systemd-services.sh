#!/bin/bash
# Installation script for ThreatCluster systemd services

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}ThreatCluster Systemd Service Installer${NC}"
echo "========================================"

# Check if running as root
if [[ $EUID -eq 0 ]]; then
   echo -e "${RED}This script should not be run as root!${NC}"
   echo "Please run as a regular user with sudo privileges."
   exit 1
fi

# Default values
DEFAULT_USER="threatcluster"
DEFAULT_PATH="/opt/threatcluster/cluster-service"
DEFAULT_ENV_FILE="/etc/threatcluster/environment"

# Get user input
read -p "Enter the system user for ThreatCluster services [${DEFAULT_USER}]: " SERVICE_USER
SERVICE_USER=${SERVICE_USER:-$DEFAULT_USER}

read -p "Enter the ThreatCluster installation path [${DEFAULT_PATH}]: " INSTALL_PATH
INSTALL_PATH=${INSTALL_PATH:-$DEFAULT_PATH}

read -p "Enter the environment file path [${DEFAULT_ENV_FILE}]: " ENV_FILE
ENV_FILE=${ENV_FILE:-$DEFAULT_ENV_FILE}

# Verify paths
if [ ! -d "$INSTALL_PATH" ]; then
    echo -e "${RED}Error: Installation path $INSTALL_PATH does not exist!${NC}"
    exit 1
fi

if [ ! -f "$INSTALL_PATH/venv/bin/python" ]; then
    echo -e "${RED}Error: Python virtual environment not found at $INSTALL_PATH/venv!${NC}"
    exit 1
fi

# Create environment directory if it doesn't exist
ENV_DIR=$(dirname "$ENV_FILE")
if [ ! -d "$ENV_DIR" ]; then
    echo -e "${YELLOW}Creating environment directory: $ENV_DIR${NC}"
    sudo mkdir -p "$ENV_DIR"
fi

# Create environment file if it doesn't exist
if [ ! -f "$ENV_FILE" ]; then
    echo -e "${YELLOW}Creating environment file: $ENV_FILE${NC}"
    sudo tee "$ENV_FILE" > /dev/null << 'EOF'
# ThreatCluster Environment Variables
# Copy your .env file contents here and adjust as needed

# Database Configuration
DB_HOST=your-db-host
DB_PORT=25060
DB_NAME=threatcluster
DB_USER=doadmin
DB_PASSWORD=your-password
DB_SSLMODE=require

# OpenAI Configuration
OPENAI_API_KEY=your-openai-api-key

# Redis/Valkey Configuration (optional)
# REDIS_URL=redis://default:your-password@your-redis-host:25061

# Application Settings
ENVIRONMENT=production
LOG_LEVEL=INFO
EOF
    sudo chmod 600 "$ENV_FILE"
    echo -e "${RED}IMPORTANT: Edit $ENV_FILE with your actual configuration!${NC}"
fi

# Create log directory
echo -e "${YELLOW}Creating log directory...${NC}"
sudo mkdir -p /var/log/threatcluster
sudo chown $SERVICE_USER:$SERVICE_USER /var/log/threatcluster

# Update service files with actual paths
echo -e "${YELLOW}Preparing service files...${NC}"

# Create temporary directory for modified service files
TEMP_DIR=$(mktemp -d)

# Copy and modify service files
for service in threatcluster-openai.service threatcluster-ioc-fetcher.service; do
    cp "$INSTALL_PATH/systemd/$service" "$TEMP_DIR/$service"
    
    # Replace paths in service file
    sed -i "s|User=threatcluster|User=$SERVICE_USER|g" "$TEMP_DIR/$service"
    sed -i "s|Group=threatcluster|Group=$SERVICE_USER|g" "$TEMP_DIR/$service"
    sed -i "s|/opt/threatcluster/cluster-service|$INSTALL_PATH|g" "$TEMP_DIR/$service"
    sed -i "s|/etc/threatcluster/environment|$ENV_FILE|g" "$TEMP_DIR/$service"
done

# Install service files
echo -e "${YELLOW}Installing systemd service files...${NC}"
sudo cp "$TEMP_DIR/"*.service /etc/systemd/system/

# Clean up temp directory
rm -rf "$TEMP_DIR"

# Reload systemd
echo -e "${YELLOW}Reloading systemd...${NC}"
sudo systemctl daemon-reload

# Enable services
echo -e "${YELLOW}Enabling services...${NC}"
sudo systemctl enable threatcluster-openai.service
sudo systemctl enable threatcluster-ioc-fetcher.service

echo ""
echo -e "${GREEN}Installation complete!${NC}"
echo ""
echo "Next steps:"
echo "1. Edit the environment file: sudo nano $ENV_FILE"
echo "2. Start the services:"
echo "   sudo systemctl start threatcluster-openai"
echo "   sudo systemctl start threatcluster-ioc-fetcher"
echo "3. Check service status:"
echo "   sudo systemctl status threatcluster-openai"
echo "   sudo systemctl status threatcluster-ioc-fetcher"
echo "4. View logs:"
echo "   sudo journalctl -u threatcluster-openai -f"
echo "   sudo journalctl -u threatcluster-ioc-fetcher -f"
echo ""
echo "To update ThreatCluster and restart services, run:"
echo "cd $INSTALL_PATH && git pull && sudo systemctl restart threatcluster-openai threatcluster-ioc-fetcher"