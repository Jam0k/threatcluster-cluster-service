#!/bin/bash
set -e

# ThreatCluster Service Installation Script
# 
# This script installs ThreatCluster as a systemd service for automatic startup
# and background processing.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SERVICE_NAME="threatcluster"
CURRENT_USER="$(whoami)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

check_requirements() {
    log_info "Checking system requirements..."
    
    # Check if systemd is available
    if ! command -v systemctl &> /dev/null; then
        log_error "systemctl not found. This script requires systemd."
        exit 1
    fi
    
    # Check if running as root or can sudo
    if [[ $EUID -eq 0 ]]; then
        log_warning "Running as root. Consider running as a regular user and using sudo when needed."
    elif ! sudo -n true 2>/dev/null; then
        log_error "This script requires sudo privileges for service installation."
        log_info "Please run: sudo -v"
        exit 1
    fi
    
    # Check if virtual environment exists
    if [[ ! -d "$PROJECT_DIR/venv" ]]; then
        log_error "Virtual environment not found at $PROJECT_DIR/venv"
        log_info "Please create a virtual environment first:"
        log_info "  cd $PROJECT_DIR"
        log_info "  python3 -m venv venv"
        log_info "  source venv/bin/activate"
        log_info "  pip install -r requirements.txt"
        exit 1
    fi
    
    # Check if .env file exists
    if [[ ! -f "$PROJECT_DIR/.env" ]]; then
        log_error "Environment file not found at $PROJECT_DIR/.env"
        log_info "Please create the .env file with database credentials first."
        exit 1
    fi
    
    log_success "Requirements check passed"
}

install_service() {
    log_info "Installing ThreatCluster systemd service..."
    
    # Create service file with current user and paths
    local service_file="/tmp/${SERVICE_NAME}.service"
    
    cat > "$service_file" << EOF
[Unit]
Description=ThreatCluster Continuous Processing Daemon
Documentation=https://threatcluster.com/docs
After=network.target postgresql.service
Wants=network.target

[Service]
Type=simple
User=${CURRENT_USER}
Group=${CURRENT_USER}
WorkingDirectory=${PROJECT_DIR}
Environment=PATH=${PROJECT_DIR}/venv/bin:/usr/local/bin:/usr/bin:/bin
Environment=PYTHONPATH=${PROJECT_DIR}
ExecStart=${PROJECT_DIR}/venv/bin/python -m src.daemon
ExecReload=/bin/kill -HUP \$MAINPID
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=threatcluster

# Resource limits
LimitNOFILE=65536
TimeoutStartSec=300
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
EOF
    
    # Install service file
    sudo cp "$service_file" "/etc/systemd/system/${SERVICE_NAME}.service"
    rm "$service_file"
    
    # Reload systemd and enable service
    sudo systemctl daemon-reload
    sudo systemctl enable "$SERVICE_NAME"
    
    log_success "Service installed successfully"
}

test_daemon() {
    log_info "Testing daemon in test mode..."
    
    cd "$PROJECT_DIR"
    if ./venv/bin/python -m src.daemon --once --debug; then
        log_success "Daemon test completed successfully"
    else
        log_error "Daemon test failed"
        log_info "Check the logs for details:"
        log_info "  tail -f $PROJECT_DIR/logs/threatcluster_daemon_$(date +%Y%m%d).log"
        exit 1
    fi
}

show_usage() {
    log_success "ThreatCluster service installation completed!"
    echo
    log_info "Service Management Commands:"
    echo "  Start service:    sudo systemctl start $SERVICE_NAME"
    echo "  Stop service:     sudo systemctl stop $SERVICE_NAME" 
    echo "  Restart service:  sudo systemctl restart $SERVICE_NAME"
    echo "  Check status:     sudo systemctl status $SERVICE_NAME"
    echo "  View logs:        sudo journalctl -u $SERVICE_NAME -f"
    echo "  Disable service:  sudo systemctl disable $SERVICE_NAME"
    echo
    log_info "Log Files:"
    echo "  Daemon logs:      $PROJECT_DIR/logs/threatcluster_daemon_YYYYMMDD.log"
    echo "  System logs:      sudo journalctl -u $SERVICE_NAME"
    echo
    log_info "Quick Start:"
    echo "  1. Start the service: sudo systemctl start $SERVICE_NAME"
    echo "  2. Check status:      sudo systemctl status $SERVICE_NAME"
    echo "  3. View logs:         tail -f $PROJECT_DIR/logs/threatcluster_daemon_$(date +%Y%m%d).log"
}

main() {
    echo "=========================================="
    echo "ThreatCluster Service Installation"
    echo "=========================================="
    echo
    
    log_info "Project directory: $PROJECT_DIR"
    log_info "Service name: $SERVICE_NAME"
    log_info "Running as user: $CURRENT_USER"
    echo
    
    check_requirements
    test_daemon
    install_service
    show_usage
}

# Run main function
main "$@"