#!/bin/bash

# ThreatCluster Service Control Script
#
# Easy-to-use wrapper for managing the ThreatCluster systemd service

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SERVICE_NAME="threatcluster"

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

check_service_exists() {
    if ! systemctl list-unit-files | grep -q "^${SERVICE_NAME}.service"; then
        log_error "ThreatCluster service is not installed."
        log_info "Run: $PROJECT_DIR/scripts/install-service.sh"
        exit 1
    fi
}

cmd_start() {
    log_info "Starting ThreatCluster service..."
    if sudo systemctl start "$SERVICE_NAME"; then
        log_success "Service started successfully"
        sleep 2
        cmd_status
    else
        log_error "Failed to start service"
        exit 1
    fi
}

cmd_stop() {
    log_info "Stopping ThreatCluster service..."
    if sudo systemctl stop "$SERVICE_NAME"; then
        log_success "Service stopped successfully"
    else
        log_error "Failed to stop service"
        exit 1
    fi
}

cmd_restart() {
    log_info "Restarting ThreatCluster service..."
    if sudo systemctl restart "$SERVICE_NAME"; then
        log_success "Service restarted successfully"
        sleep 2
        cmd_status
    else
        log_error "Failed to restart service"
        exit 1
    fi
}

cmd_status() {
    echo
    log_info "Service Status:"
    sudo systemctl status "$SERVICE_NAME" --no-pager -l
    echo
    
    # Show recent logs
    log_info "Recent logs (last 10 lines):"
    sudo journalctl -u "$SERVICE_NAME" -n 10 --no-pager
    echo
}

cmd_logs() {
    local lines="${1:-50}"
    log_info "Showing last $lines log entries:"
    echo
    sudo journalctl -u "$SERVICE_NAME" -n "$lines" --no-pager
}

cmd_follow() {
    log_info "Following live logs (Ctrl+C to exit):"
    echo
    sudo journalctl -u "$SERVICE_NAME" -f
}

cmd_enable() {
    log_info "Enabling ThreatCluster service for automatic startup..."
    if sudo systemctl enable "$SERVICE_NAME"; then
        log_success "Service enabled for automatic startup"
    else
        log_error "Failed to enable service"
        exit 1
    fi
}

cmd_disable() {
    log_info "Disabling ThreatCluster service automatic startup..."
    if sudo systemctl disable "$SERVICE_NAME"; then
        log_success "Service disabled from automatic startup"
    else
        log_error "Failed to disable service"
        exit 1
    fi
}

cmd_test() {
    log_info "Running ThreatCluster in test mode..."
    cd "$PROJECT_DIR"
    
    if ./venv/bin/python -m src.daemon --once --debug; then
        log_success "Test completed successfully"
    else
        log_error "Test failed"
        exit 1
    fi
}

cmd_interactive() {
    log_info "Starting ThreatCluster in interactive mode..."
    cd "$PROJECT_DIR"
    ./venv/bin/python -m src.main
}

cmd_logs_file() {
    local log_file="$PROJECT_DIR/logs/threatcluster_daemon_$(date +%Y%m%d).log"
    
    if [[ -f "$log_file" ]]; then
        log_info "Showing daemon log file: $log_file"
        echo
        tail -f "$log_file"
    else
        log_warning "Log file not found: $log_file"
        log_info "Available log files:"
        ls -la "$PROJECT_DIR/logs/" | grep threatcluster || log_warning "No log files found"
    fi
}

show_help() {
    echo "ThreatCluster Service Control"
    echo "=========================================="
    echo
    echo "Usage: $0 <command> [arguments]"
    echo
    echo "Service Commands:"
    echo "  start              Start the service"
    echo "  stop               Stop the service"
    echo "  restart            Restart the service"
    echo "  status             Show service status"
    echo "  enable             Enable automatic startup"
    echo "  disable            Disable automatic startup"
    echo
    echo "Logging Commands:"
    echo "  logs [lines]       Show recent logs (default: 50 lines)"
    echo "  follow             Follow live logs"
    echo "  logs-file          Follow daemon log file"
    echo
    echo "Testing Commands:"
    echo "  test               Run pipeline once in test mode"
    echo "  interactive        Start interactive CLI mode"
    echo
    echo "Examples:"
    echo "  $0 start                    # Start the service"
    echo "  $0 status                   # Check service status"
    echo "  $0 logs 100                 # Show last 100 log entries"
    echo "  $0 follow                   # Follow live logs"
    echo "  $0 test                     # Test pipeline execution"
    echo
}

main() {
    local command="${1:-help}"
    
    case "$command" in
        start)
            check_service_exists
            cmd_start
            ;;
        stop)
            check_service_exists
            cmd_stop
            ;;
        restart)
            check_service_exists
            cmd_restart
            ;;
        status)
            check_service_exists
            cmd_status
            ;;
        logs)
            check_service_exists
            cmd_logs "$2"
            ;;
        follow)
            check_service_exists
            cmd_follow
            ;;
        enable)
            check_service_exists
            cmd_enable
            ;;
        disable)
            check_service_exists
            cmd_disable
            ;;
        test)
            cmd_test
            ;;
        interactive)
            cmd_interactive
            ;;
        logs-file)
            cmd_logs_file
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "Unknown command: $command"
            echo
            show_help
            exit 1
            ;;
    esac
}

main "$@"