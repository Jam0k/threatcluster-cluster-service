#!/bin/bash

# ThreatCluster Installation Test Script
# 
# This script tests the installation files without requiring a full environment

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

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

test_files() {
    log_info "Testing installation files..."
    
    local files=(
        "src/daemon.py"
        "src/main.py"
        "systemd/threatcluster.service"
        "systemd/threatcluster-dev.service"
        "scripts/install-service.sh"
        "scripts/threatcluster-ctl.sh"
    )
    
    local missing_files=()
    
    for file in "${files[@]}"; do
        local full_path="$PROJECT_DIR/$file"
        if [[ -f "$full_path" ]]; then
            log_success "✓ $file exists"
        else
            log_error "✗ $file missing"
            missing_files+=("$file")
        fi
    done
    
    if [[ ${#missing_files[@]} -eq 0 ]]; then
        log_success "All required files are present"
        return 0
    else
        log_error "Missing files: ${missing_files[*]}"
        return 1
    fi
}

test_permissions() {
    log_info "Testing file permissions..."
    
    local scripts=(
        "scripts/install-service.sh"
        "scripts/threatcluster-ctl.sh"
    )
    
    for script in "${scripts[@]}"; do
        local full_path="$PROJECT_DIR/$script"
        if [[ -x "$full_path" ]]; then
            log_success "✓ $script is executable"
        else
            log_error "✗ $script is not executable"
            log_info "Fix with: chmod +x $full_path"
        fi
    done
}

test_python_syntax() {
    log_info "Testing Python syntax..."
    
    # Test daemon.py syntax without importing dependencies
    if python3 -m py_compile "$PROJECT_DIR/src/daemon.py" 2>/dev/null; then
        log_success "✓ daemon.py syntax is valid"
    else
        log_error "✗ daemon.py has syntax errors"
        return 1
    fi
    
    # Test that main.py has the --daemon flag
    if grep -q "\-\-daemon" "$PROJECT_DIR/src/main.py"; then
        log_success "✓ main.py has --daemon flag"
    else
        log_error "✗ main.py missing --daemon flag"
        return 1
    fi
}

test_service_files() {
    log_info "Testing systemd service files..."
    
    local service_files=(
        "systemd/threatcluster.service"
        "systemd/threatcluster-dev.service"
    )
    
    for service_file in "${service_files[@]}"; do
        local full_path="$PROJECT_DIR/$service_file"
        
        # Check basic service file structure
        if grep -q "\[Unit\]" "$full_path" && \
           grep -q "\[Service\]" "$full_path" && \
           grep -q "\[Install\]" "$full_path"; then
            log_success "✓ $service_file has correct structure"
        else
            log_error "✗ $service_file missing required sections"
        fi
        
        # Check for essential fields
        if grep -q "ExecStart=" "$full_path"; then
            log_success "✓ $service_file has ExecStart"
        else
            log_error "✗ $service_file missing ExecStart"
        fi
    done
}

show_usage_info() {
    echo
    log_info "Installation Usage:"
    echo "1. Install dependencies (if not already done):"
    echo "   cd $PROJECT_DIR"
    echo "   python3 -m venv venv"
    echo "   source venv/bin/activate"
    echo "   pip install -r requirements.txt"
    echo
    echo "2. Set up environment:"
    echo "   cp .env.example .env"
    echo "   # Edit .env with your database credentials"
    echo
    echo "3. Install as systemd service:"
    echo "   $PROJECT_DIR/scripts/install-service.sh"
    echo
    echo "4. Manage the service:"
    echo "   $PROJECT_DIR/scripts/threatcluster-ctl.sh start"
    echo "   $PROJECT_DIR/scripts/threatcluster-ctl.sh status"
    echo "   $PROJECT_DIR/scripts/threatcluster-ctl.sh logs"
    echo
    echo "Alternative: Run in daemon mode manually:"
    echo "   cd $PROJECT_DIR"
    echo "   source venv/bin/activate"
    echo "   python -m src.main --daemon"
    echo
    echo "Or use with nohup/screen:"
    echo "   nohup python -m src.main --daemon &"
    echo "   screen -S threatcluster python -m src.main --daemon"
}

main() {
    echo "=========================================="
    echo "ThreatCluster Installation Test"
    echo "=========================================="
    echo
    
    log_info "Project directory: $PROJECT_DIR"
    echo
    
    local all_passed=true
    
    if ! test_files; then
        all_passed=false
    fi
    echo
    
    test_permissions
    echo
    
    if ! test_python_syntax; then
        all_passed=false
    fi
    echo
    
    test_service_files
    echo
    
    if $all_passed; then
        log_success "All tests passed! Installation files are ready."
        show_usage_info
    else
        log_error "Some tests failed. Please fix the issues above."
        return 1
    fi
}

main "$@"