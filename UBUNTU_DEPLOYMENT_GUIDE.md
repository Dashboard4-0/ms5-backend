# MS5.0 Floor Dashboard - Ubuntu Deployment Guide

## Table of Contents
1. [Prerequisites](#prerequisites)
2. [System Requirements](#system-requirements)
3. [Initial Server Setup](#initial-server-setup)
4. [Installation Steps](#installation-steps)
5. [Configuration](#configuration)
6. [Database Setup](#database-setup)
7. [SSL Certificate Setup](#ssl-certificate-setup)
8. [Deployment](#deployment)
9. [Verification](#verification)
10. [Common Gotchas & Troubleshooting](#common-gotchas--troubleshooting)
11. [Maintenance & Updates](#maintenance--updates)
12. [Security Hardening](#security-hardening)
13. [Monitoring Setup](#monitoring-setup)
14. [Backup & Recovery](#backup--recovery)

---

## Prerequisites

### Required Knowledge
- Basic Linux command line experience
- Understanding of Docker and Docker Compose
- Basic networking concepts
- SSL/TLS certificate management
- Database administration basics

### Required Access
- Root or sudo access to Ubuntu server
- Domain name pointing to your server (for SSL)
- Email access for SSL certificate validation
- GitHub account access

---

## System Requirements

### Minimum Hardware Requirements
```bash
# CPU: 2 cores (4 cores recommended)
# RAM: 4GB (8GB recommended)
# Storage: 50GB SSD (100GB recommended)
# Network: 100Mbps (1Gbps recommended)
```

### Software Requirements
- Ubuntu 20.04 LTS or 22.04 LTS (recommended)
- Docker Engine 20.10+
- Docker Compose 2.0+
- Git
- curl/wget
- nano/vim (text editor)

---

## Initial Server Setup

### Step 1: Update System Packages
```bash
# Update package lists
sudo apt update

# Upgrade existing packages
sudo apt upgrade -y

# Install essential packages
sudo apt install -y curl wget git nano htop unzip software-properties-common apt-transport-https ca-certificates gnupg lsb-release
```

### Step 2: Create Application User
```bash
# Create dedicated user for the application
sudo adduser ms5app

# Add user to docker group (will be created later)
sudo usermod -aG sudo ms5app

# Switch to application user
su - ms5app
```

### Step 3: Configure Firewall
```bash
# Enable UFW firewall
sudo ufw enable

# Allow SSH (IMPORTANT: Do this first!)
sudo ufw allow ssh

# Allow HTTP and HTTPS
sudo ufw allow 80
sudo ufw allow 443

# Allow application ports (if needed for direct access)
sudo ufw allow 8000  # Backend API
sudo ufw allow 3000  # Grafana (optional, should be behind reverse proxy)

# Check firewall status
sudo ufw status
```

**⚠️ GOTCHA:** Always configure SSH access before enabling firewall to avoid locking yourself out!

---

## Installation Steps

### Step 1: Install Docker Engine
```bash
# Remove old Docker versions
sudo apt remove -y docker docker-engine docker.io containerd runc

# Add Docker's official GPG key
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg

# Add Docker repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Update package lists
sudo apt update

# Install Docker Engine
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

# Add current user to docker group
sudo usermod -aG docker $USER

# Enable Docker service
sudo systemctl enable docker
sudo systemctl start docker

# Verify installation
docker --version
docker compose version
```

**⚠️ GOTCHA:** You need to log out and back in for docker group membership to take effect!

### Step 2: Install Docker Compose (if not included)
```bash
# Download Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose

# Make it executable
sudo chmod +x /usr/local/bin/docker-compose

# Verify installation
docker-compose --version
```

### Step 3: Clone Repository
```bash
# Create application directory
sudo mkdir -p /opt/ms5-backend
sudo chown ms5app:ms5app /opt/ms5-backend

# Switch to application user
su - ms5app

# Clone the repository
cd /opt/ms5-backend
git clone https://github.com/Dashboard4-0/ms5-backend.git .

# Verify files
ls -la
```

---

## Configuration

### Step 1: Environment Configuration
```bash
# Copy production environment template
cp env.production .env

# Edit environment file
nano .env
```

### Step 2: Critical Environment Variables to Configure
```bash
# Database Configuration
POSTGRES_PASSWORD_PRODUCTION=YOUR_SECURE_DATABASE_PASSWORD_HERE
POSTGRES_DB_PRODUCTION=factory_telemetry
POSTGRES_USER_PRODUCTION=ms5_user_production

# Redis Configuration
REDIS_PASSWORD_PRODUCTION=YOUR_SECURE_REDIS_PASSWORD_HERE

# Application Security
SECRET_KEY_PRODUCTION=YOUR_VERY_LONG_RANDOM_SECRET_KEY_HERE
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30
REFRESH_TOKEN_EXPIRE_DAYS=7

# Domain Configuration
ALLOWED_ORIGINS_PRODUCTION=https://yourdomain.com,https://www.yourdomain.com
ALLOWED_HOSTS_PRODUCTION=yourdomain.com,www.yourdomain.com,api.yourdomain.com
CORS_ORIGINS_PRODUCTION=https://yourdomain.com,https://www.yourdomain.com

# Monitoring Configuration
GRAFANA_ADMIN_PASSWORD_PRODUCTION=YOUR_GRAFANA_ADMIN_PASSWORD_HERE
GRAFANA_DOMAIN_PRODUCTION=monitoring.yourdomain.com

# MinIO Configuration
MINIO_USER_PRODUCTION=production_minio_user
MINIO_PASSWORD_PRODUCTION=YOUR_MINIO_PASSWORD_HERE

# Flower Configuration
FLOWER_USER_PRODUCTION=flower_admin
FLOWER_PASSWORD_PRODUCTION=YOUR_FLOWER_PASSWORD_HERE

# Performance Configuration
WORKERS_PRODUCTION=4
MAX_CONNECTIONS_PRODUCTION=1000
```

**⚠️ GOTCHA:** Generate strong passwords! Use a password manager or:
```bash
# Generate secure passwords
openssl rand -base64 32  # For SECRET_KEY_PRODUCTION
openssl rand -base64 16  # For database passwords
```

### Step 3: Create Required Directories
```bash
# Create application directories
mkdir -p logs reports uploads temp ssl/production backups

# Set proper permissions
chmod 755 logs reports uploads temp ssl/production backups
```

---

## Database Setup

### Step 1: Prepare Database
```bash
# Start only the database services first
docker compose -f docker-compose.production.yml up -d postgres redis

# Wait for services to be ready
docker compose -f docker-compose.production.yml logs -f postgres
# Press Ctrl+C when you see "database system is ready to accept connections"
```

### Step 2: Run Database Migrations
```bash
# Connect to PostgreSQL container
docker exec -it ms5_postgres_production psql -U ms5_user_production -d factory_telemetry

# Run migrations (you'll need to copy SQL files to the container or run them externally)
# Exit psql
\q
```

**Alternative: Run migrations from host**
```bash
# Install PostgreSQL client
sudo apt install -y postgresql-client

# Run migrations
PGPASSWORD=YOUR_DATABASE_PASSWORD psql -h localhost -p 5432 -U ms5_user_production -d factory_telemetry -f /path/to/001_init_telemetry.sql
PGPASSWORD=YOUR_DATABASE_PASSWORD psql -h localhost -p 5432 -U ms5_user_production -d factory_telemetry -f /path/to/002_plc_equipment_management.sql
# ... continue with all migration files
```

**⚠️ GOTCHA:** Database migrations must be run in order! Check the migration files in the parent directory.

---

## SSL Certificate Setup

### Option 1: Let's Encrypt (Recommended)
```bash
# Install Certbot
sudo apt install -y certbot

# Stop nginx temporarily
docker compose -f docker-compose.production.yml stop nginx

# Generate certificate
sudo certbot certonly --standalone -d yourdomain.com -d www.yourdomain.com -d api.yourdomain.com -d monitoring.yourdomain.com

# Copy certificates to application directory
sudo cp /etc/letsencrypt/live/yourdomain.com/fullchain.pem ssl/production/production.crt
sudo cp /etc/letsencrypt/live/yourdomain.com/privkey.pem ssl/production/production.key

# Set proper permissions
sudo chown ms5app:ms5app ssl/production/*
sudo chmod 600 ssl/production/production.key
sudo chmod 644 ssl/production/production.crt

# Set up auto-renewal
sudo crontab -e
# Add this line:
# 0 12 * * * /usr/bin/certbot renew --quiet && docker compose -f /opt/ms5-backend/docker-compose.production.yml restart nginx
```

### Option 2: Self-Signed Certificate (Development Only)
```bash
# Generate self-signed certificate
openssl req -x509 -nodes -days 365 -newkey rsa:2048 \
    -keyout ssl/production/production.key \
    -out ssl/production/production.crt \
    -subj "/C=US/ST=State/L=City/O=Organization/CN=yourdomain.com"

# Set proper permissions
chmod 600 ssl/production/production.key
chmod 644 ssl/production/production.crt
```

**⚠️ GOTCHA:** Self-signed certificates will show security warnings in browsers. Only use for development!

---

## Deployment

### Step 1: Start All Services
```bash
# Start all services
docker compose -f docker-compose.production.yml up -d

# Check service status
docker compose -f docker-compose.production.yml ps
```

### Step 2: Monitor Startup
```bash
# Watch logs during startup
docker compose -f docker-compose.production.yml logs -f

# Check specific service logs
docker compose -f docker-compose.production.yml logs -f backend
docker compose -f docker-compose.production.yml logs -f postgres
docker compose -f docker-compose.production.yml logs -f nginx
```

### Step 3: Verify Services
```bash
# Check if all containers are running
docker ps

# Check service health
curl -f http://localhost/health
curl -f https://yourdomain.com/health
```

---

## Verification

### Step 1: Basic Health Checks
```bash
# Backend API health
curl -f http://localhost:8000/health
curl -f https://yourdomain.com/health

# Detailed health check
curl -f https://yourdomain.com/health/detailed

# Metrics endpoint
curl -f https://yourdomain.com/metrics
```

### Step 2: Database Connectivity
```bash
# Test database connection
docker exec -it ms5_postgres_production psql -U ms5_user_production -d factory_telemetry -c "SELECT version();"

# Check database tables
docker exec -it ms5_postgres_production psql -U ms5_user_production -d factory_telemetry -c "\dt factory_telemetry.*"
```

### Step 3: WebSocket Connection
```bash
# Test WebSocket connection (requires websocat)
sudo apt install -y websocat
echo '{"type": "ping"}' | websocat ws://localhost:8000/ws/
```

### Step 4: Monitoring Dashboards
```bash
# Access Grafana (if monitoring subdomain is configured)
curl -f https://monitoring.yourdomain.com

# Check Prometheus metrics
curl -f http://localhost:9090
```

---

## Common Gotchas & Troubleshooting

### 1. Docker Permission Issues
**Problem:** `permission denied while trying to connect to Docker daemon`
```bash
# Solution: Add user to docker group and restart session
sudo usermod -aG docker $USER
# Log out and back in, or run:
newgrp docker
```

### 2. Port Already in Use
**Problem:** `bind: address already in use`
```bash
# Check what's using the port
sudo netstat -tlnp | grep :80
sudo netstat -tlnp | grep :443

# Kill the process or change port in docker-compose.yml
sudo kill -9 <PID>
```

### 3. Database Connection Failed
**Problem:** `connection refused` or `authentication failed`
```bash
# Check database container status
docker compose -f docker-compose.production.yml logs postgres

# Verify environment variables
docker compose -f docker-compose.production.yml config

# Reset database password
docker compose -f docker-compose.production.yml down
docker volume rm ms5-backend_postgres_data_production
docker compose -f docker-compose.production.yml up -d postgres
```

### 4. SSL Certificate Issues
**Problem:** `SSL certificate verify failed`
```bash
# Check certificate files exist and have correct permissions
ls -la ssl/production/
sudo chown ms5app:ms5app ssl/production/*
sudo chmod 600 ssl/production/production.key
sudo chmod 644 ssl/production/production.crt

# Test certificate
openssl x509 -in ssl/production/production.crt -text -noout
```

### 5. Memory Issues
**Problem:** Containers getting killed due to OOM
```bash
# Check system memory
free -h
docker stats

# Increase swap space
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

### 6. Disk Space Issues
**Problem:** `no space left on device`
```bash
# Check disk usage
df -h
docker system df

# Clean up Docker
docker system prune -a
docker volume prune
```

### 7. Firewall Blocking Connections
**Problem:** Can't access services from outside
```bash
# Check firewall status
sudo ufw status

# Allow required ports
sudo ufw allow 80
sudo ufw allow 443
sudo ufw allow 8000  # If accessing backend directly

# Check if services are binding to correct interfaces
netstat -tlnp | grep :80
```

### 8. Environment Variable Issues
**Problem:** Services not starting with correct configuration
```bash
# Check environment variables in container
docker exec -it ms5_backend_production env | grep -E "(DATABASE|REDIS|SECRET)"

# Validate docker-compose configuration
docker compose -f docker-compose.production.yml config
```

---

## Maintenance & Updates

### Regular Maintenance Tasks
```bash
# Update system packages
sudo apt update && sudo apt upgrade -y

# Update Docker images
docker compose -f docker-compose.production.yml pull

# Restart services with new images
docker compose -f docker-compose.production.yml up -d

# Clean up old Docker resources
docker system prune -f
```

### Application Updates
```bash
# Pull latest code
git pull origin main

# Rebuild and restart services
docker compose -f docker-compose.production.yml build
docker compose -f docker-compose.production.yml up -d
```

### Log Management
```bash
# Rotate logs to prevent disk space issues
sudo logrotate -f /etc/logrotate.conf

# Clean application logs
find logs/ -name "*.log" -mtime +7 -delete
```

---

## Security Hardening

### 1. System Security
```bash
# Disable root login
sudo nano /etc/ssh/sshd_config
# Set: PermitRootLogin no
sudo systemctl restart ssh

# Install fail2ban
sudo apt install -y fail2ban
sudo systemctl enable fail2ban
sudo systemctl start fail2ban
```

### 2. Docker Security
```bash
# Run containers as non-root user (already configured in Dockerfile)
# Use read-only root filesystem where possible
# Scan images for vulnerabilities
docker scan ms5-backend_backend
```

### 3. Network Security
```bash
# Configure fail2ban for Docker containers
sudo nano /etc/fail2ban/jail.local
# Add Docker-specific rules
```

### 4. Application Security
```bash
# Regular security updates
sudo apt update && sudo apt upgrade -y

# Monitor for security advisories
# Set up intrusion detection
sudo apt install -y aide
sudo aideinit
sudo mv /var/lib/aide/aide.db.new /var/lib/aide/aide.db
```

---

## Monitoring Setup

### 1. System Monitoring
```bash
# Install monitoring tools
sudo apt install -y htop iotop nethogs

# Set up log monitoring
sudo apt install -y logwatch
```

### 2. Application Monitoring
```bash
# Access Grafana dashboard
# URL: https://monitoring.yourdomain.com
# Default login: admin / YOUR_GRAFANA_ADMIN_PASSWORD

# Check Prometheus metrics
# URL: http://localhost:9090
```

### 3. Alerting Setup
```bash
# Configure email alerts in AlertManager
# Edit alertmanager.yml with your SMTP settings
# Test alerting
curl -X POST http://localhost:9093/api/v1/alerts
```

---

## Backup & Recovery

### 1. Database Backup
```bash
# Create backup script
cat > backup-db.sh << 'EOF'
#!/bin/bash
BACKUP_DIR="/opt/ms5-backend/backups"
DATE=$(date +%Y%m%d_%H%M%S)
mkdir -p $BACKUP_DIR

# Backup database
docker exec ms5_postgres_production pg_dump -U ms5_user_production factory_telemetry > $BACKUP_DIR/db_backup_$DATE.sql

# Compress backup
gzip $BACKUP_DIR/db_backup_$DATE.sql

# Keep only last 7 days of backups
find $BACKUP_DIR -name "db_backup_*.sql.gz" -mtime +7 -delete
EOF

chmod +x backup-db.sh

# Schedule daily backups
crontab -e
# Add: 0 2 * * * /opt/ms5-backend/backup-db.sh
```

### 2. Application Backup
```bash
# Backup application data
tar -czf /opt/ms5-backend/backups/app_backup_$(date +%Y%m%d_%H%M%S).tar.gz \
    logs/ reports/ uploads/ .env ssl/
```

### 3. Recovery Procedures
```bash
# Database recovery
gunzip backups/db_backup_YYYYMMDD_HHMMSS.sql.gz
docker exec -i ms5_postgres_production psql -U ms5_user_production -d factory_telemetry < backups/db_backup_YYYYMMDD_HHMMSS.sql

# Application recovery
tar -xzf backups/app_backup_YYYYMMDD_HHMMSS.tar.gz
```

---

## Quick Reference Commands

### Essential Commands
```bash
# Check service status
docker compose -f docker-compose.production.yml ps

# View logs
docker compose -f docker-compose.production.yml logs -f

# Restart services
docker compose -f docker-compose.production.yml restart

# Stop all services
docker compose -f docker-compose.production.yml down

# Start all services
docker compose -f docker-compose.production.yml up -d

# Check system resources
htop
df -h
free -h

# Check network connectivity
curl -f https://yourdomain.com/health
```

### Emergency Procedures
```bash
# Emergency stop
docker compose -f docker-compose.production.yml down

# Emergency restart
docker compose -f docker-compose.production.yml up -d

# Check what's wrong
docker compose -f docker-compose.production.yml logs --tail=100

# Reset everything (DANGER: This will delete all data!)
docker compose -f docker-compose.production.yml down -v
docker system prune -a
```

---

## Support & Documentation

### Useful Resources
- [Docker Documentation](https://docs.docker.com/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Let's Encrypt Documentation](https://letsencrypt.org/docs/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)

### Getting Help
1. Check application logs: `docker compose -f docker-compose.production.yml logs -f`
2. Check system logs: `journalctl -u docker`
3. Verify configuration: `docker compose -f docker-compose.production.yml config`
4. Test connectivity: `curl -f https://yourdomain.com/health`

### Contact Information
- Repository: https://github.com/Dashboard4-0/ms5-backend
- Documentation: See README.md in the repository
- Issues: Use GitHub Issues for bug reports

---

**⚠️ IMPORTANT REMINDERS:**
1. Always backup before making changes
2. Test in staging environment first
3. Monitor logs during deployment
4. Keep SSL certificates updated
5. Regular security updates are critical
6. Document any custom configurations

This guide should help you successfully deploy the MS5.0 Floor Dashboard backend on Ubuntu Linux. Follow each step carefully and refer to the troubleshooting section if you encounter issues.
