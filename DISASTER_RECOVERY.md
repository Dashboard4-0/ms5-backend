# MS5.0 Floor Dashboard - Disaster Recovery Procedures

## Overview

This document outlines the comprehensive disaster recovery procedures for the MS5.0 Floor Dashboard system. These procedures ensure rapid recovery from various types of failures while maintaining data integrity and system availability.

## Table of Contents

1. [Recovery Scenarios](#recovery-scenarios)
2. [Recovery Procedures](#recovery-procedures)
3. [Recovery Testing](#recovery-testing)
4. [Recovery Validation](#recovery-validation)
5. [Recovery Documentation](#recovery-documentation)
6. [Recovery Contacts](#recovery-contacts)

## Recovery Scenarios

### 1. Database Failure
- **Scenario**: Complete database corruption or hardware failure
- **Impact**: System unavailable, all data potentially lost
- **Recovery Time Objective (RTO)**: 4 hours
- **Recovery Point Objective (RPO)**: 1 hour

### 2. Application Server Failure
- **Scenario**: Server hardware failure or OS corruption
- **Impact**: Application unavailable, database intact
- **Recovery Time Objective (RTO)**: 2 hours
- **Recovery Point Objective (RPO)**: 0 hours

### 3. Network Infrastructure Failure
- **Scenario**: Network connectivity issues or load balancer failure
- **Impact**: System unreachable, services intact
- **Recovery Time Objective (RTO)**: 1 hour
- **Recovery Point Objective (RPO)**: 0 hours

### 4. Storage Failure
- **Scenario**: File system corruption or disk failure
- **Impact**: Application files lost, database may be affected
- **Recovery Time Objective (RTO)**: 3 hours
- **Recovery Point Objective (RPO)**: 2 hours

### 5. Complete Site Failure
- **Scenario**: Data center outage or natural disaster
- **Impact**: Complete system unavailability
- **Recovery Time Objective (RTO)**: 8 hours
- **Recovery Point Objective (RPO)**: 4 hours

## Recovery Procedures

### 1. Database Recovery

#### 1.1 Full Database Recovery

```bash
# 1. Stop all application services
docker-compose down

# 2. Restore database from backup
./restore.sh -t database -i <backup_id>

# 3. Validate database integrity
./validate_database.sh

# 4. Start application services
docker-compose up -d

# 5. Verify system functionality
curl -f http://localhost/api/health || exit 1
```

#### 1.2 Point-in-Time Recovery

```bash
# 1. Identify target recovery time
TARGET_TIME="2024-01-01 12:00:00"

# 2. Restore base backup
./restore.sh -t database -i <base_backup_id>

# 3. Apply WAL files up to target time
pg_basebackup -D /tmp/recovery -Ft -z -P -h <backup_host>
pg_receivewal -D /tmp/wal_archive -h <backup_host>

# 4. Configure recovery.conf
cat > /tmp/recovery/recovery.conf << EOF
restore_command = 'cp /tmp/wal_archive/%f %p'
recovery_target_time = '$TARGET_TIME'
recovery_target_action = 'promote'
EOF

# 5. Start PostgreSQL in recovery mode
pg_ctl -D /tmp/recovery start
```

#### 1.3 Database Schema Recovery

```bash
# 1. Restore schema-only backup
./restore.sh -t database -i <schema_backup_id>

# 2. Apply data-only backup
./restore.sh -t database -i <data_backup_id>

# 3. Run migration scripts
./deploy_migrations.sh

# 4. Validate schema
./validate_database.sh
```

### 2. Application Recovery

#### 2.1 Complete Application Recovery

```bash
# 1. Provision new server
# (Use infrastructure automation scripts)

# 2. Install Docker and dependencies
curl -fsSL https://get.docker.com -o get-docker.sh
sh get-docker.sh

# 3. Clone application repository
git clone <repository_url> /opt/ms5-dashboard
cd /opt/ms5-dashboard/backend

# 4. Restore configuration
./restore.sh -t config -i <config_backup_id>

# 5. Restore application files
./restore.sh -t files -i <files_backup_id>

# 6. Restore database
./restore.sh -t database -i <database_backup_id>

# 7. Start services
docker-compose up -d

# 8. Verify deployment
./validate_deployment.sh
```

#### 2.2 Application Files Recovery

```bash
# 1. Stop application services
docker-compose down

# 2. Restore application files
./restore.sh -t files -i <files_backup_id>

# 3. Restart services
docker-compose up -d

# 4. Verify functionality
curl -f http://localhost/api/health
```

### 3. Configuration Recovery

#### 3.1 Complete Configuration Recovery

```bash
# 1. Backup current configuration
./backup.sh -t config

# 2. Restore configuration
./restore.sh -t config -i <config_backup_id>

# 3. Validate configuration
docker-compose config

# 4. Restart services with new configuration
docker-compose down
docker-compose up -d
```

#### 3.2 Environment-Specific Recovery

```bash
# 1. Restore staging configuration
./restore.sh -t config -i <staging_config_backup_id>

# 2. Restore production configuration
./restore.sh -t config -i <production_config_backup_id>

# 3. Deploy to respective environments
docker-compose -f docker-compose.staging.yml up -d
docker-compose -f docker-compose.production.yml up -d
```

### 4. Network Recovery

#### 4.1 Load Balancer Recovery

```bash
# 1. Identify failed load balancer
# 2. Activate standby load balancer
# 3. Update DNS records
# 4. Verify traffic routing
```

#### 4.2 DNS Recovery

```bash
# 1. Update DNS records to point to backup servers
# 2. Verify DNS propagation
# 3. Test connectivity from multiple locations
```

### 5. Complete Site Recovery

#### 5.1 Hot Standby Site Activation

```bash
# 1. Activate standby data center
# 2. Restore latest database backup
./restore.sh -t database -i <latest_backup_id>

# 3. Restore application files
./restore.sh -t files -i <latest_files_backup_id>

# 4. Restore configuration
./restore.sh -t config -i <latest_config_backup_id>

# 5. Start services
docker-compose up -d

# 6. Update DNS to point to standby site
# 7. Verify system functionality
```

#### 5.2 Cold Standby Site Recovery

```bash
# 1. Provision infrastructure
# 2. Install required software
# 3. Restore from backups (same as hot standby)
# 4. Configure monitoring and alerting
# 5. Perform full system validation
```

## Recovery Testing

### 1. Regular Recovery Testing

#### 1.1 Monthly Database Recovery Test

```bash
# 1. Create test environment
docker-compose -f docker-compose.test.yml up -d

# 2. Restore database backup
./restore.sh -t database -i <test_backup_id>

# 3. Validate database
./validate_database.sh

# 4. Test application functionality
./test_recovery.sh

# 5. Document results
echo "Recovery test completed: $(date)" >> recovery_test_log.txt
```

#### 1.2 Quarterly Full System Recovery Test

```bash
# 1. Provision isolated test environment
# 2. Simulate complete failure
# 3. Execute full recovery procedures
# 4. Validate system functionality
# 5. Document lessons learned
```

### 2. Recovery Test Scenarios

#### 2.1 Database Corruption Test

```bash
# 1. Intentionally corrupt database
# 2. Execute database recovery procedures
# 3. Verify data integrity
# 4. Test application functionality
```

#### 2.2 Server Failure Test

```bash
# 1. Simulate server failure
# 2. Execute application recovery procedures
# 3. Verify service availability
# 4. Test end-to-end functionality
```

## Recovery Validation

### 1. Database Validation

```bash
# 1. Check database connectivity
psql "$DATABASE_URL" -c "SELECT 1;"

# 2. Validate schema integrity
./validate_database.sh

# 3. Check data consistency
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM factory_telemetry.users;"

# 4. Verify foreign key constraints
psql "$DATABASE_URL" -c "SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_type = 'FOREIGN KEY';"
```

### 2. Application Validation

```bash
# 1. Check service health
curl -f http://localhost/api/health

# 2. Test API endpoints
./test_api_endpoints.sh

# 3. Verify WebSocket connectivity
./test_websocket.sh

# 4. Check log files for errors
docker-compose logs --tail=100
```

### 3. System Integration Validation

```bash
# 1. Test PLC connectivity
./test_plc_integration.sh

# 2. Verify monitoring systems
curl -f http://localhost:9090/targets  # Prometheus
curl -f http://localhost:3000/api/health  # Grafana

# 3. Test backup systems
./backup.sh -t full
```

## Recovery Documentation

### 1. Recovery Runbook

- **Location**: `/docs/recovery_runbook.md`
- **Content**: Step-by-step recovery procedures
- **Update Frequency**: Monthly
- **Review**: Quarterly

### 2. Recovery Test Reports

- **Location**: `/logs/recovery_tests/`
- **Content**: Test results and lessons learned
- **Update Frequency**: After each test
- **Review**: Monthly

### 3. Recovery Metrics

- **RTO Tracking**: Average recovery times
- **RPO Tracking**: Data loss measurements
- **Success Rate**: Recovery success percentage
- **Review**: Monthly

## Recovery Contacts

### 1. Primary Recovery Team

| Role | Name | Contact | Availability |
|------|------|---------|--------------|
| Recovery Manager | [Name] | [Email/Phone] | 24/7 |
| Database Administrator | [Name] | [Email/Phone] | 24/7 |
| System Administrator | [Name] | [Email/Phone] | 24/7 |
| Network Administrator | [Name] | [Email/Phone] | 24/7 |

### 2. Escalation Contacts

| Level | Role | Contact | Response Time |
|-------|------|---------|---------------|
| Level 1 | On-call Engineer | [Contact] | 15 minutes |
| Level 2 | Senior Engineer | [Contact] | 30 minutes |
| Level 3 | Engineering Manager | [Contact] | 1 hour |
| Level 4 | IT Director | [Contact] | 2 hours |

### 3. External Contacts

| Service | Provider | Contact | Support Level |
|---------|----------|---------|---------------|
| Cloud Provider | [Provider] | [Contact] | 24/7 |
| Database Support | [Provider] | [Contact] | Business Hours |
| Network Provider | [Provider] | [Contact] | 24/7 |

## Recovery Communication

### 1. Incident Notification

```bash
# 1. Send initial notification
./notify_incident.sh -s "Database failure detected"

# 2. Update stakeholders
./update_status.sh -m "Recovery in progress"

# 3. Send completion notification
./notify_completion.sh -s "System recovered successfully"
```

### 2. Status Updates

- **Frequency**: Every 30 minutes during recovery
- **Channels**: Email, Slack, SMS
- **Content**: Current status, estimated completion time, next steps

### 3. Post-Recovery Communication

- **Incident Report**: Within 24 hours
- **Root Cause Analysis**: Within 48 hours
- **Lessons Learned**: Within 1 week
- **Process Improvements**: Within 2 weeks

## Recovery Automation

### 1. Automated Recovery Scripts

```bash
# 1. Auto-recovery for common failures
./auto_recovery.sh

# 2. Health check and auto-restart
./health_check.sh

# 3. Automated failover
./failover.sh
```

### 2. Monitoring and Alerting

```bash
# 1. Recovery status monitoring
./monitor_recovery.sh

# 2. Automated alerting
./alert_recovery.sh

# 3. Recovery metrics collection
./collect_recovery_metrics.sh
```

## Recovery Maintenance

### 1. Regular Maintenance Tasks

- **Daily**: Verify backup integrity
- **Weekly**: Test recovery procedures
- **Monthly**: Update recovery documentation
- **Quarterly**: Full recovery testing
- **Annually**: Review and update recovery plans

### 2. Recovery Tool Updates

- **Backup Scripts**: Monthly review and updates
- **Recovery Scripts**: Monthly review and updates
- **Monitoring Tools**: Quarterly updates
- **Documentation**: Monthly updates

## Recovery Lessons Learned

### 1. Common Recovery Issues

1. **Insufficient Backup Testing**: Regular testing prevents recovery failures
2. **Outdated Documentation**: Keep procedures current and tested
3. **Inadequate Monitoring**: Early detection reduces recovery time
4. **Poor Communication**: Clear communication during incidents is critical

### 2. Recovery Improvements

1. **Automated Recovery**: Reduce manual intervention where possible
2. **Better Monitoring**: Implement comprehensive system monitoring
3. **Regular Testing**: Schedule and execute regular recovery tests
4. **Documentation**: Maintain up-to-date recovery procedures

## Recovery Metrics and KPIs

### 1. Recovery Time Metrics

- **Mean Time to Recovery (MTTR)**: Target < 4 hours
- **Recovery Time Objective (RTO)**: Target < 4 hours
- **Recovery Point Objective (RPO)**: Target < 1 hour

### 2. Recovery Success Metrics

- **Recovery Success Rate**: Target > 99%
- **Data Loss Incidents**: Target 0 per year
- **Recovery Test Pass Rate**: Target 100%

### 3. Recovery Quality Metrics

- **Recovery Validation Success**: Target 100%
- **Post-Recovery Issues**: Target < 5%
- **Recovery Documentation Accuracy**: Target > 95%

---

**Document Version**: 1.0  
**Last Updated**: $(date)  
**Next Review**: $(date -d "+1 month")  
**Owner**: MS5.0 Development Team
