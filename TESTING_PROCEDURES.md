# MS5.0 Floor Dashboard - Testing Procedures

## Overview

This document outlines the comprehensive testing procedures for the MS5.0 Floor Dashboard system. These procedures ensure system reliability, performance, and functionality across all deployment environments.

## Table of Contents

1. [Testing Strategy](#testing-strategy)
2. [Test Types](#test-types)
3. [Test Environments](#test-environments)
4. [Test Execution](#test-execution)
5. [Test Automation](#test-automation)
6. [Test Reporting](#test-reporting)
7. [Test Maintenance](#test-maintenance)

## Testing Strategy

### 1. Testing Pyramid

```
                    /\
                   /  \
                  / E2E \
                 /______\
                /        \
               / Integration \
              /______________\
             /                \
            /    Unit Tests    \
           /____________________\
```

- **Unit Tests (70%)**: Fast, isolated tests for individual components
- **Integration Tests (20%)**: Tests for component interactions
- **End-to-End Tests (10%)**: Full system workflow tests

### 2. Testing Principles

- **Automated Testing**: All tests should be automated where possible
- **Continuous Testing**: Tests run on every code change
- **Environment Parity**: Test environments mirror production
- **Fail Fast**: Quick feedback on test failures
- **Comprehensive Coverage**: All critical paths tested

## Test Types

### 1. Unit Tests

#### 1.1 Backend Unit Tests

**Location**: `backend/tests/unit/`
**Framework**: pytest
**Coverage Target**: 90%

```bash
# Run unit tests
cd backend
pytest tests/unit/ -v --cov=. --cov-report=html

# Run specific test file
pytest tests/unit/test_production_service.py -v

# Run tests with coverage
pytest tests/unit/ --cov=. --cov-report=term-missing
```

**Test Categories**:
- Service layer tests
- Model tests
- Utility function tests
- API endpoint tests
- Database operation tests

#### 1.2 Frontend Unit Tests

**Location**: `frontend/src/__tests__/`
**Framework**: Jest + React Testing Library
**Coverage Target**: 85%

```bash
# Run unit tests
cd frontend
npm test

# Run tests with coverage
npm test -- --coverage

# Run specific test file
npm test -- --testPathPattern=ProductionDashboard.test.tsx
```

**Test Categories**:
- Component tests
- Hook tests
- Utility function tests
- Service tests
- Redux/state management tests

### 2. Integration Tests

#### 2.1 API Integration Tests

**Location**: `backend/tests/integration/`
**Framework**: pytest + httpx
**Coverage Target**: 80%

```bash
# Run integration tests
cd backend
pytest tests/integration/ -v

# Run with test database
pytest tests/integration/ -v --database-url=postgresql://test:test@localhost:5432/test_db
```

**Test Categories**:
- API endpoint integration
- Database integration
- Redis integration
- WebSocket integration
- External service integration

#### 2.2 Database Integration Tests

**Location**: `backend/tests/integration/test_database.py`

```python
# Example database integration test
def test_production_data_flow():
    # Test complete production data flow
    # from PLC to database to API
    pass

def test_andon_escalation_flow():
    # Test andon escalation workflow
    pass
```

### 3. End-to-End Tests

#### 3.1 System E2E Tests

**Location**: `test_e2e/`
**Framework**: pytest + playwright
**Coverage Target**: 70%

```bash
# Run E2E tests
cd test_e2e
pytest -v

# Run specific E2E test
pytest test_production_workflow.py -v
```

**Test Scenarios**:
- Complete production workflow
- Andon escalation process
- OEE calculation process
- User authentication flow
- Data visualization

#### 3.2 Performance E2E Tests

**Location**: `test_performance/`
**Framework**: pytest + locust

```bash
# Run performance tests
cd test_performance
pytest -v

# Run load tests
locust -f test_api_load.py --host=http://localhost:8000
```

### 4. Smoke Tests

#### 4.1 Deployment Smoke Tests

**Location**: `backend/test_smoke.sh`
**Purpose**: Verify basic system functionality after deployment

```bash
# Run smoke tests
./test_smoke.sh -e staging
./test_smoke.sh -e production
```

**Test Coverage**:
- API health endpoints
- Database connectivity
- Redis connectivity
- WebSocket connectivity
- Authentication endpoints
- Production endpoints
- Monitoring endpoints
- System resources
- Docker services

### 5. Security Tests

#### 5.1 Authentication Tests

**Location**: `test_security/`
**Framework**: pytest + requests

```bash
# Run security tests
cd test_security
pytest -v
```

**Test Categories**:
- Authentication bypass attempts
- Authorization checks
- Input validation
- SQL injection prevention
- XSS prevention
- CSRF protection

### 6. Performance Tests

#### 6.1 Load Tests

**Location**: `test_performance/`
**Framework**: locust

```bash
# Run load tests
cd test_performance
locust -f test_api_load.py --host=http://localhost:8000 --users=100 --spawn-rate=10
```

**Test Scenarios**:
- API endpoint load testing
- Database load testing
- WebSocket load testing
- Concurrent user simulation
- Memory usage testing
- CPU usage testing

## Test Environments

### 1. Development Environment

**Purpose**: Local development and testing
**Configuration**: `docker-compose.yml`
**Database**: Local PostgreSQL
**Features**: Debug mode, hot reload, detailed logging

### 2. Staging Environment

**Purpose**: Pre-production testing
**Configuration**: `docker-compose.staging.yml`
**Database**: Staging PostgreSQL
**Features**: Production-like configuration, monitoring enabled

### 3. Production Environment

**Purpose**: Live system
**Configuration**: `docker-compose.production.yml`
**Database**: Production PostgreSQL
**Features**: Optimized performance, security hardened

### 4. Test Environment

**Purpose**: Automated testing
**Configuration**: `docker-compose.test.yml`
**Database**: Test PostgreSQL
**Features**: Isolated testing, fast cleanup

## Test Execution

### 1. Pre-Deployment Testing

```bash
# 1. Run unit tests
cd backend && pytest tests/unit/ -v
cd frontend && npm test

# 2. Run integration tests
cd backend && pytest tests/integration/ -v

# 3. Run security tests
cd test_security && pytest -v

# 4. Run performance tests
cd test_performance && pytest -v
```

### 2. Deployment Testing

```bash
# 1. Deploy to staging
./deploy.sh -e staging -t full

# 2. Run smoke tests
./test_smoke.sh -e staging

# 3. Run E2E tests
cd test_e2e && pytest -v

# 4. Run load tests
cd test_performance && locust -f test_api_load.py --host=http://staging:8000
```

### 3. Post-Deployment Testing

```bash
# 1. Validate deployment
./validate_deployment.sh -e staging -t full

# 2. Run smoke tests
./test_smoke.sh -e staging

# 3. Monitor system performance
./monitor_performance.sh -e staging
```

## Test Automation

### 1. CI/CD Pipeline Integration

**File**: `.github/workflows/ci-cd.yml`

```yaml
# Unit Tests
- name: Run unit tests
  run: |
    cd backend
    pytest tests/unit/ -v --cov=.

# Integration Tests
- name: Run integration tests
  run: |
    cd backend
    pytest tests/integration/ -v

# E2E Tests
- name: Run E2E tests
  run: |
    cd test_e2e
    pytest -v
```

### 2. Automated Test Execution

**Schedule**: Every commit, daily, weekly
**Triggers**: Code changes, deployment, manual

```bash
# Daily test execution
0 2 * * * /opt/ms5-dashboard/backend/run_daily_tests.sh

# Weekly comprehensive testing
0 3 * * 0 /opt/ms5-dashboard/backend/run_weekly_tests.sh
```

### 3. Test Data Management

**Location**: `test_data/`
**Purpose**: Consistent test data across environments

```bash
# Load test data
./load_test_data.sh -e staging

# Clean test data
./clean_test_data.sh -e staging
```

## Test Reporting

### 1. Test Results

**Location**: `test_results/`
**Formats**: HTML, XML, JSON
**Tools**: pytest-html, coverage.py, allure

```bash
# Generate HTML report
pytest --html=test_results/report.html --self-contained-html

# Generate coverage report
pytest --cov=. --cov-report=html --cov-report=term
```

### 2. Test Metrics

**Metrics Tracked**:
- Test pass rate
- Test execution time
- Code coverage
- Bug detection rate
- Test maintenance effort

### 3. Test Dashboards

**Tools**: Grafana, Jenkins
**Purpose**: Visual test metrics and trends

## Test Maintenance

### 1. Test Review

**Frequency**: Monthly
**Scope**: Test effectiveness, coverage, maintenance

### 2. Test Updates

**Triggers**: Code changes, requirement changes
**Process**: Update tests, review coverage, validate

### 3. Test Cleanup

**Frequency**: Quarterly
**Scope**: Remove obsolete tests, optimize test suite

## Test Scripts

### 1. Test Execution Scripts

```bash
# Run all tests
./run_all_tests.sh

# Run specific test suite
./run_test_suite.sh unit
./run_test_suite.sh integration
./run_test_suite.sh e2e

# Run tests for specific component
./run_component_tests.sh production
./run_component_tests.sh andon
./run_component_tests.sh oee
```

### 2. Test Data Scripts

```bash
# Generate test data
./generate_test_data.sh

# Reset test data
./reset_test_data.sh

# Backup test data
./backup_test_data.sh
```

### 3. Test Environment Scripts

```bash
# Setup test environment
./setup_test_env.sh

# Cleanup test environment
./cleanup_test_env.sh

# Reset test environment
./reset_test_env.sh
```

## Test Best Practices

### 1. Test Design

- **Arrange-Act-Assert**: Clear test structure
- **Single Responsibility**: One test per scenario
- **Descriptive Names**: Clear test purpose
- **Independent Tests**: No test dependencies

### 2. Test Data

- **Consistent Data**: Same test data across runs
- **Minimal Data**: Only necessary test data
- **Clean Data**: Fresh data for each test
- **Realistic Data**: Production-like test data

### 3. Test Maintenance

- **Regular Updates**: Keep tests current
- **Remove Obsolete Tests**: Clean up unused tests
- **Optimize Performance**: Fast test execution
- **Document Changes**: Track test modifications

## Test Troubleshooting

### 1. Common Issues

**Test Failures**:
- Check test data
- Verify environment setup
- Review test logs
- Validate test assumptions

**Performance Issues**:
- Optimize test execution
- Reduce test data
- Parallel test execution
- Mock external services

### 2. Debugging Tests

```bash
# Run tests with debug output
pytest -v -s --tb=long

# Run specific test with debug
pytest -v -s test_specific.py::test_function

# Run tests with logging
pytest --log-cli-level=DEBUG
```

## Test Documentation

### 1. Test Documentation

**Location**: `docs/testing/`
**Content**: Test procedures, test data, test results

### 2. Test Reports

**Location**: `test_results/`
**Content**: Test execution reports, coverage reports, performance reports

### 3. Test Metrics

**Location**: `test_metrics/`
**Content**: Test metrics, trends, analysis

---

**Document Version**: 1.0  
**Last Updated**: $(date)  
**Next Review**: $(date -d "+1 month")  
**Owner**: MS5.0 Development Team
