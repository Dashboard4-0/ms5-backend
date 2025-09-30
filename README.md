# MS5.0 Floor Dashboard - Backend API

This is the backend API for the MS5.0 Floor Dashboard, a comprehensive factory management system designed for tablet-based operations with role-based access control.

## Features

- **Production Management**: Production lines, schedules, and job assignments
- **Real-time OEE Calculations**: Overall Equipment Effectiveness tracking
- **Andon System**: Machine stoppage alerts with escalation management
- **Role-based Access Control**: Comprehensive permission system
- **WebSocket Support**: Real-time updates and notifications
- **RESTful API**: Complete CRUD operations for all entities
- **Database Integration**: PostgreSQL with TimescaleDB for time-series data
- **Authentication**: JWT-based authentication with refresh tokens
- **Monitoring**: Prometheus metrics and health checks
- **Documentation**: Auto-generated API documentation

## Technology Stack

- **Framework**: FastAPI (Python 3.11+)
- **Database**: PostgreSQL 15+ with TimescaleDB extension
- **Cache**: Redis
- **Authentication**: JWT with bcrypt password hashing
- **WebSocket**: Native FastAPI WebSocket support
- **Monitoring**: Prometheus + Grafana
- **Containerization**: Docker + Docker Compose
- **Background Tasks**: Celery with Redis broker

## Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Redis
- Docker & Docker Compose (optional)

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd backend
   ```

2. **Create virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   ```bash
   cp env.example .env
   # Edit .env with your configuration
   ```

5. **Set up database**
   ```bash
   # Create database
   createdb factory_telemetry
   
   # Run migrations (when available)
   alembic upgrade head
   ```

6. **Run the application**
   ```bash
   uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
   ```

### Docker Development

1. **Start all services**
   ```bash
   docker-compose up -d
   ```

2. **View logs**
   ```bash
   docker-compose logs -f backend
   ```

3. **Stop services**
   ```bash
   docker-compose down
   ```

## API Documentation

Once the server is running, you can access:

- **Interactive API Docs**: http://localhost:8000/docs
- **ReDoc Documentation**: http://localhost:8000/redoc
- **OpenAPI Schema**: http://localhost:8000/openapi.json

## API Endpoints

### Authentication
- `POST /api/v1/auth/login` - User login
- `POST /api/v1/auth/refresh` - Refresh access token
- `POST /api/v1/auth/logout` - User logout
- `GET /api/v1/auth/profile` - Get user profile
- `PUT /api/v1/auth/profile` - Update user profile

### Production Management
- `GET /api/v1/production/lines` - List production lines
- `POST /api/v1/production/lines` - Create production line
- `GET /api/v1/production/lines/{id}` - Get production line
- `PUT /api/v1/production/lines/{id}` - Update production line
- `DELETE /api/v1/production/lines/{id}` - Delete production line

### Job Management
- `GET /api/v1/jobs/my-jobs` - Get user's job assignments
- `GET /api/v1/jobs/{id}` - Get job assignment
- `POST /api/v1/jobs/{id}/accept` - Accept job assignment
- `POST /api/v1/jobs/{id}/start` - Start job
- `POST /api/v1/jobs/{id}/complete` - Complete job

### OEE & Analytics
- `POST /api/v1/oee/calculate` - Calculate OEE
- `GET /api/v1/oee/lines/{id}` - Get line OEE history
- `GET /api/v1/oee/lines/{id}/current` - Get current OEE
- `GET /api/v1/oee/lines/{id}/daily-summary` - Get daily OEE summary

### Andon System
- `POST /api/v1/andon/events` - Create Andon event
- `GET /api/v1/andon/events` - List Andon events
- `PUT /api/v1/andon/events/{id}/acknowledge` - Acknowledge event
- `PUT /api/v1/andon/events/{id}/resolve` - Resolve event

### Dashboard
- `GET /api/v1/dashboard/lines` - Get dashboard lines
- `GET /api/v1/dashboard/summary` - Get dashboard summary
- `GET /api/v1/dashboard/lines/{id}/status` - Get line status
- `GET /api/v1/dashboard/lines/{id}/oee` - Get line OEE

### Equipment
- `GET /api/v1/equipment/status` - Get equipment status
- `GET /api/v1/equipment/{code}/status` - Get equipment detail
- `GET /api/v1/equipment/{code}/faults` - Get equipment faults
- `POST /api/v1/equipment/{code}/maintenance` - Schedule maintenance

### Reports
- `GET /api/v1/reports/production` - Get production reports
- `POST /api/v1/reports/production/generate` - Generate report
- `GET /api/v1/reports/production/{id}/pdf` - Download PDF report

### WebSocket
- `WS /ws/` - WebSocket endpoint for real-time updates

## WebSocket Events

### Client to Server
- `subscribe` - Subscribe to line or equipment updates
- `unsubscribe` - Unsubscribe from updates
- `ping` - Health check

### Server to Client
- `line_status_update` - Production line status change
- `equipment_status_update` - Equipment status change
- `andon_event` - New Andon event
- `oee_update` - OEE calculation update
- `downtime_event` - Downtime event
- `job_update` - Job assignment update
- `system_alert` - System-wide alert

## Authentication

The API uses JWT-based authentication. Include the access token in the Authorization header:

```
Authorization: Bearer <access_token>
```

For WebSocket connections, include the token as a query parameter:

```
WS /ws/?token=<access_token>
```

## User Roles

- **admin**: Full system access
- **production_manager**: Production management and oversight
- **shift_manager**: Shift-level management
- **engineer**: Technical and maintenance operations
- **operator**: Production line operations
- **maintenance**: Equipment maintenance
- **quality**: Quality control
- **viewer**: Read-only access

## Database Schema

The backend uses PostgreSQL with the following main schemas:

- `factory_telemetry` - Main application data
- `public` - System tables and extensions

Key tables include:
- `production_lines` - Production line definitions
- `production_schedules` - Production scheduling
- `job_assignments` - Job assignments
- `oee_calculations` - OEE time-series data
- `andon_events` - Andon system events
- `downtime_events` - Downtime tracking
- `users` - User management

## Configuration

Configuration is managed through environment variables. See `env.example` for all available options.

Key configuration areas:
- Database connection
- Redis cache
- JWT settings
- CORS configuration
- File upload limits
- Monitoring settings

## Monitoring

The application includes comprehensive monitoring:

- **Health Checks**: `/health` and `/health/detailed`
- **Metrics**: Prometheus metrics at `/metrics`
- **Logging**: Structured logging with correlation IDs
- **Tracing**: Request tracing for debugging

## Development

### Code Style

The project uses:
- **Black** for code formatting
- **isort** for import sorting
- **flake8** for linting
- **mypy** for type checking

Run code quality checks:
```bash
black app/
isort app/
flake8 app/
mypy app/
```

### Testing

Run tests:
```bash
pytest
```

With coverage:
```bash
pytest --cov=app
```

### Database Migrations

Create migration:
```bash
alembic revision --autogenerate -m "Description"
```

Apply migrations:
```bash
alembic upgrade head
```

## Deployment

### Production Deployment

1. **Set production environment variables**
2. **Configure reverse proxy (Nginx)**
3. **Set up SSL certificates**
4. **Configure monitoring and alerting**
5. **Set up backup procedures**

### Docker Production

```bash
docker-compose -f docker-compose.prod.yml up -d
```

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Check database URL and credentials
   - Ensure database is running
   - Verify network connectivity

2. **Authentication Issues**
   - Check JWT secret key
   - Verify token expiration settings
   - Check user permissions

3. **WebSocket Connection Issues**
   - Verify token in query parameter
   - Check CORS settings
   - Ensure WebSocket support in proxy

### Logs

View application logs:
```bash
# Docker
docker-compose logs -f backend

# Local
tail -f logs/app.log
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Run code quality checks
6. Submit a pull request

## License

This project is proprietary software. All rights reserved.

## Support

For support and questions, contact the development team.
