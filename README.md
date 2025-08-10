# BridgeDB - Database Migration [Developing Phase]

## 1. Project Overview

BridgeDB is a comprehensive web-based database migration tool designed to simplify the process of transferring data between different database systems. Built with modern technologies, it offers an intuitive interface to manage migrations with real-time progress tracking.

**Version:** 1.0.0  
**Last Updated:** 2025-08-10  
**Author:** UjjwalKo

### 1.1 Key Features

- **Multi-Database Support**: Migrate between MySQL, PostgreSQL, Oracle, and SQL Server
- **User-friendly Interface**: Intuitive step-by-step wizard with real-time feedback
- **Schema Analysis**: Automatic schema inspection and compatibility checking
- **Selective Migration**: Choose specific databases and tables to migrate
- **Migration Options**: Control table creation, indexing, and transaction behavior
- **Real-time Monitoring**: Live progress tracking with WebSocket updates
- **Dark/Light Theme**: Toggle between visual modes for different environments
- **Secure Authentication**: Google OAuth integration for secure access
- **Migration History**: Track and manage previous migration operations

## 2. Architecture

### 2.1 Technology Stack

BridgeDB employs a modern technology stack:

- **Backend**: 
  - Python 3.10 with FastAPI framework
  - WebSockets for real-time updates
  - SQLAlchemy for database abstraction
  - AsyncIO for non-blocking operations

- **Frontend**:
  - HTML5/CSS3/JavaScript
  - Bootstrap 5 for responsive design
  - Custom CSS for theming and components
  - Vanilla JavaScript for interactivity

- **Authentication**:
  - Google OAuth 2.0 integration
  - JWT token-based session management

- **Database Support**:
  - MySQL/MariaDB
  - PostgreSQL
  - Oracle Database
  - Microsoft SQL Server

### 2.2 Directory Structure

```
bridgedb/
├── main.py              # Main FastAPI application entry point
├── google_auth.py       # Google OAuth authentication handler
├── db.py                # Database connector and utilities
├── templates/           # Jinja2 templates
│   ├── login.html       # Login page template
│   └── dashboard.html   # Main dashboard template
├── static/              # Static assets
│   ├── css/             # CSS stylesheets
│   ├── js/              # JavaScript files
│   └── img/             # Image assets
├── migrations/          # Migration history storage
├── .env                 # Environment variables
└── requirements.txt     # Project dependencies
```

## 3. Installation & Setup

### 3.1 Prerequisites

- Python 3.10 
- pip 
- Database client libraries for supported databases
- Valid Google OAuth credentials

### 3.2 Environment Setup

1. **Clone the repository**:

2. **Create and activate a virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

### 3.3 Configuration

Create a `.env` file in the project root with the following variables:

```env
# OAuth Configuration
GOOGLE_CLIENT_ID=your-google-client-id
GOOGLE_CLIENT_SECRET=your-google-client-secret

# Security
SECRET_KEY=your-secure-random-string

# App Configuration
APP_HOST=localhost
APP_PORT=8000
BASE_URL=http://localhost:8000
```

### 3.4 Database Client Libraries

Install the client libraries for the database systems you intend to use:

```bash
# For MySQL
pip install mysqlclient

# For PostgreSQL
pip install psycopg2-binary

# For Oracle
pip install cx_Oracle

# For SQL Server
pip install pyodbc
```

### 3.5 Running the Application

Start the application with:

```bash
python main.py
```

Or using uvicorn directly:

```bash
uvicorn main:app --host localhost --port 8000 --reload
```

## 4. Authentication

### 4.1 Google OAuth Setup

1. Create a project in the [Google Cloud Console](https://console.cloud.google.com/)
2. Navigate to "APIs & Services" > "Credentials"
3. Create an "OAuth client ID" with application type "Web application"
4. Add authorized redirect URIs:
   - `http://localhost:8000/auth/callback` (development)
   - `https://your-domain.com/auth/callback` (production)
5. Copy the Client ID and Client Secret to your `.env` file

### 4.2 Authentication Flow

1. User visits the application and is redirected to the login page
2. User selects "Continue with Google"
3. Google OAuth consent screen appears
4. After granting access, the user is redirected to the application
5. A JWT token is created and stored in an HTTP-only cookie
6. The user session is maintained until logout or token expiration

## 5. Core Modules

### 5.1 Database Connector (`db.py`)

The Database Connector module handles all database connections, schema inspection, and migration operations.

#### 5.1.1 Key Classes

- **`DatabaseConnector`**: Manages database connections for source and target databases
- **`SchemaInspector`**: Analyzes database schemas and compares compatibility
- **`DatabaseMigrator`**: Performs the actual migration of data

#### 5.1.2 Connection Management

The connector maintains separate connections for source and target databases:

```python
# Creating a connection to MySQL database
connector.connect(
    db_type="mysql",
    config={
        "host": "localhost",
        "port": 3306,
        "username": "root",
        "password": "password"
    },
    connection_id="source"
)
```

### 5.2 Authentication (`google_auth.py`)

Handles Google OAuth authentication and session management.

#### 5.2.1 Key Functions

- **`get_google_auth_url()`**: Generates the Google authentication URL
- **`exchange_code_for_token()`**: Exchanges authorization code for OAuth tokens
- **`get_user_info()`**: Retrieves user information from Google
- **`create_access_token()`**: Creates JWT session token
- **`verify_token()`**: Validates JWT session token

### 5.3 Main Application (`main.py`)

The main FastAPI application that ties everything together.

#### 5.3.1 Key Routes

- **`/`**: Main dashboard page (authenticated users only)
- **`/login`**: Login page
- **`/login/google`**: Initiates Google OAuth flow
- **`/auth/callback`**: OAuth callback endpoint
- **`/logout`**: Ends user session
- **`/api/connect`**: API endpoint to connect to databases
- **`/api/schemas`**: API endpoint to retrieve database schemas
- **`/api/migrate`**: API endpoint to start migration
- **`/ws/progress`**: WebSocket endpoint for real-time updates

## 6. User Interface

### 6.1 Login Page

The login page provides:
- BridgeDB logo and application title
- Google OAuth login option
- Simple, modern design with dark theme

### 6.2 Dashboard

The dashboard consists of:

#### 6.2.1 Navigation Bar
- BridgeDB logo and title
- Dashboard link
- Theme toggle switch (dark/light)
- User profile dropdown with logout option

#### 6.2.2 Migration Wizard
A step-by-step process divided into four stages:

1. **Source Database Configuration**
   - Database type selection
   - Connection parameters (host, port, credentials)
   - Custom fields based on database type (e.g., Oracle service name)
   - Connection testing

2. **Destination Database Configuration**
   - Similar options as source database
   - Different connection for the target system

3. **Data Selection**
   - Source database/schema selection
   - Table selection with filtering options
   - Destination database/schema selection
   - Schema validation

4. **Migration Execution**
   - Migration options (create tables, include indexes, etc.)
   - Migration summary
   - Progress tracking
   - Start/cancel controls

#### 6.2.3 Migration History
- Table of previous migrations
- Status indicators
- Action buttons for detailed reports
- Refresh option

### 6.3 Theming

The UI supports both dark and light themes:

- **Dark Theme**: Deep navy background (#141b33) with lighter card backgrounds (#1e2745) for better contrast
- **Light Theme**: Light gray background (#f5f7fa) with white cards (#ffffff)

Theme preferences are stored in the browser's localStorage and applied automatically on subsequent visits.

## 7. Migration Process

### 7.1 Workflow

The migration process follows a defined workflow:

1. **Connect to Source**: Establish connection to source database
2. **Connect to Destination**: Establish connection to destination database
3. **Schema Analysis**: Analyze schemas of both databases
4. **Object Selection**: Select databases, schemas, and tables to migrate
5. **Validation**: Validate compatibility between source and destination
6. **Configuration**: Set migration options and parameters
7. **Execution**: Perform the actual data transfer
8. **Verification**: Verify data integrity after migration

### 7.2 Migration Options

BridgeDB provides several migration configuration options:

- **Create Tables**: Automatically create tables if they don't exist in the destination
- **Truncate Tables**: Empty existing tables before migration
- **Include Indexes**: Create indexes on destination tables
- **Use Transactions**: Perform migration within transactions for atomicity

### 7.3 Progress Tracking

Real-time progress is tracked and displayed through:

- Progress bar showing overall completion percentage
- Current table being processed
- Record count being migrated
- Elapsed time
- Estimated time remaining
- Status updates via WebSocket connection

## 8. API Reference

### 8.1 Authentication Endpoints

#### `GET /login/google`
- Initiates Google OAuth flow
- Redirects to Google consent screen

#### `GET /auth/callback`
- OAuth callback endpoint
- Handles token exchange and session creation

#### `GET /logout`
- Terminates user session
- Removes authentication cookies

### 8.2 Database Connection

#### `POST /api/connect`
- **Purpose**: Establish database connection
- **Parameters**:
  - `connection_id`: Identifier for the connection (e.g., "source", "target")
  - `db_type`: Database type (mysql, postgresql, oracle, sqlserver)
  - `config`: Connection parameters (host, port, credentials, etc.)
- **Returns**: Connection status and details

### 8.3 Schema Management

#### `GET /api/schemas/{connection_id}`
- **Purpose**: Retrieve available schemas/databases
- **Parameters**:
  - `connection_id`: Identifier for the connection
- **Returns**: List of available schemas/databases

#### `GET /api/tables/{connection_id}/{schema}`
- **Purpose**: Retrieve tables in a schema
- **Parameters**:
  - `connection_id`: Identifier for the connection
  - `schema`: Schema/database name
- **Returns**: List of tables with structure information

### 8.4 Migration Control

#### `POST /api/migrate`
- **Purpose**: Start a migration operation
- **Parameters**:
  - `source`: Source database configuration
  - `destination`: Destination database configuration
  - `tables`: List of tables to migrate
  - `options`: Migration options
- **Returns**: Migration job ID and initial status

#### `GET /api/migrations`
- **Purpose**: Retrieve migration history
- **Returns**: List of past migrations with status and details

#### `GET /api/migrations/{migration_id}`
- **Purpose**: Get details of a specific migration
- **Parameters**:
  - `migration_id`: Migration job identifier
- **Returns**: Detailed information about the migration

### 8.5 WebSockets

#### `WebSocket /ws/progress`
- **Purpose**: Real-time progress updates
- **Events**:
  - `progress`: Update on migration progress
  - `status`: Status changes (running, completed, failed)
  - `error`: Error notifications
  - `complete`: Migration completion details

## 9. Database Support Details

### 9.1 MySQL/MariaDB

- **Connection**: Uses `mysqlclient` driver
- **Default Port**: 3306
- **Special Features**:
  - Support for different storage engines
  - `AUTO_INCREMENT` handling
  - Character set and collation management

### 9.2 PostgreSQL

- **Connection**: Uses `psycopg2` driver
- **Default Port**: 5432
- **Special Features**:
  - Schema support
  - Custom data types
  - Sequence handling

### 9.3 Oracle Database

- **Connection**: Uses `cx_Oracle` driver
- **Default Port**: 1521
- **Special Features**:
  - Service name or SID connection
  - Tablespace management
  - PL/SQL object support

### 9.4 SQL Server

- **Connection**: Uses `pyodbc` driver
- **Default Port**: 1433
- **Special Features**:
  - Windows authentication option
  - Schema support
  - Identity column handling

## 10. Security Considerations

### 10.1 Authentication Security

- OAuth 2.0 with Google provides strong authentication
- JWT tokens with expiration for session management
- HTTP-only cookies to prevent JavaScript access
- CSRF protection for form submissions

### 10.2 Database Security

- Credentials are never stored, only used in memory
- Option for SSL/TLS encrypted database connections
- Limited connection privileges based on operation needs
- Password fields are masked in the UI

### 10.3 General Security

- Input validation on all API endpoints
- Protection against SQL injection
- Proper error handling without exposing sensitive details
- Rate limiting for API endpoints

## 11. Troubleshooting

### 11.1 Common Issues

#### Authentication Problems
- Verify Google OAuth credentials
- Check redirect URI configuration
- Ensure cookies are enabled in the browser

#### Database Connection Failures
- Check network connectivity
- Verify credentials
- Ensure database server allows remote connections
- Check firewall settings

#### Migration Errors
- Verify compatible schema structures
- Check for sufficient permissions
- Ensure adequate disk space
- Verify data type compatibility

### 11.2 Logging

BridgeDB logs detailed information for troubleshooting:

- Application logs in console output
- Debug level logging can be enabled in development
- Error details for failed operations
- Migration history for tracking past operations

## 12. Future Enhancements

Planned features for future releases:

- **More Database Systems**: Support for MongoDB, Redis, and other NoSQL databases
- **Custom Transformations**: Data transformation during migration
- **Scheduled Migrations**: Set up recurring migration tasks
- **Advanced Filtering**: Complex query support for selective migration
- **Migration Templates**: Save and reuse migration configurations
- **REST API**: Comprehensive API for integration with other systems
- **Enterprise Features**: LDAP integration, role-based access control
- **Performance Optimization**: Parallel processing for large datasets

## 13. Contributing

We welcome contributions to BridgeDB! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests to ensure functionality
5. Commit your changes (`git commit -m 'Add some amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

---

© 2025 UjjwalKo | Last updated: 2025-08-10 16:28:45
