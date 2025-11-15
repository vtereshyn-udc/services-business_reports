# Amazon Reports - Automated Data Collection Platform

## Table of Contents

- [Executive Summary](#executive-summary)
- [Project Architecture](#project-architecture)
- [Core Components](#core-components)
  - [Entry Point (main.py)](#entry-point-mainpy)
  - [Scheduler System](#scheduler-system)
  - [Services Documentation](#services-documentation)
  - [Configuration System](#configuration-system)
  - [Database Layer](#database-layer)
  - [Supporting Infrastructure](#supporting-infrastructure)
- [Scheduler Mechanism](#scheduler-mechanism)
- [Execution Flow](#execution-flow)
- [Active Schedule](#active-schedule)

---

## Executive Summary

This is a sophisticated **Amazon Seller Central data extraction and automation platform** that orchestrates multiple services to collect, process, and store various Amazon marketplace reports. The system uses:

- **Web Automation**: Playwright-based browser automation
- **API Integration**: Amazon SP-API and AD-API
- **Smart Scheduling**: APScheduler with time randomization
- **Multi-Database**: SQLite, BigQuery, PostgreSQL
- **Async Architecture**: High-performance asynchronous execution

---

## Project Architecture

### High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Scheduler (scheduler.py)                │
│  - APScheduler with AsyncIOScheduler                        │
│  - Cron-based job management                                │
│  - Dynamic time randomization                               │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├──> Executes: poetry run python main.py [args]
                 │
┌────────────────▼────────────────────────────────────────────┐
│                      main.py (Orchestrator)                 │
│  - Argument parsing                                         │
│  - Service factory pattern                                  │
│  - Service routing and execution                            │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ├──> Routes to appropriate service
                 │
┌────────────────▼────────────────────────────────────────────┐
│                         Services Layer                      │
│  ┌──────────────────┐  ┌──────────────────┐                │
│  │ Web Automation   │  │   API Services   │                │
│  │   Services       │  │                  │                │
│  ├──────────────────┤  ├──────────────────┤                │
│  │ • AmazonAds      │  │ • AmazonAD       │                │
│  │ • BrandAnalytics │  │ • AmazonSP       │                │
│  │ • Fulfillment    │  │ • BrandAnalyticsAPI              │
│  │ • BusinessReports│  │                  │                │
│  │ • Payments       │  │                  │                │
│  │ • Awd            │  │                  │                │
│  │ • Shipments      │  │                  │                │
│  │ • Support        │  │                  │                │
│  │ • Datarova       │  │                  │                │
│  └──────────────────┘  └──────────────────┘                │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│                    Infrastructure Layer                     │
│  ┌────────────┐  ┌────────────┐  ┌──────────────┐         │
│  │  Database  │  │   Logger   │  │ Notifications│         │
│  │            │  │            │  │              │         │
│  │ • SQLite   │  │ • Custom   │  │ • Telegram   │         │
│  │ • BigQuery │  │   Logger   │  │   Bot        │         │
│  │ • Postgres │  │ • Cleaner  │  │              │         │
│  └────────────┘  └────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────┘
```

---

## Core Components

### Entry Point (main.py)

**Location**: `main.py:24-54`

**Purpose**: Service orchestration hub that routes execution requests to appropriate services.

**Architecture Pattern**: Factory Pattern

#### Key Mechanisms

**1. Argument Parsing**

Loads argument configuration from `settings/arguments.json`:

- **Required**: `--user_id`, `--service`
- **Optional**: `--category`, `--report_type`, `--period`

**2. Service Registry** (`main.py:34-47`)

```python
services = {
    "amazon_ads": AmazonAds,
    "brand_analytics": BrandAnalytics,
    "awd": Awd,
    "fulfillment": Fulfillment,
    "shipments": Shipments,
    "support": Support,
    "business_reports": BusinessReports,
    "datarova": Datarova,
    "payments": Payments,
    "api_ad": AmazonAD,
    "api_sp": AmazonSP,
    "brand_analytics_api": BrandAnalyticsAPI
}
```

**3. Dynamic Service Instantiation**

```python
service = services[args.service](**kwargs)
service.run()
```

---

## Scheduler System

### Architecture

**Location**: `scheduler.py`

**Framework**: APScheduler (AsyncIOScheduler)

**Key Features**:
- ✅ Asynchronous execution
- ✅ Cron-based triggering
- ✅ Dynamic time randomization within ranges
- ✅ Process locking per user
- ✅ Automatic job rescheduling

### Scheduler Workflow

```
┌─────────────────────────────────────────────────────────────┐
│ 1. Initialization (scheduler.py:30-32)                      │
│    - Create AsyncIOScheduler instance                       │
│    - Initialize user locks dictionary                       │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│ 2. Job Creation (scheduler.py:172-235)                      │
│    - Read schedule.json                                     │
│    - Check if job enabled                                   │
│    - Parse job arguments                                    │
│    - Get last execution time from database                  │
│    - Calculate next execution time                          │
│    - Add job to scheduler                                   │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│ 3. Job Execution (scheduler.py:132-169)                     │
│    - Check for active processes (user-based locking)        │
│    - Wait if process already running                        │
│    - Execute: poetry run python main.py [args]              │
│    - Calculate next run time                                │
│    - Reschedule job with new trigger                        │
└─────────────────────────────────────────────────────────────┘
```

### Schedule Configuration

**Location**: `settings/schedule.json`

**Structure**:

```json
{
  "type": "daily|weekly|monthly",
  "enabled": true,
  "time_range": [
    {"start": "HH:MM", "end": "HH:MM"}
  ],
  "day": 15,
  "args": [
    "--user=X",
    "--service=service_name",
    "--category=category_name"
  ]
}
```

#### Scheduling Types

| Type | Description | Example |
|------|-------------|---------|
| **daily** | Execute every day within time range | Sales reports |
| **weekly** | Execute once per week | Shipment tracking |
| **monthly** | Execute once per month (optionally on specific day) | Financial reports |

#### Time Randomization (`scheduler.py:58-99`)

**Algorithm**:
1. Randomizes execution time within specified ranges
2. Prevents detection patterns
3. Multiple time windows per task allowed
4. Uses last execution time to avoid duplicate runs

**Example**:
```python
time_range = [{"start": "00:00", "end": "02:00"}]
# Actual execution: Random time between 00:00-02:00
# Next day: Different random time in same range
```

### Process Management

#### User-Based Locking (`scheduler.py:138-144`)

```python
async with self.lock[user_id]:
    while await self.active_process(user_id=user_id):
        logger.info(f"waiting for active process :: user_id={user_id}")
        await asyncio.sleep(60)
```

**Purpose**: Ensures only one process per user runs simultaneously

#### Process Detection (`scheduler.py:102-129`)

- Uses WMIC to check running Python processes
- Parses command line arguments
- Ensures only one process per user

#### Keep-Alive Mechanism (`scheduler.py:35-45`)

**Windows-specific feature**:
- Simulates keyboard input (Shift key press)
- Prevents system sleep during long operations
- Runs every 5 minutes

---

## Services Documentation

### Web Automation Services

All web automation services inherit from `PlaywrightAsync` base class.

#### Base: PlaywrightAsync

**Location**: `base/playwright_async.py`

**Key Features**:
- Browser automation via Playwright
- Remote browser connection (CDP endpoint)
- Element interaction utilities
- Authentication handling
- CAPTCHA solving integration

**Constructor Parameters**:
- `user_id`: User identifier (mapped to credentials)
- `port`: CDP endpoint port for browser connection

---

### Service Catalog

#### 1. amazon_ads

**Class**: `AmazonAds`
**Location**: `services/amazon_ads.py`
**Type**: Web Automation
**Purpose**: Amazon Advertising reports via web scraping

**Features**:
- Random scrolling to simulate human behavior
- Random clicking on elements
- Navigation management

**Usage**:
```bash
python main.py --user_id=X --service=amazon_ads
```

---

#### 2. brand_analytics

**Class**: `BrandAnalytics`
**Location**: `services/brand_analytics.py`
**Type**: Web Automation
**Purpose**: Amazon Brand Analytics reports collection

**Features**:
- Weekly report tracking
- Report deduplication
- Multiple data export formats

**Data Storage**: Google Sheets, BigQuery, PostgreSQL

**Usage**:
```bash
python main.py --user_id=X --service=brand_analytics --category=CATEGORY_NAME
```

---

#### 3. fulfillment

**Class**: `Fulfillment`
**Location**: `services/fulfillment.py`
**Type**: Web Automation
**Purpose**: FBA (Fulfillment by Amazon) reports

**Categories**:

| Category | Frequency | Description |
|----------|-----------|-------------|
| `manage_fba_inventory` | Daily | Inventory snapshots |
| `fba_inventory` | Daily | FBA inventory levels |
| `replacements` | Daily | Customer replacements |
| `fulfilled_shipments` | Daily | Shipment details |
| `reimbursements` | Monthly | Reimbursements |
| `inventory_surcharge` | Monthly (day 15) | Monthly fees |
| `storage_fees` | Monthly (day 15) | Storage fees |
| `shipment_detail` | Monthly | Shipment details |
| `order_detail` | Monthly | Order details |
| `promotions` | Daily | Promotions |
| `fba_customer_returns` | Daily | Returns |

**Data Flow**:
1. Navigate to report page
2. Generate/download report
3. Save to CSV
4. Upload to BigQuery/PostgreSQL

**Usage**:
```bash
python main.py --user_id=2 --service=fulfillment --category=manage_fba_inventory
```

---

#### 4. business_reports

**Class**: `BusinessReports`
**Location**: `services/business_reports.py`
**Type**: Web Automation
**Purpose**: Amazon Business Reports (sales, traffic)

**Categories**:
- `sales_traffic_daily`: Daily sales and traffic metrics

**Features**:
- Automated report download
- Data cleaning
- Log cleanup integration

**Usage**:
```bash
python main.py --user_id=3 --service=business_reports --category=sales_traffic_daily
```

---

#### 5. payments

**Class**: `Payments`
**Location**: `services/payments.py`
**Type**: Web Automation
**Purpose**: Financial transaction reports

**Categories**:
- `transaction`: Daily transaction reports

**Data Storage**: CSV, BigQuery, PostgreSQL

**Usage**:
```bash
python main.py --user_id=4 --service=payments --category=transaction
```

---

#### 6. awd

**Class**: `Awd`
**Location**: `services/awd.py`
**Type**: Web Automation
**Purpose**: Amazon Warehousing & Distribution reports

**Categories**:

| Category | Frequency | Format | Description |
|----------|-----------|--------|-------------|
| `storage` | Monthly (day 15) | XLSX | Storage reports |
| `processing` | Monthly (day 15) | XLSX | Processing fees |
| `transportation` | Monthly (day 15) | XLSX | Transportation costs |
| `inventory` | Daily | CSV | Inventory snapshots |
| `shipment_awd_inbound` | Daily | CSV | Inbound shipments |

**Usage**:
```bash
python main.py --user_id=2 --service=awd --category=inventory
```

---

#### 7. shipments

**Class**: `Shipments`
**Location**: `services/shipments.py`
**Type**: Web Automation
**Purpose**: Shipment tracking and management

**Schedule**: Weekly
**User**: user_id=2

**Usage**:
```bash
python main.py --user_id=2 --service=shipments
```

---

#### 8. support

**Class**: `Support`
**Location**: `services/support.py`
**Type**: Web Automation
**Purpose**: Customer support case management

**Note**: Not currently in active schedule

---

#### 9. datarova

**Class**: `Datarova`
**Location**: `services/datarova.py`
**Type**: Web Automation
**Purpose**: Integration with Datarova platform

**Schedule**: Daily (00:45-01:00)
**User**: user_id=0

**Usage**:
```bash
python main.py --user_id=0 --service=datarova
```

---

### API-Based Services

#### 10. api_ad

**Class**: `AmazonAD`
**Location**: `services/api_ad.py:27-153`
**Type**: API Integration
**Purpose**: Amazon Advertising API reports

**API Framework**: `ad-api` (Amazon Advertising API Python SDK)

**Authentication**:
- OAuth refresh token
- Client ID/Secret
- Profile ID

**Report Types**:
- Sponsored Products
- Sponsored Brands
- Sponsored Display

**Workflow**:
1. Create report via API (`create_report`)
2. Poll status until COMPLETED (`report_status`)
3. Download gzipped JSON report
4. Convert to DataFrame with snake_case columns
5. Save as CSV
6. Upload to PostgreSQL

**Data Transformation** (`api_ad.py:35-38`):
```python
def camel_to_snake(name: str) -> str:
    # Converts camelCase to snake_case
    # Example: campaignName -> campaign_name
```

**Usage**:
```bash
python main.py --user_id=11 --service=api_ad --period=30
```

**Schedule**: Daily (02:00-04:00)

---

#### 11. api_sp

**Class**: `AmazonSP`
**Location**: `services/api_sp.py:33-200`
**Type**: API Integration
**Purpose**: Amazon Selling Partner API reports

**API Framework**: `python-amazon-sp-api`

**Authentication**:
- LWA (Login with Amazon) credentials
- Refresh token
- Client ID/Secret

**Client Types**:
- **Reports**: Most report types
- **Orders**: Order-specific data

**Supported Report Types**:
- Inventory reports
- Sales reports
- Order reports
- Financial reports
- FBA reports

**Workflow**:
1. Either fetch existing reports or create new report
2. Poll report status
3. Get report document
4. Download and decompress
5. Parse (CSV/TSV/XML/JSON)
6. Transform and save

**Special Features**:
- XML parsing for specific report types
- Multi-format support
- Automatic date range calculation

**Usage**:
```bash
python main.py --user_id=X --service=api_sp --category=REPORT_TYPE
```

---

#### 12. brand_analytics_api

**Class**: `BrandAnalyticsAPI`
**Location**: `services/brand_analytics_api.py`
**Type**: API Integration
**Purpose**: Brand Analytics via SP-API

**Categories**:
- `asin`: ASIN-level analytics

**Schedule**: Daily (00:00-02:00)
**User**: user_id=10

**Usage**:
```bash
python main.py --user_id=10 --service=brand_analytics_api --category=asin
```

---

## Configuration System

### Config Architecture

**Location**: `settings/config.py:7-46`

**Pattern**: Singleton Configuration Object

**Initialization**:
1. Read all `.json` files from `settings/` directory
2. Convert to uppercase attribute names
3. Load environment variables from `.env`
4. Transform user list to dictionary indexed by user ID

**Key Paths**:
```python
config.project_path        # Project root
config.screenshots_path    # Browser screenshots
config.db_path            # SQLite database
config.reports_path       # Downloaded reports
config.logs_path          # Application logs
config.main_script_path   # main.py path
```

### Configuration Files

#### arguments.json
**Purpose**: CLI argument definitions
**Location**: `settings/arguments.json`

```json
[
  {"flag": "--user_id", "required": true},
  {"flag": "--service", "required": true},
  {"flag": "--category", "required": false},
  {"flag": "--report_type", "required": false},
  {"flag": "--period", "required": false}
]
```

---

#### schedule.json
**Purpose**: Job scheduling configuration
**Location**: `settings/schedule.json`
**Structure**: Array of job definitions

See [Scheduler System](#scheduler-system) for details.

---

#### users.json
**Purpose**: User account management
**Location**: `settings/users.json`

**Structure**:
```json
{
  "id": "user_id",
  "username": "account_name",
  "email": "email@example.com",
  "phone": "phone_number",
  "port": 9222,
  "ads": "url"
}
```

**User Mapping**:

| ID | Username | Purpose | Port |
|----|----------|---------|------|
| 0 | - | Datarova | - |
| 1 | brand_analytics | Brand Analytics | 9222 |
| 2 | logist | Fulfillment/AWD | 9223 |
| 3 | business_reports | Business Reports | 9224 |
| 4 | finance | Payments | 9225 |
| 10 | - | Brand Analytics API | - |
| 11 | - | Advertising API | - |

---

#### Other Configuration Files

| File | Purpose |
|------|---------|
| `api.json` | API endpoints |
| `api_ad.json` | AD API report configurations |
| `api_sp.json` | SP API report configurations |
| `url.json` | Service URLs |
| `headers.json` | HTTP headers |
| `google_credentials.json` | GCP service account |
| `google_sheets.json` | Google Sheets mappings |

---

## Database Layer

### SQLite (Primary Database)

**Location**: `database/database.py`
**Purpose**: Task tracking and scheduling state

**Key Tables**:
- `task`: Job execution history

**Key Operations**:

#### get_task (`database.py:45-71`)
- Retrieves last execution time for a task
- Used by scheduler to determine next run

#### update_task
- Records task execution
- Tracks status (started/completed/failed)

---

### BigQuery

**Location**: `database/big_query.py`
**Purpose**: Cloud data warehouse for analytics
**Usage**: Report data storage

---

### PostgreSQL

**Location**: `database/postgres_db.py`
**Purpose**: Relational database for structured data
**Usage**: Report data with schema validation

---

## Supporting Infrastructure

### Logging System

**Location**: `loggers/logger.py`

**Features**:
- Structured logging
- File and console output
- Log rotation

**Cleaner**: `loggers/cleaner.py`
- Automatic old log removal

---

### Notifications

**Location**: `notifications/telegram.py`
**Function**: `bot_task()`
**Purpose**: Telegram bot for task notifications

---

### Utilities

#### Decorators (`utils/decorators.py`)
- Exception handling
- Async exception handling

#### Authenticator (`utils/authenticator.py`)
- OTP generation
- 2FA handling

#### CAPTCHA Solver (`utils/captcha_solver.py`)
- Automated CAPTCHA solving

#### Google Sheets (`utils/google_sheets.py`)
- Data export to Google Sheets

---

## Scheduler Mechanism

### How the Scheduler Runs Services

#### Step-by-Step Flow

**1. Startup** (`scheduler.py:262-266`)

```python
scheduler = Scheduler()
scheduler.run()
  -> asyncio.run(self.execute())
```

**2. Job Creation** (`scheduler.py:172-235`)

```python
for schedule in config.SCHEDULE:
    if not schedule["enabled"]:
        continue

    # Parse arguments
    task = extract_args(schedule["args"])

    # Get last execution from database
    last_date = await db.get_task(task=task)

    # Calculate next execution time
    start_time = await self.set_exact_time(
        time_range=schedule["time_range"],
        last_date=last_date
    )

    # Add job
    self.scheduler.add_job(
        self.start_service,
        trigger=CronTrigger(**start_time, start_date=start_date),
        args=schedule["args"],
        kwargs={...},
        id=job_id
    )
```

**3. Time Randomization** (`scheduler.py:58-99`)

```python
# Select random time within range
time_range = [{"start": "00:00", "end": "02:00"}]

# Algorithm:
# 1. Check current time against ranges
# 2. If within range, use current time + random offset
# 3. If before range, use start + random offset
# 4. Random offset calculated: randint(1, delta_minutes)
# 5. Ensures each run is at different time
```

**4. Job Execution** (`scheduler.py:132-169`)

```python
async def start_service(self, *args, job_id, time_range, day, job_type):
    user_id = extract_user_id(args)

    # Ensure only one process per user
    async with self.lock[user_id]:
        while await self.active_process(user_id):
            await asyncio.sleep(60)

        # Execute service
        await asyncio.create_subprocess_exec(
            "poetry", "run", "python", config.main_script_path, *args
        )

        # Calculate next run time
        next_time = calculate_next_time(job_type, time_range, day)

        # Reschedule
        self.scheduler.reschedule_job(
            job_id,
            trigger=CronTrigger(**next_time, start_date=next_date)
        )
```

### Config File Usage

**Example from `schedule.json`**:

```json
{
  "type": "daily",
  "enabled": true,
  "time_range": [
    {"start": "00:30", "end": "00:45"},
    {"start": "06:00", "end": "06:15"},
    {"start": "10:00", "end": "10:15"}
  ],
  "args": [
    "--user=2",
    "--service=fulfillment",
    "--category=manage_fba_inventory"
  ]
}
```

**How It Works**:

1. Scheduler reads all entries from `schedule.json`
2. Filters by `enabled: true`
3. Parses `type`, `time_range`, `args`, optional `day`
4. Queries database for last execution of this exact task
5. Calculates next execution:
   - If never run before: Random time in first time_range today
   - If run today in time_range: Random time in next time_range
   - If all time_ranges exhausted today: Random time tomorrow in first range
   - For monthly with `day`: Schedules on specific day of month
6. Creates cron job with calculated time
7. When job fires:
   - Checks no other process for this user running
   - Executes: `poetry run python main.py --user=2 --service=fulfillment --category=manage_fba_inventory`
   - Reschedules for next occurrence

---

## Execution Flow

### Example: Daily Fulfillment Report

```
1. Scheduler (02:37 AM) - Random time in 00:30-00:45 range
   └─> Checks: No active process for user_id=2
   └─> Executes: poetry run python main.py --user=2 --service=fulfillment --category=manage_fba_inventory

2. main.py receives arguments
   └─> Parses: user_id=2, service=fulfillment, category=manage_fba_inventory
   └─> Creates: Fulfillment(user_id="2", category="manage_fba_inventory")
   └─> Calls: service.run()

3. Fulfillment.run()
   └─> Connects to browser (port 9223 for user_id=2)
   └─> Authenticates if needed
   └─> Navigates to FBA inventory page
   └─> Generates report
   └─> Downloads CSV
   └─> Saves to: reports/fulfillment/manage_fba_inventory_YYYY-MM-DD.csv
   └─> Uploads to BigQuery/PostgreSQL
   └─> Updates task status in SQLite

4. Scheduler
   └─> Calculates next run (tomorrow, random time 00:30-00:45)
   └─> Reschedules job
```

---

## Active Schedule

### Current Active Schedule (from `settings/schedule.json`)

#### Daily Tasks

| Time Range | User | Service | Category | Purpose |
|------------|------|---------|----------|---------|
| 00:45-01:00 | 0 | datarova | - | Datarova integration |
| 00:00-00:20 | 3 | business_reports | sales_traffic_daily | Sales/traffic metrics |
| 00:30-00:45, 06:00-06:15, 10:00-10:15 | 2 | fulfillment | manage_fba_inventory | FBA inventory |
| 00:30-00:45, 06:00-06:15, 10:00-10:15 | 2 | fulfillment | fba_inventory | FBA levels |
| 00:30-01:15 | 2 | fulfillment | replacements | Replacements |
| 01:01-01:30 | 2 | fulfillment | fulfilled_shipments | Shipments |
| 00:45-01:00 | 2 | fulfillment | promotions | Promotions |
| 00:45-01:00 | 2 | fulfillment | fba_customer_returns | Returns |
| 01:01-01:06 | 4 | payments | transaction | Transactions |
| 00:30-01:30 | 2 | awd | inventory | AWD inventory |
| 00:45-01:00 | 2 | awd | shipment_awd_inbound | Inbound shipments |
| 00:00-02:00 | 10 | brand_analytics_api | asin | ASIN analytics |
| 02:00-04:00 | 11 | api_ad | - | Ad reports |

#### Weekly Tasks

| Time Range | User | Service | Purpose |
|------------|------|---------|---------|
| 00:30-01:30 | 2 | shipments | Shipment tracking |

#### Monthly Tasks

| Time Range | User | Service | Category | Day | Purpose |
|------------|------|---------|----------|-----|---------|
| 01:00-11:00 | 2 | fulfillment | reimbursements | - | Reimbursements |
| 01:00-11:00 | 2 | fulfillment | inventory_surcharge | 15 | Surcharge fees |
| 01:00-11:00 | 2 | fulfillment | storage_fees | 15 | Storage fees |
| 01:00-11:00 | 2 | fulfillment | shipment_detail | - | Shipment details |
| 01:00-11:00 | 2 | fulfillment | order_detail | - | Order details |
| 01:00-11:00 | 2 | awd | storage | 15 | AWD storage |
| 01:00-11:00 | 2 | awd | processing | 15 | AWD processing |
| 01:00-11:00 | 2 | awd | transportation | 15 | AWD transport |

---

## Quick Start

### Running Scheduler

```bash
python scheduler.py
```

### Running Individual Service

```bash
python main.py --user_id=2 --service=fulfillment --category=manage_fba_inventory
```

### Environment Setup

1. Install dependencies:
```bash
poetry install
```

2. Configure `.env` file with credentials

3. Set up configuration files in `settings/`:
   - `users.json`
   - `schedule.json`
   - API credentials (`.env`)

4. Start scheduler:
```bash
python scheduler.py
```

---

## Summary

This is a **production-grade, enterprise-level automation system** for Amazon Seller Central data collection. The scheduler acts as the **central nervous system**, reading from `schedule.json` to orchestrate 12 different services, executing them at randomized times to collect various Amazon marketplace reports through both web automation and API integrations.

**Key Metrics**:
- **12 Services**: 9 web automation + 3 API integration
- **5 User Accounts**: Segregated by function
- **20+ Report Types**: Covering sales, fulfillment, advertising, analytics
- **3 Databases**: SQLite, BigQuery, PostgreSQL
- **Async Architecture**: High-performance concurrent execution
- **Smart Scheduling**: Time randomization to avoid detection