# Billing Job

A Spring Batch application for processing telecom billing data, calculating billing totals, and generating reports.

## Overview

This application processes telecom billing data from CSV files, calculates billing totals based on data usage, call duration, and SMS count, and generates reports for billing records that exceed a configurable spending threshold.

## Technologies Used

- Java 21
- Spring Boot 3.5.3
- Spring Batch
- PostgreSQL
- Docker
- Gradle

## Project Structure

- `src/main/java/self/development/batch/billingjob/`
  - `BillingJobApplication.java` - Main application entry point
  - `config/BillingConfig.java` - Spring Batch job configuration
  - `job/BillingJob.java` - Job implementation
  - `model/` - Data models
    - `BillingData.java` - Input data model
    - `ReportingData.java` - Output data model
  - `processor/BillingDataProcessor.java` - Data processing logic
  - `service/PricingService.java` - Service for pricing calculations
  - `task/FilePreparationTasklet.java` - Tasklet for file preparation
  - `exception/PricingException.java` - Custom exception
  - `skip/BillingDataSkipListener.java` - Skip listener for error handling
- `src/main/resources/`
  - `application.yaml` - Application configuration
  - Sample CSV files for testing
- `infra/`
  - `docker-compose.yaml` - Docker Compose configuration for PostgreSQL

## Setup Instructions

### Prerequisites

- Java 21
- Docker and Docker Compose

### Database Setup

1. Start the PostgreSQL database using Docker Compose:

```bash
cd infra
docker-compose up -d
```

This will start a PostgreSQL instance with the following configuration:
- Database: mydatabase
- Username: myuser
- Password: mypassword
- Port: 5432

### Building the Application

```bash
./gradlew build
```

## Running the Application

```bash
./gradlew bootRun --args="--input.file=src/main/resources/telecom_data.csv"
```

### Job Parameters

The application accepts the following job parameters:

- `input.file` (required) - Path to the input CSV file
- `output.file` (optional) - Path to the output report file (default: staging/processed-report.csv)
- `data.year` (optional) - Year for filtering data
- `data.month` (optional) - Month for filtering data
- `skip.file` (optional) - Path to file for storing skipped records

## Job Flow

The main job consists of three steps:

1. **Copy Step** - Prepares files using a tasklet
2. **File Ingestion Step** - Reads billing data from CSV files and writes to the database
   - Skips invalid records (up to 10)
   - Logs skipped records
3. **Processor Step** - Reads data from the database, processes it, and writes reports
   - Calculates billing totals based on usage metrics
   - Filters out records below the spending threshold
   - Handles pricing exceptions with retry logic

## Testing

Run the tests using:

```bash
./gradlew test
```

## Sample Data Format

Input CSV files should have the following format:

```
dataYear,dataMonth,accountId,phoneNumber,dataUsage,callDuration,smsCount
2023,1,1001,555-1234,10.5,120,45
```

## Configuration

The application can be configured through `application.yaml` or environment variables:

- `spring.application.name` - Application name
- `spring.datasource.*` - Database connection settings
- `spring.batch.job.name` - Default job name
- `spring.batch.job.enabled` - Auto-start job on startup
- `spring.cellular.pricing.*` - Pricing factors for different services
- `spring.cellular.spending.threshold` - Threshold for filtering billing records
