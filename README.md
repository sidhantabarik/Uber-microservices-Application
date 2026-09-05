# Uber-microservices-Application
Production-style Uber-inspired microservices application built with Spring Boot, Redis Geospatial, Kafka, and H2/MySQL. Implements real-time driver location tracking, nearest-driver matching, event-driven ride processing, and a complete ride lifecycle.

# 🚗 Uber-Microservices Application

A backend-focused **Uber-inspired ride-booking microservices application** built with **Java, Spring Boot, Redis Geospatial, Apache Kafka, and H2/MySQL**.

The project demonstrates how a ride-sharing platform can efficiently locate nearby drivers, process ride requests asynchronously, and manage the complete ride lifecycle using a distributed microservices architecture.

---

## 🎯 Project Overview

When a rider requests a ride, the system needs to identify an available driver near the pickup location quickly.

A simple database query such as:

```sql
SELECT * FROM drivers
WHERE latitude BETWEEN ?
AND longitude BETWEEN ?;
```

does not scale well for high-frequency location updates and large numbers of drivers.

This project uses **Redis Geospatial indexing** to efficiently store and query driver locations.

The ride-request workflow is handled asynchronously using **Apache Kafka**, while individual business responsibilities are separated into independent Spring Boot microservices.

### Core Technologies

* Java
* Spring Boot
* Spring Data JPA / Hibernate
* REST APIs
* Apache Kafka
* Redis Geospatial
* H2 Database
* MySQL
* Maven
* Zookeeper
* Git / GitHub

---

# 🏗️ Architecture

The application consists of three core microservices:

| Service            |   Port | Responsibility                                                                         |
| ------------------ | -----: | -------------------------------------------------------------------------------------- |
| `location-service` | `8082` | Stores and retrieves real-time driver locations using Redis Geospatial                 |
| `ride-service`     | `8083` | Creates rides, manages ride lifecycle, and publishes ride events                       |
| `matching-service` | `8084` | Consumes ride requests, finds nearby drivers, scores them, and assigns the best driver |

### High-Level Flow

```text
                    ┌──────────────────┐
                    │    Rider App     │
                    └────────┬─────────┘
                             │
                             │ Request Ride
                             ▼
                    ┌──────────────────┐
                    │   Ride Service   │
                    │     :8083        │
                    └────────┬─────────┘
                             │
                             │ ride.requested
                             ▼
                    ┌──────────────────┐
                    │      Kafka       │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Matching Service │
                    │     :8084        │
                    └────────┬─────────┘
                             │
                             │ Find Nearby Drivers
                             ▼
                    ┌──────────────────┐
                    │ Location Service │
                    │     :8082        │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │      Redis       │
                    │   GEO Index      │
                    └────────┬─────────┘
                             │
                             │ Nearby Drivers
                             ▼
                    ┌──────────────────┐
                    │ Matching / Score │
                    │     Driver       │
                    └────────┬─────────┘
                             │
                             │ ride.matched
                             ▼
                    ┌──────────────────┐
                    │      Kafka       │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   Ride Service   │
                    │ Update Ride      │
                    └──────────────────┘
```

---

# 🔄 End-to-End Ride Flow

### 1. Driver Location Update

Driver sends current GPS coordinates to the `location-service`.

```text
Driver App
    ↓
Location Service
    ↓
Redis GEOADD
```

Redis stores the driver's longitude, latitude, and driver ID in a geospatial index.

---

### 2. Rider Requests Ride

The rider sends pickup and destination information to `ride-service`.

```text
Rider
  ↓
Ride Service
  ↓
Create Ride
  ↓
Publish ride.requested
  ↓
Kafka
```

The ride initially enters:

```text
REQUESTED
```

---

### 3. Matching Service Consumes Event

`matching-service` consumes the `ride.requested` Kafka event.

It requests nearby drivers from `location-service`.

```text
Kafka
  ↓
Matching Service
  ↓
Location Service
  ↓
Redis GEO Search
  ↓
Nearby Drivers
```

---

### 4. Driver Matching

The matching service evaluates available drivers based on factors such as:

* Distance from pickup point
* Driver rating
* Matching score

The best candidate is selected.

```text
Nearby Drivers
      ↓
Calculate Score
      ↓
Rank Drivers
      ↓
Select Best Driver
```

---

### 5. Ride Match Event

After selecting the driver:

```text
Matching Service
      ↓
ride.matched
      ↓
Kafka
      ↓
Ride Service
      ↓
Assign Driver
```

The ride status becomes:

```text
ACCEPTED
```

---

# 🚦 Ride State Machine

The ride follows a defined lifecycle:

```text
REQUESTED
    ↓
MATCHING
    ↓
ACCEPTED
    ↓
STARTED
    ↓
COMPLETED
```

This prevents arbitrary ride-state transitions and provides a simple example of a **state-machine-based business workflow**.

---

# 📍 Redis Geospatial

Real-time driver locations are maintained in Redis using geospatial commands.

Example:

```text
GEOADD drivers:locations longitude latitude driverId
```

Nearby-driver queries can then be performed using Redis geospatial capabilities.

Useful commands:

```text
ZRANGE drivers:locations 0 -1
```

View stored drivers.

```text
GEOPOS drivers:locations driver:1
```

Get the current coordinates of a driver.

```text
GEODIST drivers:locations driver:1 driver:2 km
```

Calculate distance between two drivers.

---

# 📨 Kafka Event-Driven Communication

Kafka is used to decouple ride processing from driver matching.

Example event flow:

```text
Ride Service
     │
     │ ride.requested
     ▼
   Kafka
     │
     ▼
Matching Service
     │
     │ ride.matched
     ▼
   Kafka
     │
     ▼
Ride Service
```

This allows the matching process to operate asynchronously instead of forcing the ride service to perform the complete matching operation synchronously.

---

# 🗄️ Database

The ride service uses a relational database for persistent ride information.

For local development, **H2 in-memory database** can be used so the application can run without requiring an external MySQL server.

Example local configuration:

```yaml
spring:
  datasource:
    url: jdbc:h2:mem:ride_db;DB_CLOSE_DELAY=-1;MODE=MySQL
```

MySQL can be used when persistent database storage is required.

---

# 🖥️ Local Development Setup

## Prerequisites

Install/configure:

```text
Java
Maven
Redis
Apache Kafka
Apache Zookeeper
Git
```

The local Windows setup used for this project expects:

```text
C:\Redis
C:\kafka
```

---

# ▶️ Start Infrastructure

## 1. Start Redis

```powershell
C:\Redis\redis-server.exe
```

Default port:

```text
6379
```

---

## 2. Start Zookeeper

```powershell
C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties
```

Default port:

```text
2181
```

---

## 3. Start Kafka

Open another terminal:

```powershell
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties
```

Default port:

```text
9092
```

Wait for Kafka to finish starting before launching the microservices.

---

# 🚀 Start Microservices

Open separate terminals.

## Location Service

```powershell
cd location-service
mvn spring-boot:run
```

Runs on:

```text
http://localhost:8082
```

---

## Ride Service

```powershell
cd ride-service
mvn spring-boot:run
```

Runs on:

```text
http://localhost:8083
```

---

## Matching Service

```powershell
cd matching-service
mvn spring-boot:run
```

Runs on:

```text
http://localhost:8084
```

---

# 🧪 End-to-End Testing

## Step 1 — Add Driver Locations

### Driver 1

```http
POST http://localhost:8082/api/v1/locations/drivers/update
```

```json
{
  "driverId": "driver:1",
  "latitude": 12.9716,
  "longitude": 77.5946
}
```

### Driver 2

```http
POST http://localhost:8082/api/v1/locations/drivers/update
```

```json
{
  "driverId": "driver:2",
  "latitude": 12.9800,
  "longitude": 77.5800
}
```

### Driver 3

```http
POST http://localhost:8082/api/v1/locations/drivers/update
```

```json
{
  "driverId": "driver:3",
  "latitude": 12.9600,
  "longitude": 77.6100
}
```

---

# 🚕 Step 2 — Request a Ride

```http
POST http://localhost:8083/api/v1/rides/request
```

```json
{
  "riderId": "rider:1",
  "pickupLatitude": 12.9716,
  "pickupLongitude": 77.5946,
  "pickupAddress": "MG Road, Bangalore",
  "dropLatitude": 12.9352,
  "dropLongitude": 77.6245,
  "dropAddress": "Koramangala, Bangalore"
}
```

The ride service creates the ride and publishes a ride-request event to Kafka.

---

# 🔎 Step 3 — Check Ride Status

```http
GET http://localhost:8083/api/v1/rides/{rideId}
```

After successful matching, the response should contain an assigned driver and a status such as:

```text
ACCEPTED
```

---

# ▶️ Step 4 — Start Ride

```http
PUT http://localhost:8083/api/v1/rides/{rideId}/start
```

Expected state:

```text
STARTED
```

---

# ✅ Step 5 — Complete Ride

```http
PUT http://localhost:8083/api/v1/rides/{rideId}/complete
```

Expected state:

```text
COMPLETED
```

---

# 📜 Step 6 — Rider Ride History

```http
GET http://localhost:8083/api/v1/rides/rider/{riderId}
```

This retrieves the rides associated with the rider.

---

# 🔍 Verify Redis

Open Redis CLI:

```powershell
redis-cli
```

Or, depending on the local Redis installation:

```powershell
C:\Redis\redis-cli.exe
```

View stored drivers:

```text
ZRANGE drivers:locations 0 -1
```

Check a driver's position:

```text
GEOPOS drivers:locations driver:1
```

Check distance:

```text
GEODIST drivers:locations driver:1 driver:2 km
```

---

# 🧠 Key Concepts Demonstrated

### Microservices

Independent services with clearly separated responsibilities.

### Redis Geospatial

Efficient location-based searching for nearby drivers.

### Apache Kafka

Asynchronous event-driven communication between services.

### REST APIs

Service-to-service and client-to-service communication.

### Ride State Machine

```text
REQUESTED → MATCHING → ACCEPTED → STARTED → COMPLETED
```

### Driver Matching

Nearby drivers are retrieved and evaluated using a scoring mechanism based on factors such as distance and rating.

### Spring Data JPA / Hibernate

Used for relational persistence and object-relational mapping.

### H2 / MySQL

H2 provides convenient local development, while MySQL can be used for persistent storage.

---

# 📌 Why Redis Instead of Normal SQL for Driver Location?

Driver locations can change frequently.

If thousands of drivers continuously update their latitude and longitude, repeatedly querying a relational database for geographic proximity can become expensive.

Redis provides an in-memory geospatial index designed for location-based operations.

Conceptually:

```text
Traditional SQL

Driver Table
     ↓
Scan / Filter
     ↓
Calculate Distance
     ↓
Sort
     ↓
Nearest Driver
```

With Redis Geospatial:

```text
Driver Location
      ↓
Redis GEO Index
      ↓
Nearby Search
      ↓
Candidate Drivers
```

The important architectural idea is **not that SQL cannot handle location queries**, but that a specialized geospatial index such as Redis can make high-frequency proximity lookups much more efficient.

---

# 🏆 What This Project Demonstrates

This project provides practical exposure to:

```text
Spring Boot
     ↓
Microservices
     ↓
REST APIs
     ↓
Kafka
     ↓
Event-Driven Architecture
     ↓
Redis Geospatial
     ↓
Driver Matching
     ↓
Ride State Management
     ↓
Database Persistence
```

It is designed as a learning project to demonstrate the backend architecture and core engineering concepts behind a ride-sharing system.

---

# 📂 Project Structure

```text
Uber-Microservices/
│
├── location-service/
│   └── src/
│
├── ride-service/
│   └── src/
│
├── matching-service/
│   └── src/
│
├── pom.xml
├── docker-compose.yml
└── README.md
```

---

# 🔮 Possible Future Improvements

The current implementation can be extended with:

* Driver availability status
* Driver acceptance/rejection
* Ride cancellation
* Driver authentication
* Rider authentication
* JWT / Spring Security
* API Gateway
* Service Discovery
* Circuit Breaker
* Redis caching
* Kafka retry and dead-letter topics
* Idempotent event processing
* Distributed tracing
* Centralized logging
* Docker/Kubernetes deployment
* Real-time driver tracking using WebSocket
* Surge pricing
* Payment service
* Notification service

---

# ⚠️ Disclaimer

This is an **Uber-inspired educational backend project** created to demonstrate microservices, event-driven architecture, geospatial indexing, and distributed system concepts.

It is not affiliated with or an official implementation of Uber.

---

## 👨‍💻 Author

**Sidhanta Barik**

Java Full Stack Developer | Backend-Focused

**Core Technologies:**

```text
Java
Spring Boot
Microservices
REST APIs
Kafka
Redis
Spring Security
JWT
JPA / Hibernate
MySQL
H2
```

