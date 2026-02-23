# Ecommerce ETL Pipeline (PySpark + PostgreSQL)

## 📌 Project Overview

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using:

- PySpark for data processing
- PostgreSQL as data warehouse
- Star schema design (Fact + Dimension tables)
- UPSERT logic using staging tables

---

## 🏗 Architecture

CSV Files → PySpark → Transformations → Staging Tables → PostgreSQL Warehouse

---

## 🗄 Database Schema

### Fact Table
- fact_orders

### Dimension Tables
- dim_customers
- dim_products

---

## ⚙️ Tech Stack

- Python
- PySpark
- PostgreSQL
- SQL
- Git & GitHub

---

## 🚀 How to Run

1. Set environment variable:

```powershell
$env:DB_PASSWORD="YOUR_PASSWORD"