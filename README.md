# 🧠 MiniSQL: Eventual Durability-Aware SQL Engine

This project was developed as part of the **Database Internals** module to explore and implement **eventual durability** in a custom-built database. Our team built a lightweight SQL database from scratch in Java, named **MiniSQL**, and incorporated an experimental durability model inspired by cutting-edge database research.

Our core idea: **Decouple commit from durability** to improve throughput for specific transaction types. This allows users to choose between `FAST` (eventually durable) and `SAFE` (strongly durable) transactions.

---

## 📌 Features

- ✅ SQL Parser with support for:
  - `CREATE`, `INSERT`, `UPDATE`, `DELETE`, `SELECT`
- 🧠 Write-Ahead Logging (WAL)
- 📁 Page-based storage engine
- 🗃️ LRU buffer pool for in-memory page management
- 💾 Durable and Eventually Durable transaction models
- 💡 Support for hints: `/*+ FAST */` vs `/*+ SAFE */`
- 📈 Performance benchmark suite for write-intensive workloads

---

## 🛠️ Architecture Overview

### Engine (CLI)
- Interactive shell to execute SQL queries
- Handles user inputs and recovery
- Initiates safe shutdown procedures

### Storage Layer
- Page abstraction for on-disk data
- LRU buffer pool management
- Write-Ahead Log (WAL) for recovery

### Durability Model
- **FAST Transactions**: Commit instantly without flushing WAL
- **SAFE Transactions**: Commit only after WAL + page flush
- WAL flushing is handled by a background thread for FAST transactions

---

## 🧪 Benchmarks

To test eventual durability:

```sql
/*+ FAST */
UPDATE kv SET v = v + 1 WHERE k = <random_key>;
```
```sql
/*+ SAFE */
UPDATE kv SET v = v + 1 WHERE k = <random_key>;
```

## 🧪 Benchmarking Performance

We evaluated performance using:

- 🧵 8 parallel threads  
- 🗃️ 256-row preloaded key-value table  
- ⏱️ 5-second write-intensive workload  

**Metrics Collected:**

- ✅ Transactions committed  
- ⏱️ Latency (µs)  
- 🚀 Throughput (txns/sec)  

---

## 🚀 Getting Started

### Prerequisites

- Java 11 or higher
- Maven (for build and dependency management)

### Build and Run

```bash
git clone https://github.com/RadCod3/customdb.git
cd customdb
mvn clean compile exec:java
```
### 📄 Example Usage

```sql
CREATE TABLE kv (k INT PRIMARY KEY, v INT);
INSERT INTO kv VALUES (1, 100);
UPDATE kv SET v = v + 1 WHERE k = 1; /*+ FAST */
```
---
## 👥 Contributors

- [**Ampavila I.A.**](https://github.com/inuka-00)
- [**Kurukulasooriya K.V.R.**](https://github.com/Zury7)
- [**Samrakoon R.S.A.**](https://github.com/RadCod3)
- [**Subodha K.R.A.**](https://github.com/ashaneo)
- [**Wijesinghe U.M.**](https://github.com/Udaramalinda)
---

## 📄 Related Work

We also implemented a similar **eventual durability** mechanism in a fork of **MySQL 8.0.36**:

🔗 [MySQL Fork with Eventual Durability](https://github.com/inuka-00/mysql-server/tree/ed-mysql-8.0.36)

