# 🏙️ NYC Smart City Data Pipeline

> **Case Study 2 · Unit 5.2 DataFrames · PySpark + Pandas**

An end-to-end **interactive PySpark pipeline visualizer** built as a single-page web application. It simulates the complete lifecycle of a real smart-city data platform — ingesting 6 live NYC data sources, processing 3M+ rows across 4 CPU cores, answering 10 analytical queries, and achieving up to **8.4× speedup** through performance optimizations.

---

## 👥 Team

| Name | Roll No |
|---|---|
| Varun Murugan | 124A8118 |
| Rankin Yanu | 124A8124 |
| Navin Nadar | 225A8134 |
| Abhay Hanchate | 225A8129 |

---

## 📋 Table of Contents

- [Project Overview](#-project-overview)
- [Live Stats at a Glance](#-live-stats-at-a-glance)
- [File Structure](#-file-structure)
- [How to Run](#-how-to-run)
- [Application Sections](#-application-sections)
- [Data Sources](#-data-sources)
- [PySpark Concepts Demonstrated](#-pyspark-concepts-demonstrated)
- [10 Analytical Queries](#-10-analytical-queries)
- [5 Performance Optimizations](#-5-performance-optimizations)
- [Key Findings](#-key-findings)
- [Tech Stack](#-tech-stack-web-app)
- [Design System](#-design-system)

---

## 🎯 Project Overview

This project simulates what a real **NYC Smart City data team** would build using **Apache Spark (PySpark)**. The web app is a fully interactive, animated visualizer that walks through every stage of the pipeline — from raw data ingestion to final insights — using real NYC open data figures.

### What it demonstrates

| Concept | Implementation |
|---|---|
| **Distributed Processing** | 2.96M taxi rows split across 4 partitions / 4 CPU cores |
| **Lazy Evaluation** | Timing proof: 0.04s to build plan vs 1.24s to run `count()` |
| **Narrow vs Wide Transforms** | Visual separation of `filter/select` vs `groupBy/join/orderBy` |
| **DAG Visualization** | Animated particle flow across the full pipeline graph |
| **Live Data** | NYC DOT traffic API fetched fresh on every notebook run |
| **Nested JSON Flattening** | `arrays_zip + explode` converts Open-Meteo parallel arrays to rows |
| **5 Optimizations** | Cache · Broadcast Join · Shuffle Partitions · AQE · Parquet |
| **Pandas vs Spark** | Side-by-side benchmark at 100k, 500k, 1M, and 3M rows |

---

## 📊 Live Stats at a Glance

| Metric | Value |
|---|---|
| 🚕 Taxi Trips | **2,964,624** rows (January 2024) |
| 📞 311 Requests | **249,832** citizen complaints |
| 🗂️ Data Sources | **6** (Parquet, CSV, JSON, REST APIs, Live) |
| 📝 Queries | **10** analytical queries across 4 domains |
| ⚡ Best Speedup | **8.4×** (Parquet vs CSV) |
| 🖥️ CPU Cores | **4** (local[*] mode) |
| 🔥 Peak Hour | **6 PM** — 158,000 trips/hr |
| 🗺️ Busiest Hub | **Midtown Center** — 89,432 pickups |

---

## 📁 File Structure

```
DEEEEE/
├── index.html             # Full single-page application (660 lines)
├── style.css              # Complete design system & all component styles
├── app.js                 # All UI logic, renderers, charts, animations (~1070 lines)
├── dag.js                 # DAG canvas renderer with animated particle flow
├── data.js                # All simulated pipeline data (502 lines of structured constants)
├── README.md              # This file
└── SOLUTION_EXPLAINED.md  # Deep-dive explanation of Spark concepts & solution
```

### Role of each file

| File | Purpose |
|---|---|
| `index.html` | HTML skeleton: nav, all 12 sections, Chart.js import, loading screen |
| `style.css` | CSS variables (design tokens), glassmorphism cards, animated components, responsive grid |
| `app.js` | Section renderers, fetch simulator, query charts (Chart.js), benchmark charts, pipeline stepper |
| `dag.js` | Canvas-based DAG with SVG-style edges, animated data-flow particles, hover tooltips |
| `data.js` | Single source of truth: all schemas, sample rows, query results, optimization timings, insights |

---

## 🚀 How to Run

This is a **pure front-end project** — no build step, no server required.

```bash
# Option 1: Just open the file
open index.html        # macOS
start index.html       # Windows
xdg-open index.html    # Linux

# Option 2: Local dev server (recommended to avoid CORS issues)
npx serve .
# or
python -m http.server 8080
```

Then navigate to `http://localhost:8080` (or just open `index.html` directly).

> **Dependencies loaded via CDN:**
> - [Chart.js 4.4.3](https://www.chartjs.org/) — all query charts and benchmark graphs
> - [Google Fonts](https://fonts.google.com/) — Inter, JetBrains Mono, Outfit

---

## 🗂️ Application Sections

### 1. Hero — Landing Page

The landing section greets the user with animated count-up numbers, a pipeline stage preview strip, and two CTAs. The background has a living canvas animation of 120 slowly pulsing dots.

**Animated stats:** 2.96M, 249k, 6 sources, 10 queries, 8.4× speedup, 4 cores — all count up on scroll.

**Pipeline preview strip:**
```
📥 Ingest → 🔧 Clean → 🔀 Distribute → 🔍 Query → ⚡ Optimize → 📊 Output
```

---

### 2. Spark Map — Complete API Reference

A visual map of **every PySpark API call** used in the pipeline, organized into 8 color-coded categories:

| Category | Color | Examples |
|---|---|---|
| SETUP | Green | `SparkSession.builder`, `.getOrCreate()` |
| READER | Cyan | `spark.read.parquet()`, `spark.read.csv()`, `spark.read.json()` |
| Narrow Transformations | Purple | `filter()`, `select()`, `withColumn()`, `fillna()` |
| Wide Transformations | Amber | `groupBy()`, `join()`, `orderBy()`, `pivot()` |
| Actions | Red | `count()`, `show()`, `toPandas()`, `write.parquet()` |
| Window Functions | Pink | `row_number()`, `dense_rank()`, `sum().over()` |
| Spark SQL | Cyan | `createOrReplaceTempView()`, `spark.sql()` |
| Optimizations | Amber | `cache()`, `broadcast()`, AQE, Parquet format |

Below the cards, a **30-row API reference table** lists every method with its type, where it's used, and what it does in this specific project.

---

### 3. DAG — Pipeline Architecture

An interactive **canvas-rendered DAG** showing the full data flow from raw sources to outputs:

- **Animated particles** travel along edges to represent data moving through the pipeline
- **Hover any node** to see a tooltip with details (rows, operation type, Spark stage)
- **Color-coded nodes:** Cyan = Data Sources, Purple = Spark Engine stages, Amber = Outputs
- **Narrow vs Wide cards** explain the transformation types with real operation tags

---

### 4. Step 1 — SparkSession Setup

Shows the SparkSession configuration with a code block:

```python
spark = (SparkSession.builder
  .appName("SmartCityNYC")
  .master("local[*]")          # all CPU cores
  .config("spark.driver.memory", "4g")
  .config("spark.sql.adaptive.enabled", True)
  .config("spark.sql.adaptive.coalescePartitions.enabled", True)
  .getOrCreate())
```

Also includes an **architecture diagram** showing how `local[*]` mode works: one driver distributing tasks to 4 CPU cores (Core 0–3), each processing one partition.

---

### 5. Live Fetch Simulator

The most interactive section. Select any of the 6 data sources and click **▶ Run Fetch** to simulate the complete fetch → parse → DataFrame creation pipeline in an animated terminal.

**For each source, you see:**
- HTTP GET request URLs with parameters
- Download/parse progress with timestamps
- PySpark ingestion code appearing line by line
- Spark job metadata: Job #, Stages, Tasks, Partitions, Rows, Exec Time

**After the simulation completes, 4 output tabs appear:**

| Tab | Shows |
|---|---|
| **Schema** | Full `printSchema()` output with typed column badges |
| **Data Preview** | `df.show(3)` — 3 sample rows in a formatted table |
| **Stats** | Rows, columns, file size, partitions, tasks, read time |
| **PySpark Code** | The actual ingestion code for that source |

---

### 6. Step 2 — Data Ingestion

Clickable cards for all 6 data sources. Clicking any card updates the schema panel and data preview. Also includes the **JSON flattening diagram** for the Open-Meteo API:

```
Raw JSON nested arrays → arrays_zip → explode → 744-row flat Spark DataFrame
```

---

### 7. Step 3 — Distributed Processing

- **Partition grid**: 4 partition cards with animated count-up row numbers (741,156 each)
- **Core activity pills**: Core 0–3 shown as active with pulsing green LEDs
- **Job stages visualization**: Shows Stage 1 (FileScan → Filter → partial HashAgg, 4 tasks) → Shuffle → Stage 2 (final HashAgg → Sort, 8 tasks)
- **Physical plan**: The actual `explain()` output showing `Exchange`, `HashAggregate`, `FileScan` with predicate pushdown

---

### 8. Step 4 — Cleaning & Transformation

**Taxi Trips Cleaning:**

| Rule | Rows Removed | % |
|---|---|---|
| Pickup inside Jan 2024 | 18 | 0.001% |
| Duration 1–180 min | 4,821 | 0.16% |
| Distance 0–100 miles | 61,240 | 2.07% |
| Fare & total > 0 | 12,834 | 0.43% |
| Passengers 1–6 | 28,442 | 0.96% |
| Speed ≤ 80 mph | 28,904 | 0.98% |
| **Total removed** | **136,259** | **4.6%** |

Result: **2,828,365 clean rows** (95.4% retained)

Enriched columns added via `withColumn`: `pickup_hour`, `pickup_date`, `day_of_week`, `is_weekend`, `tip_pct`, `payment_method`

**Lazy Evaluation Proof (animated timeline):**
```
Define cleaning chain (0.04s) → Add withColumn (0.18s) → count() ACTION (1.24s)
                                                          ↑ Spark runs everything here
```

---

### 9. Step 5 — 10 Analytical Queries

10 expandable query cards, each with a live Chart.js visualization. See [10 Analytical Queries](#-10-analytical-queries) section for full details.

---

### 10. Step 6 — Performance Optimizations

5 optimization cards with animated baseline vs optimized progress bars. See [5 Performance Optimizations](#-5-performance-optimizations) for full details.

---

### 11. Step 7 — Benchmark & Analysis

Two Chart.js charts:

**Pandas vs PySpark — same job at different data sizes:**

| Rows | Pandas | Spark |
|---|---|---|
| 100,000 | 0.38s | 1.92s |
| 500,000 | 1.82s | 2.14s |
| 1,000,000 | 3.91s | 2.48s |
| 2,964,624 | 11.24s | 3.12s |

**Core Scaling — Amdahl's Law:**

| Cores | Time | Speedup |
|---|---|---|
| 1 | 8.4s | 1.0× |
| 2 | 5.2s | 1.6× |
| 4 | 3.1s | 2.7× |

> 4 cores → only 2.7× speedup (not 4×) because file I/O and task scheduling are fixed overheads.

Also includes a **Full Pipeline Walkthrough** — an interactive step-by-step stepper card.

---

### 12. Smart City Insights

8 insight cards with key findings, a tools & technologies table, and the team card.

---

## 📦 Data Sources

| # | Source | Format | Rows | Size | API |
|---|---|---|---|---|---|
| 1 | NYC Yellow Taxi Trips (Jan 2024) | **Parquet** | 2,964,624 | 50 MB | TLC Open Data |
| 2 | Taxi Zone Lookup | CSV | 265 | 20 KB | TLC (file download) |
| 3 | 311 Service Requests (Jan 2024) | CSV (API pages) | 249,832 | 78 MB | Socrata REST API |
| 4 | Air Quality — Hourly | JSON (nested) | 744 | 300 KB | Open-Meteo (free, no key) |
| 5 | Weather — Hourly | JSON (nested) | 744 | 200 KB | Open-Meteo (free, no key) |
| 6 | Live Traffic Speeds | JSON (real-time) | ~148 sensors | 50 KB | NYC DOT API (live) |

**Why these sources?**
- **Mix of all formats**: Parquet, CSV, JSON, REST APIs, paged APIs, live API
- **Different sizes**: 3M rows down to 265 rows — demonstrates broadcast join
- **Same time period**: January 2024 → enables joins by hour and date
- **Cross-domain**: Mobility + Citizen Services + Environment + Real-time Traffic

---

## 🔥 PySpark Concepts Demonstrated

### Core Architecture
- `SparkSession.builder` with `local[*]`, `driver.memory`, AQE config
- Driver + 4 executor cores, one partition per core
- Spark UI at `:4040` — Jobs, Stages, Storage, SQL, Executors tabs

### Transformations

| Type | Operations Used |
|---|---|
| **Narrow** | `filter()`, `select()`, `withColumn()`, `fillna()`, `drop()`, `cast()`, `alias()` |
| **Wide (Shuffle)** | `groupBy().agg()`, `join()`, `orderBy()`, `distinct()`, `repartition()`, `pivot()` |
| **Window Functions** | `row_number()`, `dense_rank()`, `sum().over()`, `Window.partitionBy().orderBy()` |
| **Special** | `arrays_zip()`, `explode()`, `create_map()`, `try_cast()`, `dropDuplicates()` |

### Actions
`count()`, `show()`, `toPandas()`, `collect()`, `write.parquet()`, `write.csv()`, `corr()`

### Lazy Evaluation
Building the full cleaning DAG takes ~0.04s; running `count()` takes ~1.24s — because Spark only executes when an action is called.

### Spark SQL
Q1 is implemented in both DataFrame API and SQL:

```python
trips.createOrReplaceTempView("trips")
result_sql = spark.sql("""
    SELECT hour(tpep_pickup_datetime) AS pickup_hour,
           COUNT(*) AS trip_count,
           AVG(fare_amount) AS avg_fare
    FROM trips
    GROUP BY 1 ORDER BY 1
""")
```

Both produce identical results because they use the same Catalyst optimizer.

### File Formats
- **CSV** → typed with explicit `StructType` schema (no inferSchema overhead)
- **Parquet** → carries its own schema; columnar; column pruning + predicate pushdown
- **JSON** → `multiLine=True`; nested arrays flattened with `arrays_zip + explode`

---

## 📊 10 Analytical Queries

### 🚕 Q1 — When does the city move?
**Operations:** `groupBy("pickup_hour").agg(count, avg).orderBy` + same in Spark SQL  
**Result:** Peak hour is **18:00** with 158,000 trips. Avg fare peaks at ~$20 during evening rush.  
**Chart:** Dual-axis bar (trips) + line (avg fare) by hour 0–23

### 🚕 Q2 — Busiest Mobility Hubs
**Operations:** `broadcast join` trips × zones on `LocationID`, `groupBy(Zone, Borough).count().orderBy(desc)`  
**Result:** Midtown Center — 89,432 pickups; Upper East Side N — 76,821; JFK Airport — 68,234  
**Chart:** Horizontal bar of top 10 zones, color-coded by borough

### 🚕 Q3 — Weekly Heatmap
**Operations:** `groupBy("day_of_week").pivot("pickup_hour").count()` → 7×24 matrix  
**Result:** Weekday morning/evening peaks; Saturday/Sunday late-night spike after midnight  
**Chart:** Heatmap rendered via Chart.js

### 🏛️ Q4 — Top Complaint Types
**Operations:** `groupBy("complaint_type").count()`, `sum().over(Window)` for share %  
**Result:** HEAT/HOT WATER leads — 38,492 requests (15.9% of all complaints)  
**Chart:** Horizontal bar with percentage overlay

### 🏛️ Q5 — Agency Response Speed
**Operations:** `groupBy("agency").agg(percentile_approx(resolution_hours, 0.5), avg)`  
**Result:** NYPD fastest at 4.2 hrs median; DOB slowest at 120.8 hrs (5 days!)  
**Chart:** Grouped bar — median vs average per agency

### 🏛️ Q6 — Top 3 Complaints per Borough
**Operations:** `groupBy(borough, complaint_type).count()`, `dense_rank().over(Window.partitionBy("borough"))`  
**Result:** Manhattan & Brooklyn → HEAT/HOT WATER #1; Queens/Staten Island → Blocked Driveway #1  
**Chart:** Per-borough table with rank badges

### 🌫️ Q7 — Traffic vs Air Pollution
**Operations:** `date_trunc("hour")`, `join(air_quality on hour)`, `corr("trips", "no2")`  
**Result:** r = **+0.74** for NO₂, 0.68 for PM2.5, 0.71 for CO — traffic strongly drives pollution  
**Chart:** Bar (trip counts) + line (NO₂) by hour of day

### 🌦️ Q8 — Cold Days → Heating Complaints
**Operations:** Daily HEAT/HOT WATER 311 count, `join(weather on date)`, `corr("temp", "complaints")`  
**Result:** r = **−0.89** — coldest days (-5°C) → 1,842 heating complaints  
**Chart:** Scatter plot: temperature vs complaint count

### 🚦 Q9 — Live Traffic Snapshot
**Operations:** `groupBy("borough").agg(count, avg, min)`, `orderBy("speed_mph").limit(10)`  
**Result:** Manhattan slowest at 12.4 mph; Staten Island fastest at 45.8 mph  
**Chart:** Borough speed cards + slowest 5 roads table

### 🏙️ Q10 — Borough Scorecard
**Operations:** `spark.createDataFrame` (census), 4× `left join` on borough, per-1,000-people rates  
**Result:** Integrated view combining mobility, services, and traffic for all 5 boroughs  
**Chart:** 5-column scorecard table with color-coded metrics

---

## ⚡ 5 Performance Optimizations

| # | Optimization | Baseline | Optimized | Speedup |
|---|---|---|---|---|
| **O1** | **Caching** — `trips.cache()` materializes clean DF in memory | 2,300 ms | 900 ms | **2.5×** |
| **O2** | **Broadcast Join** — copy 265-row zones to every task | 950 ms | 430 ms | **2.2×** |
| **O3** | **Shuffle Partitions** — 200 default → 8 (2 × cores) | 970 ms | 220 ms | **4.4×** |
| **O4** | **Adaptive Query Execution (AQE)** — merges tiny partitions at runtime | 970 ms | 340 ms | **2.8×** |
| **O5** | **Parquet vs CSV** — columnar, compressed, predicate pushdown | 1,840 ms | 220 ms | **8.4×** |

**Measurement methodology:** noop writer (excludes I/O time) + median of 3 runs (eliminates outliers)

### Spark UI Proof per Optimization

| # | Spark UI Evidence |
|---|---|
| O1 | Storage tab shows cached RDD with memory usage |
| O2 | SQL tab: `SortMergeJoin` → `BroadcastHashJoin` |
| O3 | Stages tab: 200 tasks → 8 tasks |
| O4 | SQL tab shows `AQEShuffleRead` coalescing partitions |
| O5 | FileScan shows 2-column `ReadSchema` + `PushedFilters` |

---

## 💡 Key Findings

1. **Peak mobility**: 6 PM is the busiest hour — 158k trips, highest avg fares ($20)
2. **Winter dominates 311**: HEAT/HOT WATER is #1 complaint in 3 of 5 boroughs
3. **Traffic ↔ pollution**: r = +0.74 correlation between taxi trips and NO₂
4. **Temperature ↔ complaints**: r = −0.89 — city can forecast heating demand from weather
5. **Manhattan crawls**: Average speed 12.4 mph vs Staten Island's 45.8 mph
6. **Parquet matters most**: 8.4× speedup vs CSV — single most impactful optimization
7. **Right-size your shuffles**: Default 200 partitions is 25× too many for 4 cores
8. **Pandas wins small, Spark wins big**: Crossover at ~3M rows in local mode

---

## 🛠️ Tech Stack (Web App)

| Technology | Version | Purpose |
|---|---|---|
| HTML5 | — | Structure and semantic markup |
| Vanilla CSS | — | Design system, animations, glassmorphism |
| Vanilla JavaScript (ES2020) | — | All UI logic, renderers, async fetch simulation |
| Chart.js | 4.4.3 | Bar, line, scatter, and heatmap charts for all 10 queries |
| Canvas API | — | Animated background dots + DAG particle flow renderer |
| IntersectionObserver API | — | Scroll-triggered animations and count-ups |
| Google Fonts | — | Inter (body), JetBrains Mono (code), Outfit (display) |

**No build step. No framework. No TypeScript. No bundler.** Open `index.html` and it works.

---

## 🎨 Design System

The entire design lives in `style.css` as CSS custom properties:

```css
/* Color Palette */
--cyan:   #00d4ff    /* Data sources, readers */
--purple: #a855f7    /* Transformations, Spark engine */
--amber:  #f59e0b    /* Wide transforms, shuffle, warnings */
--green:  #10b981    /* Success, optimized, outputs */
--pink:   #ec4899    /* Window functions */
--red:    #ef4444    /* Actions, live data */

/* Dark Theme Backgrounds */
--bg:      #0a0f1e   /* Page background */
--bg-card: #0f1628   /* Card background */
--border:  rgba(255,255,255,0.07)
```

**Key design patterns:**
- **Glassmorphism** cards with `backdrop-filter: blur(20px)` and translucent borders
- **Gradient text** for the hero title (blue → purple → orange)
- **Micro-animations** on hover for all interactive elements
- **Animated background canvas** with 120 slowly pulsing cyan particles
- **Scroll-triggered count-ups** using `IntersectionObserver`
- **Terminal emulator UI** for the fetch simulator with syntax-colored output lines
- **Responsive grid** that collapses gracefully on mobile

---

## 📖 Further Reading

- [`SOLUTION_EXPLAINED.md`](./SOLUTION_EXPLAINED.md) — Deep-dive: every Spark concept, all 10 queries, all 5 optimizations, viva Q&A, and evaluation criteria mapping
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
- [NYC Open Data Portal](https://opendata.cityofnewyork.us/)
- [Open-Meteo API](https://open-meteo.com/) — free weather & air quality, no API key required

---

*🏙️ Smart City NYC — Data Processing using Spark DataFrames · Case Study 2 · Unit 5.2*
