// ============================================================
// data.js — All simulated pipeline data for Smart City NYC
// ============================================================

const PIPELINE_DATA = {

  // ── SPARK SESSION CONFIG ──────────────────────────────────
  sparkSession: {
    version: "3.5.1",
    master: "local[*]",
    cores: 4,
    driverMemory: "4g",
    shufflePartitions: 8,
    aqeEnabled: true,
    appName: "SmartCityNYC"
  },

  // ── DATA SOURCES ──────────────────────────────────────────
  sources: [
    {
      id: "taxi_trips",
      name: "NYC Yellow Taxi Trips",
      subtitle: "Jan 2024 · TLC Open Data",
      icon: "🚕",
      format: "Parquet",
      formatColor: "#a855f7",
      rawRows: 2964624,
      columns: 19,
      sizeMB: 50,
      apiType: "File Download",
      category: "mobility",
      schema: [
        { name: "VendorID",            type: "integer" },
        { name: "tpep_pickup_datetime",type: "timestamp" },
        { name: "tpep_dropoff_datetime",type: "timestamp" },
        { name: "passenger_count",     type: "double" },
        { name: "trip_distance",       type: "double" },
        { name: "PULocationID",        type: "integer" },
        { name: "DOLocationID",        type: "integer" },
        { name: "fare_amount",         type: "double" },
        { name: "tip_amount",          type: "double" },
        { name: "total_amount",        type: "double" },
        { name: "payment_type",        type: "integer" }
      ],
      sampleRows: [
        [1, "2024-01-15 08:23:00", "2024-01-15 08:41:00", 1, 3.2, 161, 236, 14.5, 3.05, 20.1, 1],
        [2, "2024-01-15 09:01:00", "2024-01-15 09:18:00", 2, 1.8, 237, 142, 12.0, 2.40, 17.1, 1],
        [1, "2024-01-15 09:45:00", "2024-01-15 10:02:00", 1, 5.1, 132, 48,  22.0, 4.60, 29.4, 2]
      ]
    },
    {
      id: "complaints_311",
      name: "311 Service Requests",
      subtitle: "NYC Open Data API · ~250k rows",
      icon: "📞",
      format: "CSV (API Pages)",
      formatColor: "#10b981",
      rawRows: 249832,
      columns: 12,
      sizeMB: 78,
      apiType: "REST API (Socrata)",
      category: "citizen",
      schema: [
        { name: "unique_key",      type: "string" },
        { name: "created_date",    type: "timestamp" },
        { name: "closed_date",     type: "timestamp" },
        { name: "complaint_type",  type: "string" },
        { name: "agency",          type: "string" },
        { name: "borough",         type: "string" },
        { name: "status",          type: "string" }
      ],
      sampleRows: [
        ["57291834", "2024-01-02 09:15:00", "2024-01-03 14:22:00", "HEAT/HOT WATER", "HPD", "BROOKLYN", "Closed"],
        ["57291902", "2024-01-02 10:30:00", null,                  "Noise - Residential","DEP","MANHATTAN","Open"],
        ["57292015", "2024-01-02 11:45:00", "2024-01-04 08:00:00", "Blocked Driveway","NYPD","QUEENS",   "Closed"]
      ]
    },
    {
      id: "air_quality",
      name: "Air Quality (Hourly)",
      subtitle: "Open-Meteo API · 744 rows",
      icon: "🌫️",
      format: "JSON (Nested)",
      formatColor: "#f59e0b",
      rawRows: 744,
      columns: 5,
      sizeMB: 0.3,
      apiType: "REST API (Open-Meteo)",
      category: "environment",
      schema: [
        { name: "hour_ts",          type: "timestamp" },
        { name: "pm2_5",            type: "double" },
        { name: "nitrogen_dioxide", type: "double" },
        { name: "carbon_monoxide",  type: "double" }
      ],
      sampleRows: [
        ["2024-01-01 00:00:00", 8.2,  24.8, 312.1],
        ["2024-01-01 01:00:00", 7.9,  20.8, 298.5],
        ["2024-01-01 08:00:00", 14.3, 48.2, 520.3]
      ]
    },
    {
      id: "weather",
      name: "Weather (Hourly)",
      subtitle: "Open-Meteo API · 744 rows",
      icon: "🌦️",
      format: "JSON (Nested)",
      formatColor: "#f59e0b",
      rawRows: 744,
      columns: 4,
      sizeMB: 0.2,
      apiType: "REST API (Open-Meteo)",
      category: "environment",
      schema: [
        { name: "hour_ts",          type: "timestamp" },
        { name: "temperature_2m",   type: "double" },
        { name: "rain",             type: "double" },
        { name: "wind_speed_10m",   type: "double" }
      ],
      sampleRows: [
        ["2024-01-01 00:00:00", -2.1, 0.0, 12.3],
        ["2024-01-01 08:00:00",  1.4, 0.0,  8.7],
        ["2024-01-15 14:00:00",  5.2, 2.1, 15.4]
      ]
    },
    {
      id: "live_traffic",
      name: "Live Traffic Speeds",
      subtitle: "NYC DOT API · ~150 sensors",
      icon: "🚦",
      format: "JSON (Real-time)",
      formatColor: "#ef4444",
      rawRows: 148,
      columns: 6,
      sizeMB: 0.05,
      apiType: "REST API (NYC DOT) · LIVE",
      category: "traffic",
      schema: [
        { name: "link_id",   type: "string" },
        { name: "link_name", type: "string" },
        { name: "speed",     type: "double" },
        { name: "borough",   type: "string" },
        { name: "timestamp", type: "timestamp" },
        { name: "status",    type: "integer" }
      ],
      sampleRows: [
        ["4616056", "FDR Drive N @ 23rd St",   28.4, "Manhattan", "2024-01-15 14:23:00", 0],
        ["4616211", "Brooklyn Bridge Approach",  8.1, "Brooklyn",  "2024-01-15 14:23:00", 0],
        ["4616398", "I-278 Staten Island Expy", 55.2, "Staten Island","2024-01-15 14:23:00", 0]
      ]
    }
  ],

  // ── CLEANING STEPS ────────────────────────────────────────
  cleaningSteps: {
    taxi: {
      rawRows: 2964624,
      cleanRows: 2828365,
      removedRows: 136259,
      removalPct: 4.6,
      rules: [
        { rule: "Pickup inside Jan 2024",          removed: 18,    pct: 0.001 },
        { rule: "Duration 1–180 min",              removed: 4821,  pct: 0.16 },
        { rule: "Distance 0–100 miles",            removed: 61240, pct: 2.07 },
        { rule: "Fare & total > 0",                removed: 12834, pct: 0.43 },
        { rule: "Passengers 1–6",                  removed: 28442, pct: 0.96 },
        { rule: "Speed ≤ 80 mph",                  removed: 28904, pct: 0.98 }
      ],
      enrichedColumns: ["pickup_hour","pickup_date","day_of_week","is_weekend","tip_pct","payment_method"]
    },
    complaints: {
      rawRows: 249832,
      cleanRows: 241507,
      removedRows: 8325,
      rules: [
        { rule: "dropDuplicates(unique_key)",       removed: 2103, pct: 0.84 },
        { rule: "Valid date parse (try_cast)",       removed: 412,  pct: 0.16 },
        { rule: "Borough normalization",             removed: 5810, pct: 2.33 }
      ],
      enrichedColumns: ["resolution_hours","borough_clean"]
    },
    traffic: {
      rawRows: 312,
      cleanRows: 148,
      removedRows: 164,
      rules: [
        { rule: "status = 0 (valid reading)",       removed: 98,   pct: 31.4 },
        { rule: "speed > 0",                        removed: 66,   pct: 21.2 },
        { rule: "row_number() → latest per sensor", removed: 0,    pct: 0 }
      ]
    }
  },

  // ── PARTITIONS ────────────────────────────────────────────
  partitions: [
    { id: 0, rows: 741156, pct: 25.0 },
    { id: 1, rows: 741156, pct: 25.0 },
    { id: 2, rows: 741156, pct: 25.0 },
    { id: 3, rows: 741156, pct: 25.0 }
  ],

  // ── QUERIES ───────────────────────────────────────────────
  queries: [
    {
      id: "Q1",
      category: "🚕 Mobility",
      title: "When does the city move?",
      subtitle: "Trips, avg fare & distance per hour",
      operations: ["groupBy", "agg(count, avg)", "orderBy", "Spark SQL"],
      transformationType: "wide",
      executionTimeMs: 1840,
      data: {
        labels: ["0","1","2","3","4","5","6","7","8","9","10","11","12","13","14","15","16","17","18","19","20","21","22","23"],
        trips:  [28000,18000,12000,8000,5000,7000,22000,62000,98000,112000,118000,121000,119000,122000,125000,128000,132000,145000,158000,148000,135000,118000,95000,65000],
        avgFare:[16.2,15.8,15.1,14.9,14.2,13.8,14.5,17.2,18.4,17.9,17.5,17.2,17.0,17.1,17.3,17.6,18.0,18.9,20.1,19.8,19.2,18.7,18.1,17.4]
      }
    },
    {
      id: "Q2",
      category: "🚕 Mobility",
      title: "Busiest Mobility Hubs",
      subtitle: "Top 10 zones by pickup count",
      operations: ["join (broadcast)", "groupBy(Zone, Borough)", "orderBy(desc)", "limit(10)"],
      transformationType: "wide",
      executionTimeMs: 430,
      data: {
        labels: ["Midtown Center","Upper East Side N","JFK Airport","Times Sq / Theatre","Penn Station","LaGuardia Airport","Midtown South","Upper West Side N","Harlem","Lincoln Square"],
        trips:  [89432, 76821, 68234, 64521, 58932, 54211, 51234, 48291, 42831, 39204],
        boroughs: ["Manhattan","Manhattan","Queens","Manhattan","Manhattan","Queens","Manhattan","Manhattan","Manhattan","Manhattan"]
      }
    },
    {
      id: "Q3",
      category: "🚕 Mobility",
      title: "Weekly Heatmap",
      subtitle: "Trips by day × hour (pivot)",
      operations: ["groupBy(day_of_week)", "pivot(pickup_hour)", "count()"],
      transformationType: "wide",
      executionTimeMs: 2120,
      data: {
        days: ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"],
        hours: [0,6,12,18],
        matrix: [
          [25,45,95,125,118,85,55,38,28,22,18,15,12,10,8,6,5,5,8,18,45,80,105,75],
          [22,42,92,128,120,88,58,40,30,24,19,16,13,11,9,7,6,6,9,19,48,84,108,78],
          [24,44,95,130,122,90,60,42,32,25,20,17,14,12,10,8,7,7,10,20,50,88,112,80],
          [26,46,98,135,128,92,62,44,34,26,21,18,15,13,11,9,8,8,11,22,55,95,120,88],
          [30,50,108,145,138,102,72,54,40,30,25,22,18,16,14,12,10,10,15,30,72,125,155,125],
          [52,75,95,85,65,48,38,35,38,48,62,75,88,95,98,95,92,88,82,78,88,115,145,128],
          [68,52,35,25,18,15,14,18,28,42,55,68,78,88,92,90,88,85,80,75,88,108,128,98]
        ]
      }
    },
    {
      id: "Q4",
      category: "🏛️ Citizen Services",
      title: "Top Complaint Types",
      subtitle: "By count with % of total",
      operations: ["groupBy(complaint_type)", "count()", "Window.sum().over()"],
      transformationType: "wide",
      executionTimeMs: 980,
      data: {
        labels: ["HEAT/HOT WATER","Noise - Residential","Blocked Driveway","Illegal Parking","Noise - Street/Sidewalk","UNSANITARY CONDITION","Derelict Vehicle","Street Light Condition","Paint/Plaster","Plumbing"],
        counts: [38492, 24831, 18234, 16821, 15432, 12891, 10234, 9821, 8432, 7821],
        pcts:   [15.9,  10.3,   7.5,   7.0,   6.4,   5.3,   4.2,   4.1,   3.5,   3.2]
      }
    },
    {
      id: "Q5",
      category: "🏛️ Citizen Services",
      title: "Agency Response Speed",
      subtitle: "Median resolution hours per agency",
      operations: ["groupBy(agency)", "percentile_approx(resolution_hours, 0.5)", "avg()"],
      transformationType: "wide",
      executionTimeMs: 1240,
      data: {
        labels: ["NYPD","DOT","DEP","DPR","DSNY","HPD","DOB","TLC"],
        medianHrs: [4.2, 18.5, 22.1, 48.3, 8.7, 72.4, 120.8, 6.1],
        avgHrs:    [6.8, 24.2, 28.9, 72.1, 11.2, 98.3, 165.2, 9.8]
      }
    },
    {
      id: "Q6",
      category: "🏛️ Citizen Services",
      title: "Top 3 Complaints per Borough",
      subtitle: "dense_rank() window function",
      operations: ["groupBy(borough, complaint_type)", "Window dense_rank()", "filter rank ≤ 3"],
      transformationType: "wide",
      executionTimeMs: 1580,
      data: [
        { borough: "MANHATTAN",     rank: 1, complaint: "HEAT/HOT WATER",         count: 12831 },
        { borough: "MANHATTAN",     rank: 2, complaint: "Noise - Residential",     count: 9432  },
        { borough: "MANHATTAN",     rank: 3, complaint: "Blocked Driveway",        count: 6821  },
        { borough: "BROOKLYN",      rank: 1, complaint: "HEAT/HOT WATER",          count: 14291 },
        { borough: "BROOKLYN",      rank: 2, complaint: "Noise - Residential",     count: 8124  },
        { borough: "BROOKLYN",      rank: 3, complaint: "Illegal Parking",         count: 5831  },
        { borough: "QUEENS",        rank: 1, complaint: "Blocked Driveway",        count: 8421  },
        { borough: "QUEENS",        rank: 2, complaint: "Illegal Parking",         count: 7234  },
        { borough: "QUEENS",        rank: 3, complaint: "Noise - Residential",     count: 5124  },
        { borough: "BRONX",         rank: 1, complaint: "HEAT/HOT WATER",          count: 9284  },
        { borough: "BRONX",         rank: 2, complaint: "Noise - Residential",     count: 4821  },
        { borough: "BRONX",         rank: 3, complaint: "UNSANITARY CONDITION",    count: 3421  },
        { borough: "STATEN ISLAND", rank: 1, complaint: "Noise - Street/Sidewalk", count: 1824  },
        { borough: "STATEN ISLAND", rank: 2, complaint: "Blocked Driveway",        count: 1432  },
        { borough: "STATEN ISLAND", rank: 3, complaint: "Illegal Parking",         count: 1281  }
      ]
    },
    {
      id: "Q7",
      category: "🌫️ Environment",
      title: "Traffic vs Air Pollution",
      subtitle: "Trips corr. with NO₂, PM2.5, CO",
      operations: ["date_trunc(hour)", "join (by hour)", "na.drop()", "corr()"],
      transformationType: "wide",
      executionTimeMs: 2840,
      data: {
        labels: ["0","2","4","6","8","10","12","14","16","18","20","22"],
        trips:  [28000,12000,5000,22000,98000,118000,119000,125000,132000,158000,135000,95000],
        no2:    [18.2, 14.1, 11.8, 22.4, 45.8, 42.1, 38.2, 36.4, 44.2, 52.1, 41.8, 32.4],
        correlations: { no2: 0.74, pm25: 0.68, co: 0.71 }
      }
    },
    {
      id: "Q8",
      category: "🌦️ Environment",
      title: "Cold Days → Heating Complaints",
      subtitle: "Temperature vs HEAT/HOT WATER count",
      operations: ["join (by date)", "corr(temp, complaint_count)"],
      transformationType: "wide",
      executionTimeMs: 1920,
      data: {
        scatter: [
          { temp: -5.2, complaints: 1842 }, { temp: -3.1, complaints: 1721 },
          { temp: -1.8, complaints: 1654 }, { temp:  0.4, complaints: 1432 },
          { temp:  2.1, complaints: 1298 }, { temp:  3.8, complaints: 1124 },
          { temp:  5.2, complaints:  982 }, { temp:  7.1, complaints:  842 },
          { temp:  9.4, complaints:  712 }, { temp: 11.2, complaints:  624 }
        ],
        correlation: -0.89
      }
    },
    {
      id: "Q9",
      category: "🚦 Live Traffic",
      title: "Live Traffic Snapshot",
      subtitle: "Current speeds by borough",
      operations: ["groupBy(borough).agg(count, avg, min)", "orderBy(speed)"],
      transformationType: "narrow",
      executionTimeMs: 280,
      data: {
        boroughs: [
          { name: "Manhattan",    avgSpeed: 12.4, minSpeed: 4.2,  sensors: 62 },
          { name: "Brooklyn",     avgSpeed: 18.2, minSpeed: 5.8,  sensors: 32 },
          { name: "Queens",       avgSpeed: 28.4, minSpeed: 8.1,  sensors: 28 },
          { name: "Bronx",        avgSpeed: 22.1, minSpeed: 7.4,  sensors: 18 },
          { name: "Staten Island",avgSpeed: 45.8, minSpeed: 28.4, sensors: 8  }
        ],
        slowestRoads: [
          { name: "Brooklyn Bridge Approach", speed: 4.2,  borough: "Brooklyn" },
          { name: "FDR Drive S @ 48th St",    speed: 5.1,  borough: "Manhattan" },
          { name: "Holland Tunnel Approach",  speed: 6.3,  borough: "Manhattan" },
          { name: "Lincoln Tunnel Approach",  speed: 7.8,  borough: "Manhattan" },
          { name: "Williamsburg Bridge",      speed: 8.1,  borough: "Brooklyn" }
        ]
      }
    },
    {
      id: "Q10",
      category: "🏙️ Integrated View",
      title: "Borough Scorecard",
      subtitle: "Mobility + Services + Traffic combined",
      operations: ["spark.createDataFrame", "4× left join", "broadcast join", "per-1000 rates"],
      transformationType: "wide",
      executionTimeMs: 3240,
      data: [
        { borough: "Manhattan",     population: 1694251, tripsPerK: 52.8, complaintsPerK: 7.6, medianResHrs: 38.4, avgSpeed: 12.4 },
        { borough: "Brooklyn",      population: 2736074, tripsPerK: 12.1, complaintsPerK: 9.1, medianResHrs: 42.1, avgSpeed: 18.2 },
        { borough: "Queens",        population: 2405464, tripsPerK: 8.4,  complaintsPerK: 6.8, medianResHrs: 35.8, avgSpeed: 28.4 },
        { borough: "Bronx",         population: 1472654, tripsPerK: 4.2,  complaintsPerK: 11.2,medianResHrs: 48.2, avgSpeed: 22.1 },
        { borough: "Staten Island", population:  495747, tripsPerK: 0.8,  complaintsPerK: 3.7, medianResHrs: 28.4, avgSpeed: 45.8 }
      ]
    }
  ],

  // ── OPTIMIZATIONS ─────────────────────────────────────────
  optimizations: [
    {
      id: "O1",
      name: "Caching",
      icon: "💾",
      operation: "trips.cache()",
      problem: "Every query re-reads Parquet & re-runs cleaning (lazy evaluation replay)",
      fix: "cache() — materialises cleaned DF in memory",
      baselineMs: 2300,
      optimizedMs: 900,
      speedup: 2.5,
      sparkUIProof: "Storage tab shows cached RDD"
    },
    {
      id: "O2",
      name: "Broadcast Join",
      icon: "📡",
      operation: "F.broadcast(zones_df)",
      problem: "Sort-merge join shuffles 3M trip rows across cores",
      fix: "Copy 265-row zones table to every task — no trip shuffle",
      baselineMs: 950,
      optimizedMs: 430,
      speedup: 2.2,
      sparkUIProof: "SQL tab: SortMergeJoin → BroadcastHashJoin"
    },
    {
      id: "O3",
      name: "Shuffle Partitions",
      icon: "🔀",
      operation: "spark.conf.set('spark.sql.shuffle.partitions', 8)",
      problem: "Default 200 shuffle partitions → 200 tiny tasks on 4 cores (huge overhead)",
      fix: "2 × cores = 8 right-sized partitions",
      baselineMs: 970,
      optimizedMs: 220,
      speedup: 4.4,
      sparkUIProof: "Stages tab: 200 tasks → 8 tasks"
    },
    {
      id: "O4",
      name: "Adaptive Query Execution",
      icon: "🧠",
      operation: "spark.conf.set('spark.sql.adaptive.enabled', True)",
      problem: "Static plan uses 200 shuffle partitions regardless of actual data size",
      fix: "AQE merges tiny shuffle partitions at runtime based on real statistics",
      baselineMs: 970,
      optimizedMs: 340,
      speedup: 2.8,
      sparkUIProof: "SQL tab shows AQEShuffleRead coalescing"
    },
    {
      id: "O5",
      name: "Parquet vs CSV",
      icon: "📁",
      operation: "spark.read.parquet(path) vs spark.read.csv(path)",
      problem: "CSV: 326 MB, must read every column and parse all text",
      fix: "Parquet: 50 MB, columnar — reads only needed columns + predicate pushdown",
      baselineMs: 1840,
      optimizedMs: 220,
      speedup: 8.4,
      sparkUIProof: "FileScan shows 2-column ReadSchema + PushedFilters"
    }
  ],

  // ── PANDAS vs SPARK BENCHMARK ─────────────────────────────
  benchmark: {
    rowCounts: [100000, 500000, 1000000, 2964624],
    pandasTimes:  [0.38, 1.82, 3.91, 11.24],
    sparkTimes:   [1.92, 2.14, 2.48,  3.12]
  },

  // ── CORE SCALING ──────────────────────────────────────────
  coreScaling: {
    cores: [1, 2, 4],
    times: [8.4, 5.2, 3.1],
    speedups: [1.0, 1.6, 2.7]
  },

  // ── SMART CITY INSIGHTS ───────────────────────────────────
  insights: [
    { icon: "🕔", label: "Peak Hour",         value: "6 PM (18:00)",        detail: "158,000 trips/hr" },
    { icon: "📍", label: "Busiest Hub",        value: "Midtown Center",      detail: "89,432 pickups" },
    { icon: "📞", label: "Top Complaint",      value: "HEAT/HOT WATER",      detail: "38,492 requests (15.9%)" },
    { icon: "⚡", label: "Fastest Agency",     value: "NYPD",                detail: "4.2 hrs median" },
    { icon: "🐢", label: "Slowest Agency",     value: "DOB",                 detail: "120.8 hrs median" },
    { icon: "🌡️", label: "Temp-Complaint Corr",value: "r = −0.89",          detail: "Strong: cold → more HEAT complaints" },
    { icon: "🚗", label: "Traffic-NO₂ Corr",  value: "r = +0.74",           detail: "Traffic drives air pollution" },
    { icon: "🐌", label: "Slowest Borough",    value: "Manhattan",           detail: "Avg 12.4 mph right now" }
  ]
};

window.PIPELINE_DATA = PIPELINE_DATA;
