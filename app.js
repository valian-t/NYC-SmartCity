// ============================================================
// app.js v2 — Smart City NYC Pipeline Visualizer
// ============================================================

const D = window.PIPELINE_DATA;
let activeSourceId = 'taxi_trips';
let queryCharts = {};
let fetchSelectedSource = null;
let fetchRunning = false;

// ── UTILS ─────────────────────────────────────────────────────
const scrollTo = id => document.getElementById(id)?.scrollIntoView({ behavior: 'smooth', block: 'start' });
function fmt(n) { return n >= 1e6 ? (n/1e6).toFixed(2)+'M' : n >= 1e3 ? n.toLocaleString() : n; }
function fmtMs(ms) { return ms >= 1000 ? (ms/1000).toFixed(2)+'s' : ms+'ms'; }
function countUp(el, target, duration = 1400) {
  const start = performance.now();
  const fn = now => {
    const t = Math.min((now - start) / duration, 1);
    const ease = 1 - Math.pow(1 - t, 4);
    el.textContent = Math.floor(ease * target).toLocaleString();
    if (t < 1) requestAnimationFrame(fn);
    else el.textContent = target.toLocaleString();
  };
  requestAnimationFrame(fn);
}
function onVisible(el, fn, threshold = 0.2) {
  const obs = new IntersectionObserver(entries => {
    entries.forEach(e => { if (e.isIntersecting) { fn(); obs.unobserve(el); } });
  }, { threshold, rootMargin: '0px 0px -40px 0px' });
  if (el) obs.observe(el);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ── LOADING SCREEN ─────────────────────────────────────────────
function initLoading() {
  const steps = document.querySelectorAll('.loader-step');
  steps.forEach((s, i) => {
    setTimeout(() => s.classList.add('show'), 300 + i * 180);
  });
  setTimeout(() => {
    document.getElementById('loading')?.classList.add('hide');
  }, 1800);
}

// ── BACKGROUND CANVAS ──────────────────────────────────────────
function initBackground() {
  const canvas = document.getElementById('bg-canvas');
  const ctx = canvas.getContext('2d');
  const resize = () => { canvas.width = window.innerWidth; canvas.height = window.innerHeight; };
  resize();
  window.addEventListener('resize', resize);
  const stars = Array.from({ length: 120 }, () => ({
    x: Math.random(), y: Math.random(), r: Math.random() * 1.4 + 0.3,
    a: Math.random(), da: (Math.random() - 0.5) * 0.007
  }));
  const draw = () => {
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    stars.forEach(s => {
      s.a += s.da; if (s.a <= 0 || s.a >= 1) s.da *= -1;
      ctx.beginPath();
      ctx.arc(s.x * canvas.width, s.y * canvas.height, s.r, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(8,145,178,${s.a * 0.12})`; ctx.fill();
    });
    requestAnimationFrame(draw);
  };
  draw();
}

// ── NAV ────────────────────────────────────────────────────────
function initNav() {
  const btns = document.querySelectorAll('.nav-stage-btn');
  btns.forEach(btn => {
    btn.addEventListener('click', () => scrollTo(btn.dataset.section));
  });
  const obs = new IntersectionObserver(entries => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        btns.forEach(b => b.classList.remove('active'));
        document.querySelector(`[data-section="${e.target.id}"]`)?.classList.add('active');
      }
    });
  }, { rootMargin: '-40% 0px -40% 0px' });
  document.querySelectorAll('[id^="section-"]').forEach(s => obs.observe(s));
}

// ── HERO COUNT-UPS ──────────────────────────────────────────────
function initHero() {
  onVisible(document.getElementById('section-hero'), () => {
    const c1 = document.getElementById('hero-c1');
    const c2 = document.getElementById('hero-c2');
    if (c1) countUp(c1, 2964624, 1600);
    if (c2) countUp(c2, 249832, 1400);
  }, 0.3);
}

// ═══════════════════════════════════════════════════════════════
function renderSparkMap() {}


// ═══════════════════════════════════════════════════════════════
// SPARK SESSION
// ═══════════════════════════════════════════════════════════════
function renderSparkSession() {
  const container = document.getElementById('spark-config-grid');
  const cfg = D.sparkSession;
  const items = [
    { label: 'App Name', value: cfg.appName, cls: '' },
    { label: 'Version', value: cfg.version, cls: '' },
    { label: 'Master', value: cfg.master, cls: '' },
    { label: 'CPU Cores', value: cfg.cores + ' cores', cls: 'v-green' },
    { label: 'Driver Mem', value: cfg.driverMemory, cls: 'v-purple' },
    { label: 'Shuffle Parts', value: cfg.shufflePartitions, cls: '' },
    { label: 'AQE', value: '✅ ON', cls: 'v-green' },
    { label: 'Spark UI', value: ':4040', cls: 'v-amber' }
  ];
  container.innerHTML = items.map(i => `
    <div class="config-item">
      <span class="config-label">${i.label}</span>
      <span class="config-value ${i.cls}">${i.value}</span>
    </div>
  `).join('');
}

// ═══════════════════════════════════════════════════════════════
// INTERACTIVE FETCH SIMULATOR
// ═══════════════════════════════════════════════════════════════
const FETCH_SCRIPTS = {
  taxi_trips: {
    steps: [
      { t: 80,  cls: 'cmd',     text: '>>> Connecting to NYC TLC Open Data...' },
      { t: 300, cls: 'info',    text: 'GET https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' },
      { t: 600, cls: 'info',    text: '📥 Downloading... 50.2 MB (Parquet format)' },
      { t: 900, cls: 'success', text: '✓ Download complete (1.38s)' },
      { t: 1050,cls: 'spark',   text: '>>> trips = spark.read.parquet("yellow_tripdata_2024-01.parquet")' },
      { t: 1300,cls: 'spark',   text: '# Parquet carries its own schema — no inferSchema needed' },
      { t: 1550,cls: 'data',    text: '>>> Lazy plan built — no data read yet (0.04s)' },
      { t: 1750,cls: 'spark',   text: '>>> trips.count()  # ACTION — forces execution' },
      { t: 2100,cls: 'warn',    text: '[Spark Job 0] FileScan parquet → 4 tasks on 4 partitions' },
      { t: 2450,cls: 'success', text: '✓ 2,964,624 rows · 19 columns · 4 partitions · 1.2s' },
    ],
    job: '#0', stages: 1, tasks: 4, partitions: 4, rows: 2964624, timeMs: 1200
  },
  complaints_311: {
    steps: [
      { t: 80,  cls: 'cmd',     text: '>>> Fetching 311 data from NYC Open Data API (Socrata)...' },
      { t: 200, cls: 'info',    text: 'GET https://data.cityofnewyork.us/resource/erm2-nwe9.csv?$limit=100000&$offset=0' },
      { t: 400, cls: 'success', text: '✓ Page 1/3 saved — 100,000 rows (data/311/page_001.csv)' },
      { t: 600, cls: 'info',    text: 'GET ...?$offset=100000  — fetching page 2...' },
      { t: 800, cls: 'success', text: '✓ Page 2/3 saved — 100,000 rows (data/311/page_002.csv)' },
      { t: 1000,cls: 'info',    text: 'GET ...?$offset=200000  — fetching page 3...' },
      { t: 1200,cls: 'success', text: '✓ Page 3/3 saved — 49,832 rows (data/311/page_003.csv)' },
      { t: 1400,cls: 'spark',   text: '>>> complaints = spark.read.csv("data/311/", header=True,' },
      { t: 1500,cls: 'spark',   text: '      multiLine=True, escape=\'"\', schema=complaints_schema)' },
      { t: 1700,cls: 'info',    text: '# Spark reads ALL csv files in folder as ONE DataFrame!' },
      { t: 2000,cls: 'success', text: '✓ 249,832 rows · 12 columns · 3 partitions · 0.98s' },
    ],
    job: '#2', stages: 2, tasks: 3, partitions: 3, rows: 249832, timeMs: 980
  },
  air_quality: {
    steps: [
      { t: 80,  cls: 'cmd',     text: '>>> Fetching Air Quality from Open-Meteo API (no key needed)...' },
      { t: 250, cls: 'info',    text: 'GET https://air-quality-api.open-meteo.com/v1/air-quality?latitude=40.71&longitude=-74.01&hourly=pm2_5,nitrogen_dioxide,carbon_monoxide&start_date=2024-01-01&end_date=2024-01-31' },
      { t: 700, cls: 'success', text: '✓ JSON received — nested structure with parallel arrays (0.31s)' },
      { t: 900, cls: 'spark',   text: '>>> raw = spark.read.option("multiLine", True).json("air_quality.json")' },
      { t: 1100,cls: 'spark',   text: '>>> air_df = (raw' },
      { t: 1200,cls: 'spark',   text: '    .select(arrays_zip("hourly.time","hourly.pm2_5","hourly.nitrogen_dioxide").alias("z"))' },
      { t: 1350,cls: 'spark',   text: '    .select(explode("z").alias("row"))' },
      { t: 1500,cls: 'spark',   text: '    .select(to_timestamp("row.time").alias("hour_ts"), col("row.pm2_5"), col("row.nitrogen_dioxide")))' },
      { t: 1800,cls: 'data',    text: '>>> air_df.show(3)  # ACTION' },
      { t: 2100,cls: 'success', text: '✓ 744 rows (1 per hour) · 4 columns · 0.42s' },
    ],
    job: '#3', stages: 2, tasks: 2, partitions: 1, rows: 744, timeMs: 420
  },
  weather: {
    steps: [
      { t: 80,  cls: 'cmd',     text: '>>> Fetching Weather from Open-Meteo API...' },
      { t: 250, cls: 'info',    text: 'GET https://api.open-meteo.com/v1/forecast?latitude=40.71&longitude=-74.01&hourly=temperature_2m,rain,wind_speed_10m&start_date=2024-01-01&end_date=2024-01-31' },
      { t: 600, cls: 'success', text: '✓ JSON received — same nested structure as air quality (0.28s)' },
      { t: 800, cls: 'spark',   text: '>>> weather_df = (raw_weather' },
      { t: 950, cls: 'spark',   text: '    .select(arrays_zip("hourly.time","hourly.temperature_2m","hourly.rain").alias("z"))' },
      { t: 1100,cls: 'spark',   text: '    .select(explode("z").alias("row")).select(...))' },
      { t: 1400,cls: 'success', text: '✓ 744 rows · 4 columns · 0.38s' },
    ],
    job: '#4', stages: 2, tasks: 2, partitions: 1, rows: 744, timeMs: 380
  },
  live_traffic: {
    steps: [
      { t: 80,  cls: 'cmd',     text: '>>> Fetching LIVE traffic speeds from NYC DOT API...' },
      { t: 200, cls: 'warn',    text: '⚡ REAL-TIME: data changes every few minutes!' },
      { t: 400, cls: 'info',    text: 'GET https://data.cityofnewyork.us/resource/i4gi-tjb9.json' },
      { t: 700, cls: 'success', text: '✓ 312 raw sensor readings received (snapshot at this moment)' },
      { t: 900, cls: 'spark',   text: '>>> traffic_raw = spark.read.option("multiLine",True).json(f"traffic_{timestamp}.json")' },
      { t: 1100,cls: 'spark',   text: '>>> # Keep only valid readings and latest per road' },
      { t: 1200,cls: 'spark',   text: '>>> traffic_clean = (traffic_raw' },
      { t: 1300,cls: 'spark',   text: '    .filter((col("status")==0) & (col("speed")>0))' },
      { t: 1450,cls: 'spark',   text: '    .withColumn("rn", row_number().over(Window.partitionBy("link_id").orderBy(desc("datetime"))))' },
      { t: 1600,cls: 'spark',   text: '    .filter(col("rn")==1))  # latest reading per sensor' },
      { t: 1900,cls: 'success', text: '✓ 148 valid readings · 6 columns · 0.28s' },
    ],
    job: '#5', stages: 3, tasks: 4, partitions: 2, rows: 148, timeMs: 280
  }
};

function renderFetchSimulator() {
  const list = document.getElementById('fetch-source-list');
  list.innerHTML = D.sources.map(src => `
    <button class="fetch-src-btn" data-src="${src.id}" id="fsb-${src.id}">
      <span class="fetch-src-icon">${src.icon}</span>
      <div class="fetch-src-info">
        <div class="fetch-src-name">${src.name}</div>
        <div class="fetch-src-fmt">${src.format}</div>
      </div>
    </button>
  `).join('');

  document.querySelectorAll('.fetch-src-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      if (fetchRunning) return;
      document.querySelectorAll('.fetch-src-btn').forEach(b => b.classList.remove('selected'));
      btn.classList.add('selected');
      fetchSelectedSource = btn.dataset.src;
      document.getElementById('fetch-run-btn').disabled = false;
      // Preview what will happen
      const body = document.getElementById('terminal-body');
      const src = D.sources.find(s => s.id === fetchSelectedSource);
      body.innerHTML = `
        <div class="terminal-line dim">▶ Source selected: <span style="color:var(--cyan)">${src.name}</span></div>
        <div class="terminal-line dim">▶ Format: <span style="color:var(--amber)">${src.format}</span></div>
        <div class="terminal-line dim">▶ Expected rows: <span style="color:var(--green)">${fmt(src.rawRows)}</span></div>
        <div class="terminal-line dim">▶ Press ▶ Run Fetch to simulate the pipeline...</div>
      `;
      document.getElementById('fetch-output').style.display = 'none';
      document.getElementById('spark-internals').style.display = 'none';
    });
  });

  document.getElementById('fetch-run-btn').addEventListener('click', runFetchSimulation);
  document.getElementById('fetch-clear-btn').addEventListener('click', () => {
    document.getElementById('terminal-body').innerHTML = `<div class="terminal-line dim">▶ Console cleared. Select a source to begin.</div>`;
    document.getElementById('fetch-output').style.display = 'none';
    document.getElementById('spark-internals').style.display = 'none';
    document.getElementById('fetch-progress-bar').style.width = '0%';
    setFetchStatus('IDLE');
  });

  // Fetch output tabs
  document.querySelectorAll('.fot-btn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.querySelectorAll('.fot-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      if (fetchSelectedSource) showFetchOutputTab(btn.dataset.tab, fetchSelectedSource);
    });
  });
}

function setFetchStatus(status) {
  const badge = document.getElementById('fetch-status-badge');
  badge.textContent = status;
  badge.className = 'terminal-status';
  if (status === 'RUNNING') badge.classList.add('running');
  if (status === 'DONE') badge.classList.add('done');
}

async function runFetchSimulation() {
  if (!fetchSelectedSource || fetchRunning) return;
  fetchRunning = true;
  const script = FETCH_SCRIPTS[fetchSelectedSource];
  if (!script) return;

  setFetchStatus('RUNNING');
  document.getElementById('fetch-run-btn').disabled = true;
  document.getElementById('fetch-output').style.display = 'none';
  document.getElementById('spark-internals').style.display = 'none';

  const body = document.getElementById('terminal-body');
  body.innerHTML = '';

  const totalTime = script.steps[script.steps.length - 1].t + 400;

  // Animate progress bar
  let pStart = performance.now();
  const animProgress = () => {
    const elapsed = performance.now() - pStart;
    const pct = Math.min((elapsed / totalTime) * 100, 98);
    document.getElementById('fetch-progress-bar').style.width = pct + '%';
    if (pct < 98) requestAnimationFrame(animProgress);
  };
  animProgress();

  // Run steps
  for (const step of script.steps) {
    await sleep(step.t === script.steps[0].t ? step.t : step.t - script.steps[script.steps.indexOf(step) - 1].t);
    const line = document.createElement('div');
    line.className = `terminal-line ${step.cls}`;
    line.textContent = step.text;
    body.appendChild(line);
    body.scrollTop = body.scrollHeight;
  }

  await sleep(300);
  document.getElementById('fetch-progress-bar').style.width = '100%';

  // Show Spark internals
  const internals = document.getElementById('spark-internals');
  internals.style.display = 'grid';
  const vals = {
    'si-job': script.job, 'si-stages': script.stages,
    'si-tasks': script.tasks, 'si-partitions': script.partitions,
    'si-rows': fmt(script.rows), 'si-time': fmtMs(script.timeMs)
  };
  Object.entries(vals).forEach(([id, val]) => {
    const el = document.getElementById(id + '-val');
    if (el) el.textContent = val;
    document.getElementById(id)?.classList.add('lit');
  });

  // Show output panel
  const output = document.getElementById('fetch-output');
  output.style.display = 'block';
  showFetchOutputTab('schema', fetchSelectedSource);

  setFetchStatus('DONE');
  fetchRunning = false;
  document.getElementById('fetch-run-btn').disabled = false;
}

function showFetchOutputTab(tab, srcId) {
  const src = D.sources.find(s => s.id === srcId);
  if (!src) return;
  const body = document.getElementById('fetch-output-body');

  const TYPE_CLS = { integer: 'type-integer', double: 'type-double', string: 'type-string', timestamp: 'type-timestamp' };

  if (tab === 'schema') {
    body.innerHTML = `
      <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px">
        <span style="font-size:20px">${src.icon}</span>
        <div>
          <strong>${src.name}</strong> — <span style="color:var(--text-2);font-size:13px">${src.schema.length} columns · ${fmt(src.rawRows)} rows</span>
        </div>
        <span class="chip chip-cyan" style="margin-left:auto">df.printSchema()</span>
      </div>
      <div style="overflow-x:auto">
        <table class="schema-table">
          <thead><tr><th>#</th><th>Column Name</th><th>Type</th><th>Nullable</th></tr></thead>
          <tbody>
            ${src.schema.map((col, i) => `
              <tr>
                <td style="color:var(--text-3)">${i}</td>
                <td class="col-name">${col.name}</td>
                <td><span class="type-badge ${TYPE_CLS[col.type] || ''}">${col.type}</span></td>
                <td style="color:var(--green);font-size:11px">✓ true</td>
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>`;
  } else if (tab === 'preview') {
    body.innerHTML = `
      <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px">
        <strong>Sample Rows</strong>
        <span class="chip chip-purple" style="margin-left:auto">df.show(3)</span>
      </div>
      <div style="overflow-x:auto">
        <table class="data-table">
          <thead><tr>${src.schema.map(c => `<th>${c.name}</th>`).join('')}</tr></thead>
          <tbody>
            ${src.sampleRows.map(row => `
              <tr>${row.map(v => `<td>${v === null ? '<span style="color:var(--text-3)">null</span>' : v}</td>`).join('')}</tr>
            `).join('')}
          </tbody>
        </table>
      </div>
      <div style="padding:10px 0 0;font-size:11px;color:var(--text-3)">
        Showing 3 of <strong style="color:var(--cyan)">${fmt(src.rawRows)}</strong> rows
      </div>`;
  } else if (tab === 'stats') {
    const script = FETCH_SCRIPTS[srcId];
    body.innerHTML = `
      <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:12px">
        ${[
          ['Total Rows', fmt(src.rawRows), 'var(--cyan)'],
          ['Columns', src.columns, 'var(--purple)'],
          ['File Size', src.sizeMB < 1 ? (src.sizeMB*1000).toFixed(0)+'KB' : src.sizeMB+'MB', 'var(--amber)'],
          ['Partitions', script?.partitions || 1, 'var(--green)'],
          ['Spark Tasks', script?.tasks || 1, 'var(--pink)'],
          ['Read Time', fmtMs(script?.timeMs || 100), 'var(--cyan)'],
        ].map(([l, v, c]) => `
          <div style="background:var(--bg-card);border:1px solid var(--border);border-radius:10px;padding:16px;text-align:center">
            <div style="font-size:10px;color:var(--text-3);font-weight:700;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:4px;font-family:'JetBrains Mono',monospace">${l}</div>
            <div style="font-family:'Outfit',sans-serif;font-size:22px;font-weight:900;color:${c}">${v}</div>
          </div>
        `).join('')}
      </div>`;
  } else if (tab === 'code') {
    const codeMap = {
      taxi_trips:    `trips = spark.read.parquet(<span class="str">"yellow_tripdata_2024-01.parquet"</span>)\ntrips.printSchema()\ntrips.count() <span class="cm"># 2,964,624</span>`,
      taxi_zones:    `zone_schema = StructType([\n  StructField(<span class="str">"LocationID"</span>, IntegerType()),\n  StructField(<span class="str">"Borough"</span>,    StringType()),\n  StructField(<span class="str">"Zone"</span>,       StringType()),\n  StructField(<span class="str">"service_zone"</span>, StringType())\n])\nzones = spark.read.csv(<span class="str">"taxi+_zone_lookup.csv"</span>, header=<span class="kw">True</span>, schema=zone_schema)\nzones.show(<span class="num">5</span>)`,
      complaints_311:`<span class="cm"># Page through API: 3 pages × 100k rows</span>\n<span class="kw">for</span> offset <span class="kw">in</span> [0, 100000, 200000]:\n    url = f<span class="str">"https://data.cityofnewyork.us/resource/erm2-nwe9.csv?$limit=100000&$offset={offset}"</span>\n    urllib.request.urlretrieve(url, f<span class="str">"data/311/page_{offset}.csv"</span>)\n\n<span class="cm"># Spark reads ALL csv files in the folder as ONE DataFrame</span>\ncomplaints = spark.read.csv(<span class="str">"data/311/"</span>, header=<span class="kw">True</span>, multiLine=<span class="kw">True</span>, schema=complaints_schema)`,
      air_quality:   `<span class="cm"># API returns nested parallel arrays — must flatten</span>\nraw = spark.read.option(<span class="str">"multiLine"</span>, <span class="kw">True</span>).json(<span class="str">"air_quality.json"</span>)\nair_df = (raw\n  .select(arrays_zip(\n      col(<span class="str">"hourly.time"</span>),\n      col(<span class="str">"hourly.pm2_5"</span>),\n      col(<span class="str">"hourly.nitrogen_dioxide"</span>)\n  ).alias(<span class="str">"zipped"</span>))\n  .select(explode(<span class="str">"zipped"</span>).alias(<span class="str">"row"</span>))\n  .select(\n      to_timestamp(col(<span class="str">"row.time"</span>)).alias(<span class="str">"hour_ts"</span>),\n      col(<span class="str">"row.pm2_5"</span>).cast(<span class="str">"double"</span>)\n  ))`,
      weather:       `weather_df = (raw_weather\n  .select(arrays_zip(\n      col(<span class="str">"hourly.time"</span>),\n      col(<span class="str">"hourly.temperature_2m"</span>),\n      col(<span class="str">"hourly.rain"</span>)\n  ).alias(<span class="str">"z"</span>))\n  .select(explode(<span class="str">"z"</span>).alias(<span class="str">"row"</span>))\n  .select(\n      to_timestamp(col(<span class="str">"row.time"</span>)).alias(<span class="str">"hour_ts"</span>),\n      col(<span class="str">"row.temperature_2m"</span>).cast(<span class="str">"double"</span>),\n      col(<span class="str">"row.rain"</span>).cast(<span class="str">"double"</span>)\n  ))`,
      live_traffic:  `<span class="cm"># Fetched fresh each run — uses timestamp in filename</span>\ntimestamp = datetime.now().strftime(<span class="str">"%Y%m%d_%H%M"</span>)\nurl = <span class="str">"https://data.cityofnewyork.us/resource/i4gi-tjb9.json"</span>\nurllib.request.urlretrieve(url, f<span class="str">"data/traffic_{timestamp}.json"</span>)\n\n<span class="cm"># Keep only latest valid reading per road sensor</span>\nw = Window.partitionBy(<span class="str">"link_id"</span>).orderBy(desc(<span class="str">"data_as_of"</span>))\ntraffic_clean = (traffic_raw\n  .filter((col(<span class="str">"status"</span>) == <span class="num">0</span>) & (col(<span class="str">"speed"</span>) > <span class="num">0</span>))\n  .withColumn(<span class="str">"rn"</span>, row_number().over(w))\n  .filter(col(<span class="str">"rn"</span>) == <span class="num">1</span>))`,
    };
    body.innerHTML = `
      <div class="code-block">
        <div class="code-block-header">
          <span class="code-lang">PySpark — ${src.name}</span>
          <span class="code-label">Full ingestion code</span>
        </div>
        <pre>${codeMap[srcId] || '# Code not available'}</pre>
      </div>`;
  }
}

// ═══════════════════════════════════════════════════════════════
// SOURCES
// ═══════════════════════════════════════════════════════════════
function renderSources() {
  const grid = document.getElementById('sources-grid');
  grid.innerHTML = '';
  D.sources.forEach(src => {
    const card = document.createElement('div');
    card.className = 'source-card' + (src.id === activeSourceId ? ' active' : '');
    card.innerHTML = `
      <div class="source-header">
        <div class="source-icon">${src.icon}</div>
        <div style="flex:1">
          <div class="source-name">${src.name}</div>
          <div class="source-subtitle">${src.subtitle}</div>
        </div>
        <span class="format-badge" style="background:${src.formatColor}20;color:${src.formatColor};border:1px solid ${src.formatColor}50">
          ${src.format.split(' ')[0]}
        </span>
      </div>
      <div class="source-metrics">
        <div class="source-metric">
          <span class="source-metric-value">${fmt(src.rawRows)}</span>
          <span class="source-metric-label">Rows</span>
        </div>
        <div class="source-metric">
          <span class="source-metric-value">${src.columns}</span>
          <span class="source-metric-label">Columns</span>
        </div>
        <div class="source-metric">
          <span class="source-metric-value">${src.sizeMB < 1 ? (src.sizeMB*1000).toFixed(0)+'KB' : src.sizeMB+'MB'}</span>
          <span class="source-metric-label">Size</span>
        </div>
      </div>
      <div class="source-api-badge">
        <span style="color:${src.formatColor}">●</span>
        ${src.apiType}
      </div>
    `;
    card.addEventListener('click', () => {
      activeSourceId = src.id;
      document.querySelectorAll('.source-card').forEach(c => c.classList.remove('active'));
      card.classList.add('active');
      renderSchemaPanel(src); renderDataPreview(src);
    });
    grid.appendChild(card);
  });
  renderSchemaPanel(D.sources[0]);
  renderDataPreview(D.sources[0]);
}

function renderSchemaPanel(src) {
  const TC = { integer: 'type-integer', double: 'type-double', string: 'type-string', timestamp: 'type-timestamp' };
  document.getElementById('schema-panel').innerHTML = `
    <div class="schema-panel-header">
      <span>${src.icon}</span>
      <h4>${src.name} — Schema</h4>
      <div style="margin-left:auto" class="chip chip-cyan">printSchema()</div>
    </div>
    <div style="overflow-x:auto">
      <table class="schema-table">
        <thead><tr><th>#</th><th>Column Name</th><th>Type</th><th>Nullable</th></tr></thead>
        <tbody>${src.schema.map((col, i) => `
          <tr>
            <td style="color:var(--text-3)">${i}</td>
            <td class="col-name">${col.name}</td>
            <td><span class="type-badge ${TC[col.type] || ''}">${col.type}</span></td>
            <td style="color:var(--green);font-size:11px">✓ true</td>
          </tr>
        `).join('')}</tbody>
      </table>
    </div>`;
}

function renderDataPreview(src) {
  document.getElementById('data-preview-panel').innerHTML = `
    <div class="schema-panel-header">
      <span>📋</span>
      <h4>Sample Data — ${src.name}</h4>
      <div style="margin-left:auto" class="chip chip-purple">df.show(3)</div>
    </div>
    <div style="overflow-x:auto">
      <table class="data-table">
        <thead><tr>${src.schema.map(c => `<th>${c.name}</th>`).join('')}</tr></thead>
        <tbody>${src.sampleRows.map(row => `
          <tr>${row.map(v => `<td>${v === null ? '<span style="color:var(--text-3)">null</span>' : v}</td>`).join('')}</tr>
        `).join('')}</tbody>
      </table>
    </div>
    <div style="padding:10px 16px;font-size:11px;color:var(--text-3);border-top:1px solid var(--border)">
      Showing 3 of <strong style="color:var(--cyan)">${fmt(src.rawRows)}</strong> rows · ${src.columns} columns
    </div>`;
}

// ═══════════════════════════════════════════════════════════════
// DISTRIBUTED PROCESSING
// ═══════════════════════════════════════════════════════════════
function renderPartitions() {
  const grid = document.getElementById('partitions-grid');
  const cores = document.getElementById('cores-visual');
  grid.innerHTML = D.partitions.map(p => `
    <div class="partition-card">
      <div class="partition-id">Partition ${p.id}</div>
      <div class="partition-rows" id="part-count-${p.id}">0</div>
      <div class="partition-label">rows</div>
      <div class="partition-bar"><div class="partition-bar-fill" style="width:0" data-pct="${p.pct}"></div></div>
      <div style="font-size:10px;color:var(--text-3);margin-top:4px">${p.pct}%</div>
    </div>
  `).join('');
  cores.innerHTML = Array.from({ length: 4 }, (_, i) => `
    <div class="core-pill active"><span class="core-led"></span>Core ${i}</div>
  `).join('');

  // Job stages visualization
  const jsv = document.getElementById('job-stages-viz');
  jsv.innerHTML = `
    <div class="jsv-stage" style="border-color:rgba(0,212,255,0.3)">
      <div class="jsv-stage-label" style="color:var(--cyan)">Stage 1</div>
      <div class="jsv-tasks">
        ${Array(4).fill(0).map((_,i) => `<div class="jsv-task read" title="Task ${i}: FileScan">${i}</div>`).join('')}
        ${Array(4).fill(0).map((_,i) => `<div class="jsv-task filter" title="Task ${i}: Filter">${i}</div>`).join('')}
        ${Array(4).fill(0).map((_,i) => `<div class="jsv-task agg" title="Task ${i}: partial_agg">${i}</div>`).join('')}
      </div>
      <div class="jsv-desc">FileScan → Filter → partial HashAgg (all narrow — no shuffle)</div>
    </div>
    <div style="text-align:center;font-size:12px;padding:4px 0" class="jsv-shuffle-label">⇅ SHUFFLE (Exchange) — rows regrouped by pickup_hour across partitions</div>
    <div class="jsv-stage" style="border-color:rgba(245,158,11,0.3)">
      <div class="jsv-stage-label" style="color:var(--amber)">Stage 2</div>
      <div class="jsv-tasks">
        ${Array(8).fill(0).map((_,i) => `<div class="jsv-task agg" title="Task ${i}: HashAgg">${i}</div>`).join('')}
        ${Array(8).fill(0).map((_,i) => `<div class="jsv-task sort" title="Task ${i}: Sort">${i}</div>`).join('')}
      </div>
      <div class="jsv-desc">final HashAgg → Sort → result (8 shuffle partitions after AQE)</div>
    </div>
  `;

  onVisible(grid, () => {
    D.partitions.forEach(p => {
      countUp(document.getElementById(`part-count-${p.id}`), p.rows, 1400);
    });
    document.querySelectorAll('.partition-bar-fill').forEach(bar => {
      setTimeout(() => bar.style.width = bar.dataset.pct + '%', 100);
    });
  });
}

// ═══════════════════════════════════════════════════════════════
// CLEANING
// ═══════════════════════════════════════════════════════════════
function renderCleaning() {
  const cl = D.cleaningSteps.taxi;
  document.getElementById('cleaning-container').innerHTML = `
    <div class="glass-card" style="padding:28px;margin-bottom:20px">
      <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
        <span style="font-size:24px">🚕</span>
        <div>
          <h3 style="font-size:16px;font-weight:700">Taxi Trips — Data Cleaning (6 Quality Rules)</h3>
          <p style="font-size:12.5px;color:var(--text-2)">Applied in one pass using conditional sums — no repeated scans</p>
        </div>
        <div class="chip chip-purple" style="margin-left:auto">Transformations → Action</div>
      </div>
      <div class="cleaning-flow">
        <div class="cleaning-box">
          <div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-3);margin-bottom:8px;font-family:'JetBrains Mono',monospace">RAW DataFrame</div>
          <div class="row-count-big raw" id="raw-count">0</div>
          <div style="font-size:12px;color:var(--text-3);margin-top:4px">rows ingested</div>
          <div style="margin-top:14px;display:flex;flex-wrap:wrap;gap:6px">
            <span class="chip chip-amber">trips_raw</span>
            <span class="chip chip-amber">19 cols</span>
          </div>
        </div>
        <div class="cleaning-arrow">
          <div class="arrow-line"></div>
          <div class="cleaning-label">filter()</div>
        </div>
        <div class="cleaning-box" style="border-color:rgba(16,185,129,0.3)">
          <div style="font-size:10px;font-weight:700;text-transform:uppercase;color:var(--text-3);margin-bottom:8px;font-family:'JetBrains Mono',monospace">CLEAN DataFrame</div>
          <div class="row-count-big clean" id="clean-count">0</div>
          <div style="font-size:12px;color:var(--text-3);margin-top:4px">rows remaining</div>
          <div style="margin-top:10px;padding:8px 12px;border-radius:8px;background:rgba(16,185,129,0.1);border:1px solid rgba(16,185,129,0.3)">
            <span style="color:var(--green);font-size:13px;font-weight:700">✓ ${(100 - cl.removalPct).toFixed(1)}% retained</span>
            <span style="color:var(--red);font-size:12px;margin-left:10px">↓ ${cl.removedRows.toLocaleString()} removed</span>
          </div>
          <div style="margin-top:10px;display:flex;flex-wrap:wrap;gap:6px">
            <span class="chip chip-green">trips_clean</span>
            <span class="chip chip-green">25 cols</span>
          </div>
        </div>
      </div>
      <h4 style="font-size:12px;font-weight:700;color:var(--text-2);margin-bottom:10px;text-transform:uppercase;letter-spacing:0.5px;margin-top:4px">Quality Rules</h4>
      <ul class="quality-rules">
        ${cl.rules.map(r => `
          <li class="quality-rule">
            <span class="rule-text">✓ ${r.rule}</span>
            <span class="rule-pct">${r.pct}%</span>
            <span class="rule-removed">−${r.removed.toLocaleString()}</span>
          </li>
        `).join('')}
      </ul>
      <div style="margin-top:18px">
        <div style="font-size:11px;color:var(--text-3);margin-bottom:7px;font-weight:700">+ ENRICHED COLUMNS (withColumn)</div>
        <div class="enriched-columns">${cl.enrichedColumns.map(c => `<span class="enriched-col">${c}</span>`).join('')}</div>
      </div>
    </div>`;

  const obs = new IntersectionObserver(e => { e.forEach(en => {
    if (en.isIntersecting) {
      countUp(document.getElementById('raw-count'), cl.rawRows, 1500);
      countUp(document.getElementById('clean-count'), cl.cleanRows, 1500);
      obs.unobserve(en.target);
    }
  })}, { threshold: 0.25 });
  obs.observe(document.getElementById('cleaning-container'));
}

// ═══════════════════════════════════════════════════════════════
// QUERIES
// ═══════════════════════════════════════════════════════════════
function renderQueries() {
  const grid = document.getElementById('queries-grid');
  grid.innerHTML = '';
  D.queries.forEach(q => {
    const card = document.createElement('div');
    card.className = 'query-card';
    const isWide = q.transformationType === 'wide';
    card.innerHTML = `
      <div class="query-card-header">
        <div class="query-id">${q.id}</div>
        <div class="query-category">${q.category}</div>
        <div class="query-title">${q.title}</div>
        <div class="query-subtitle">${q.subtitle}</div>
        <div class="query-ops">
          ${q.operations.map(op => `<span class="op-tag ${isWide ? '' : 'narrow'}">${op}</span>`).join('')}
          <span class="chip ${isWide ? 'chip-amber' : 'chip-cyan'}" style="font-size:9px">
            ${isWide ? '⚡ WIDE/Shuffle' : '🔹 NARROW'}
          </span>
        </div>
      </div>
      <div class="query-timing">
        <div>
          <div class="timing-value">${fmtMs(q.executionTimeMs)}</div>
          <div class="timing-label">execution time</div>
        </div>
        <button class="query-expand-btn" title="Toggle chart">▼</button>
      </div>
      <div class="query-chart-area">
        <canvas id="chart-${q.id}" height="220"></canvas>
        <div id="chart-extra-${q.id}"></div>
      </div>`;
    card.querySelector('.query-expand-btn').addEventListener('click', e => {
      e.stopPropagation();
      const was = card.classList.contains('expanded');
      document.querySelectorAll('.query-card').forEach(c => c.classList.remove('expanded'));
      if (!was) { card.classList.add('expanded'); setTimeout(() => renderQueryChart(q), 60); }
    });
    grid.appendChild(card);
  });
}

const chartDefaults = () => ({
  animation: { duration: 900 },
  plugins: {
    legend: { labels: { color: '#475569', font: { size: 11 } } },
    tooltip: { backgroundColor: '#ffffff', titleColor: '#0f172a', bodyColor: '#475569', borderColor: '#e2e8f0', borderWidth: 1 }
  }
});
const scales = () => ({
  x: { ticks: { color: '#64748b' }, grid: { color: 'rgba(0,0,0,0.05)' } },
  y: { ticks: { color: '#64748b' }, grid: { color: 'rgba(0,0,0,0.05)' } }
});

function renderQueryChart(q) {
  const canvas = document.getElementById(`chart-${q.id}`);
  if (!canvas || queryCharts[q.id]) return;
  const ctx = canvas.getContext('2d');
  const extra = document.getElementById(`chart-extra-${q.id}`);

  if (q.id === 'Q1') {
    queryCharts[q.id] = new Chart(ctx, { type: 'bar', data: { labels: q.data.labels, datasets: [
      { label: 'Trips', data: q.data.trips, backgroundColor: 'rgba(8,145,178,0.25)', borderColor: '#0891b2', borderWidth: 1.5, yAxisID: 'y' },
      { label: 'Avg Fare ($)', data: q.data.avgFare, type: 'line', borderColor: '#d97706', backgroundColor: 'rgba(217,119,6,0.06)', tension: 0.45, yAxisID: 'y1', pointRadius: 3, borderWidth: 2.5 }
    ]}, options: { ...chartDefaults(), scales: { x: scales().x, y: { ...scales().y, title: { display: true, text: 'Trips', color: '#00d4ff' } }, y1: { position: 'right', ticks: { color: '#f59e0b' }, grid: { display: false }, title: { display: true, text: 'Avg Fare $', color: '#f59e0b' } } } }});
  } else if (q.id === 'Q2') {
    queryCharts[q.id] = new Chart(ctx, { type: 'bar', data: { labels: q.data.labels.map(l => l.length > 18 ? l.slice(0,16)+'…' : l), datasets: [{ label: 'Taxi Pickups', data: q.data.trips, backgroundColor: q.data.boroughs.map(b => b === 'Manhattan' ? 'rgba(8,145,178,0.45)' : 'rgba(124,58,237,0.4)'), borderColor: q.data.boroughs.map(b => b === 'Manhattan' ? '#0891b2' : '#7c3aed'), borderWidth: 1.5 }]}, options: { ...chartDefaults(), indexAxis: 'y', scales: { x: scales().x, y: { ticks: { color: '#64748b', font: { size: 10 } }, grid: { display: false } } } }});
  } else if (q.id === 'Q3') {
    queryCharts[q.id] = new Chart(ctx, { type: 'bar', data: { labels: q.data.days, datasets: [
      { label: '12am', data: q.data.matrix.map(r => r[0]),  backgroundColor: 'rgba(0,212,255,0.4)' },
      { label: '6am',  data: q.data.matrix.map(r => r[5]),  backgroundColor: 'rgba(168,85,247,0.4)' },
      { label: '12pm', data: q.data.matrix.map(r => r[11]), backgroundColor: 'rgba(245,158,11,0.5)' },
      { label: '6pm',  data: q.data.matrix.map(r => r[17]), backgroundColor: 'rgba(16,185,129,0.5)' },
      { label: '10pm', data: q.data.matrix.map(r => r[21]), backgroundColor: 'rgba(236,72,153,0.45)' }
    ]}, options: { ...chartDefaults(), scales: scales() }});
  } else if (q.id === 'Q4') {
    queryCharts[q.id] = new Chart(ctx, { type: 'doughnut', data: { labels: q.data.labels.slice(0, 6), datasets: [{ data: q.data.counts.slice(0, 6), backgroundColor: ['rgba(0,212,255,0.7)','rgba(168,85,247,0.7)','rgba(245,158,11,0.7)','rgba(16,185,129,0.7)','rgba(239,68,68,0.7)','rgba(236,72,153,0.7)'], borderColor: ['#00d4ff','#a855f7','#f59e0b','#10b981','#ef4444','#ec4899'], borderWidth: 2 }]}, options: { ...chartDefaults(), cutout: '58%' }});
  } else if (q.id === 'Q5') {
    queryCharts[q.id] = new Chart(ctx, { type: 'bar', data: { labels: q.data.labels, datasets: [
      { label: 'Median (hrs)', data: q.data.medianHrs, backgroundColor: 'rgba(0,212,255,0.5)', borderColor: '#00d4ff', borderWidth: 1.5 },
      { label: 'Average (hrs)', data: q.data.avgHrs, backgroundColor: 'rgba(168,85,247,0.4)', borderColor: '#a855f7', borderWidth: 1.5 }
    ]}, options: { ...chartDefaults(), scales: scales() }});
  } else if (q.id === 'Q6') {
    const bors = [...new Set(q.data.map(d => d.borough))];
    queryCharts[q.id] = new Chart(ctx, { type: 'bar', data: { labels: bors, datasets: [1,2,3].map((rank, ri) => ({
      label: `Rank ${rank}`, data: bors.map(b => { const d = q.data.find(x => x.borough === b && x.rank === rank); return d ? d.count : 0; }),
      backgroundColor: ['rgba(0,212,255,0.65)','rgba(168,85,247,0.5)','rgba(245,158,11,0.45)'][ri],
      borderColor: ['#00d4ff','#a855f7','#f59e0b'][ri], borderWidth: 1.5
    }))}, options: { ...chartDefaults(), scales: { x: { stacked: true, ticks: { color: '#64748b' } }, y: { stacked: true, ticks: { color: '#64748b' }, grid: { color: 'rgba(255,255,255,0.04)' } } } }});
  } else if (q.id === 'Q7') {
    queryCharts[q.id] = new Chart(ctx, { type: 'bar', data: { labels: q.data.labels, datasets: [
      { label: 'Trips', data: q.data.trips, backgroundColor: 'rgba(0,212,255,0.3)', borderColor: '#00d4ff', borderWidth: 1.5, yAxisID: 'y' },
      { label: 'NO₂ (μg/m³)', data: q.data.no2, type: 'line', borderColor: '#ef4444', backgroundColor: 'rgba(239,68,68,0.08)', tension: 0.4, yAxisID: 'y1', pointRadius: 4, borderWidth: 2.5 }
    ]}, options: { ...chartDefaults(), scales: { x: scales().x, y: { ...scales().y }, y1: { position: 'right', ticks: { color: '#ef4444' }, grid: { display: false } } } }});
    if (extra) extra.innerHTML = `<div style="display:flex;gap:10px;margin-top:10px;flex-wrap:wrap">
      <div class="chip chip-red">NO₂ corr r = ${q.data.correlations.no2}</div>
      <div class="chip chip-amber">PM2.5 r = ${q.data.correlations.pm25}</div>
      <div class="chip chip-cyan">CO r = ${q.data.correlations.co}</div>
      <div style="font-size:12px;color:var(--text-2);margin-top:4px;width:100%">Positive correlation: more traffic → more NO₂ (vehicle exhaust)</div>
    </div>`;
  } else if (q.id === 'Q8') {
    queryCharts[q.id] = new Chart(ctx, { type: 'scatter', data: { datasets: [{ label: 'Temp vs Complaints', data: q.data.scatter.map(d => ({ x: d.temp, y: d.complaints })), backgroundColor: 'rgba(0,212,255,0.65)', borderColor: '#00d4ff', pointRadius: 9, pointHoverRadius: 11 }]}, options: { ...chartDefaults(), scales: { x: { ...scales().x, title: { display: true, text: 'Temperature (°C)', color: '#64748b' } }, y: { ...scales().y, title: { display: true, text: 'Heating Complaints', color: '#64748b' } } } }});
    if (extra) extra.innerHTML = `<div class="chip chip-cyan" style="margin-top:10px">Pearson r = ${q.data.correlation} — Strong negative correlation: colder day → more HEAT complaints</div>`;
  } else if (q.id === 'Q9') {
    queryCharts[q.id] = new Chart(ctx, { type: 'bar', data: { labels: q.data.boroughs.map(b => b.name), datasets: [{ label: 'Avg Speed (mph)', data: q.data.boroughs.map(b => b.avgSpeed), backgroundColor: q.data.boroughs.map(b => b.avgSpeed < 20 ? 'rgba(239,68,68,0.6)' : b.avgSpeed < 35 ? 'rgba(245,158,11,0.6)' : 'rgba(16,185,129,0.6)'), borderColor: q.data.boroughs.map(b => b.avgSpeed < 20 ? '#ef4444' : b.avgSpeed < 35 ? '#f59e0b' : '#10b981'), borderWidth: 1.5 }]}, options: { ...chartDefaults(), scales: scales() }});
  } else if (q.id === 'Q10') {
    const maxSpeed = Math.max(...D.queries[9].data.map(b => b.avgSpeed));
    document.getElementById(`chart-${q.id}`).style.display = 'none';
    if (extra) extra.innerHTML = `
      <div style="overflow-x:auto">
        <table class="schema-table">
          <thead><tr><th>Borough</th><th>Population</th><th>Trips/1k</th><th>Complaints/1k</th><th>Median Res (hrs)</th><th>Avg Speed</th></tr></thead>
          <tbody>${D.queries[9].data.map(b => `
            <tr>
              <td style="font-weight:700">${b.borough}</td>
              <td style="color:var(--text-2)">${b.population.toLocaleString()}</td>
              <td style="color:var(--cyan);font-weight:700">${b.tripsPerK.toFixed(1)}</td>
              <td style="color:var(--amber);font-weight:700">${b.complaintsPerK.toFixed(1)}</td>
              <td style="color:${b.medianResHrs > 45 ? 'var(--red)' : 'var(--green)'};font-weight:700">${b.medianResHrs}h</td>
              <td><div style="display:flex;align-items:center;gap:8px"><span style="font-weight:700;color:${b.avgSpeed<20?'var(--red)':b.avgSpeed<35?'var(--amber)':'var(--green)'}">${b.avgSpeed} mph</span><div style="width:${(b.avgSpeed/maxSpeed)*60}px;height:6px;border-radius:3px;background:${b.avgSpeed<20?'var(--red)':b.avgSpeed<35?'var(--amber)':'var(--green)'};flex-shrink:0"></div></div></td>
            </tr>`).join('')}
          </tbody>
        </table>
      </div>
      <div style="font-size:11px;color:var(--text-3);margin-top:8px">Left join on borough · broadcast join for zones · spark.createDataFrame for Census data</div>`;
  }
}

// ═══════════════════════════════════════════════════════════════
// OPTIMIZATIONS
// ═══════════════════════════════════════════════════════════════
function renderOptimizations() {
  const maxMs = Math.max(...D.optimizations.map(o => o.baselineMs));
  document.getElementById('opts-grid').innerHTML = D.optimizations.map(o => `
    <div class="opt-card">
      <div class="opt-card-header">
        <div class="opt-icon">${o.icon}</div>
        <div>
          <div class="opt-id">${o.id}</div>
          <div class="opt-name">${o.name}</div>
        </div>
      </div>
      <code class="opt-operation">${o.operation}</code>
      <p class="opt-desc">
        <strong style="color:var(--red)">Problem:</strong> ${o.problem}<br>
        <strong style="color:var(--green)">Fix:</strong> ${o.fix}
      </p>
      <div class="speedup-bar-wrap">
        <div class="speedup-row">
          <span class="speedup-row-label">Baseline</span>
          <div class="speedup-bar-bg"><div class="speedup-bar-fill baseline opt-bar" data-width="${(o.baselineMs/maxMs)*100}" style="width:0%"></div></div>
          <span class="speedup-val" style="color:var(--red)">${fmtMs(o.baselineMs)}</span>
        </div>
        <div class="speedup-row">
          <span class="speedup-row-label">Optimized</span>
          <div class="speedup-bar-bg"><div class="speedup-bar-fill optimized opt-bar" data-width="${(o.optimizedMs/maxMs)*100}" style="width:0%"></div></div>
          <span class="speedup-val" style="color:var(--green)">${fmtMs(o.optimizedMs)}</span>
        </div>
      </div>
      <div class="speedup-badge">${o.speedup}× faster<span>speedup</span></div>
      <div class="opt-ui-proof">📸 Spark UI: <strong>${o.sparkUIProof}</strong></div>
    </div>
  `).join('');

  new IntersectionObserver(entries => {
    entries.forEach(e => {
      if (e.isIntersecting) {
        e.target.querySelectorAll('.opt-bar').forEach(b => {
          setTimeout(() => b.style.width = b.dataset.width + '%', 100);
        });
        new IntersectionObserver(() => {}).observe(e.target);
      }
    });
  }, { threshold: 0.15 }).observe(document.getElementById('opts-grid'));
}

// ═══════════════════════════════════════════════════════════════
// BENCHMARK
// ═══════════════════════════════════════════════════════════════
function renderBenchmark() {
  const bm = D.benchmark;
  const bmCanvas = document.getElementById('benchmark-chart');
  if (bmCanvas && !bmCanvas.dataset.rendered) {
    bmCanvas.dataset.rendered = '1';
    new Chart(bmCanvas.getContext('2d'), { type: 'line', data: { labels: bm.rowCounts.map(r => fmt(r) + ' rows'), datasets: [
      { label: 'Pandas', data: bm.pandasTimes, borderColor: '#d97706', backgroundColor: 'rgba(217,119,6,0.06)', tension: 0.4, pointRadius: 6, pointBackgroundColor: '#d97706', borderWidth: 2.5 },
      { label: 'PySpark', data: bm.sparkTimes, borderColor: '#0891b2', backgroundColor: 'rgba(8,145,178,0.06)', tension: 0.4, pointRadius: 6, pointBackgroundColor: '#0891b2', borderWidth: 2.5 }
    ]}, options: { ...chartDefaults(), scales: { x: scales().x, y: { ...scales().y, title: { display: true, text: 'Time (seconds)', color: '#64748b' }, ticks: { ...scales().y.ticks, callback: v => v + 's' } } } }});
  }
  const cs = document.getElementById('core-scaling-chart');
  if (cs && !cs.dataset.rendered) {
    cs.dataset.rendered = '1';
    const sc = D.coreScaling;
    new Chart(cs.getContext('2d'), { type: 'bar', data: { labels: sc.cores.map(c => `${c} Core${c > 1 ? 's' : ''}`), datasets: [
      { label: 'Time (s)', data: sc.times, backgroundColor: 'rgba(124,58,237,0.35)', borderColor: '#7c3aed', borderWidth: 1.5, yAxisID: 'y' },
      { label: 'Speedup ×', data: sc.speedups, type: 'line', borderColor: '#059669', backgroundColor: 'transparent', pointRadius: 6, pointBackgroundColor: '#059669', borderWidth: 2.5, yAxisID: 'y1' }
    ]}, options: { ...chartDefaults(), scales: { x: scales().x, y: { ...scales().y, title: { display: true, text: 'Time (s)', color: '#7c3aed' }, ticks: { ...scales().y.ticks, callback: v => v + 's' } }, y1: { position: 'right', ticks: { color: '#059669', callback: v => v + '×' }, grid: { display: false } } } }});
  }
}

// ═══════════════════════════════════════════════════════════════
// PIPELINE STEPPER
// ═══════════════════════════════════════════════════════════════
function renderPipelineStepper() {}

// ═══════════════════════════════════════════════════════════════
// INSIGHTS
// ═══════════════════════════════════════════════════════════════
function renderInsights() {
  document.getElementById('insights-grid').innerHTML = D.insights.map(ins => `
    <div class="insight-card">
      <div class="insight-icon">${ins.icon}</div>
      <div>
        <div class="insight-label">${ins.label}</div>
        <div class="insight-value">${ins.value}</div>
        <div class="insight-detail">${ins.detail}</div>
      </div>
    </div>`).join('');
}

// ═══════════════════════════════════════════════════════════════
// INIT
// ═══════════════════════════════════════════════════════════════
document.addEventListener('DOMContentLoaded', () => {
  initLoading();
  initBackground();
  initNav();
  initHero();

  renderSparkSession();
  renderFetchSimulator();
  renderSources();
  renderPartitions();
  renderCleaning();
  renderQueries();
  renderOptimizations();
  renderInsights();

  // DAG
  setTimeout(() => {
    try { new DAGRenderer('dag-container'); } catch (e) { console.warn('DAG:', e); }
  }, 1900);

  // Benchmark on scroll
  onVisible(document.getElementById('section-benchmark'), renderBenchmark, 0.1);
});
