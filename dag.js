// ============================================================
// dag.js — Animated DAG / pipeline flow visualization (SVG)
// ============================================================

class DAGRenderer {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
    this.svg = null;
    this.animFrames = [];
    this.particlePools = {};
    this.init();
  }

  init() {
    const W = Math.min(this.container.offsetWidth || 1200, 1320);
    const H = 420;

    this.svg = this._createSVG(W, H);
    this.container.appendChild(this.svg);

    this._defineGradients();
    this._drawGraph(W, H);
    this._startParticleAnimation();
  }

  _createSVG(w, h) {
    const svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
    svg.setAttribute('width', '100%');
    svg.setAttribute('viewBox', `0 0 ${w} ${h}`);
    svg.setAttribute('id', 'dag-svg');
    return svg;
  }

  _el(tag, attrs = {}, text = '') {
    const el = document.createElementNS('http://www.w3.org/2000/svg', tag);
    Object.entries(attrs).forEach(([k, v]) => el.setAttribute(k, v));
    if (text) el.textContent = text;
    return el;
  }

  _defineGradients() {
    const defs = this._el('defs');

    // Glow filter
    const filter = this._el('filter', { id: 'glow', x: '-20%', y: '-20%', width: '140%', height: '140%' });
    const fe1 = this._el('feGaussianBlur', { stdDeviation: '3', result: 'coloredBlur' });
    const feMerge = this._el('feMerge');
    feMerge.appendChild(this._el('feMergeNode', { in: 'coloredBlur' }));
    feMerge.appendChild(this._el('feMergeNode', { in: 'SourceGraphic' }));
    filter.appendChild(fe1);
    filter.appendChild(feMerge);
    defs.appendChild(filter);

    // Node gradients
    const grads = [
      { id: 'grad-source', c1: '#00d4ff', c2: '#0099cc' },
      { id: 'grad-spark',  c1: '#a855f7', c2: '#7c3aed' },
      { id: 'grad-output', c1: '#f59e0b', c2: '#d97706' },
      { id: 'grad-live',   c1: '#ef4444', c2: '#b91c1c' },
      { id: 'grad-env',    c1: '#10b981', c2: '#059669' }
    ];
    grads.forEach(({ id, c1, c2 }) => {
      const lg = this._el('linearGradient', { id, x1: '0%', y1: '0%', x2: '100%', y2: '100%' });
      lg.appendChild(this._el('stop', { offset: '0%',   'stop-color': c1 }));
      lg.appendChild(this._el('stop', { offset: '100%', 'stop-color': c2 }));
      defs.appendChild(lg);
    });

    // Arrow marker
    const marker = this._el('marker', {
      id: 'arrow', markerWidth: '10', markerHeight: '7',
      refX: '9', refY: '3.5', orient: 'auto'
    });
    const poly = this._el('polygon', {
      points: '0 0, 10 3.5, 0 7',
      fill: 'rgba(0,212,255,0.6)'
    });
    marker.appendChild(poly);
    defs.appendChild(marker);

    // Animated dash arrow
    const marker2 = this._el('marker', {
      id: 'arrow-purple', markerWidth: '10', markerHeight: '7',
      refX: '9', refY: '3.5', orient: 'auto'
    });
    const poly2 = this._el('polygon', {
      points: '0 0, 10 3.5, 0 7',
      fill: 'rgba(168,85,247,0.7)'
    });
    marker2.appendChild(poly2);
    defs.appendChild(marker2);

    const marker3 = this._el('marker', {
      id: 'arrow-amber', markerWidth: '10', markerHeight: '7',
      refX: '9', refY: '3.5', orient: 'auto'
    });
    const poly3 = this._el('polygon', {
      points: '0 0, 10 3.5, 0 7',
      fill: 'rgba(245,158,11,0.7)'
    });
    marker3.appendChild(poly3);
    defs.appendChild(marker3);

    this.svg.appendChild(defs);
  }

  _drawGraph(W, H) {
    // Layout columns
    const COL = {
      sources: 90,
      spark:   W * 0.40,
      spark2:  W * 0.58,
      out:     W * 0.85
    };

    // Source nodes
    const sources = [
      { y: 50,  label: '🚕 Taxi Trips',   sub: 'Parquet · 3M rows',    grad: 'grad-source', key: 'taxi' },
      { y: 130, label: '📞 311 Requests', sub: 'CSV API · 250k rows',  grad: 'grad-source', key: 'c311' },
      { y: 210, label: '🌫️ Air Quality',  sub: 'JSON API · 744 rows',  grad: 'grad-env',    key: 'air' },
      { y: 290, label: '🌦️ Weather',      sub: 'JSON API · 744 rows',  grad: 'grad-env',    key: 'weather' },
      { y: 370, label: '🚦 Live Traffic', sub: 'REST API · LIVE',       grad: 'grad-live',   key: 'traffic' }
    ];

    // Spark processing nodes
    const sparkNodes = [
      { x: COL.spark,  y: 100, w: 150, h: 56, label: '📥 Read', sub: 'DataFrames + Schemas',   grad: 'grad-spark', key: 'read' },
      { x: COL.spark,  y: 200, w: 150, h: 56, label: '🔧 Clean', sub: 'Filter · Enrich · Dedup', grad: 'grad-spark', key: 'clean' },
      { x: COL.spark,  y: 300, w: 150, h: 56, label: '📡 Distribute', sub: 'Partitions · Tasks', grad: 'grad-spark', key: 'dist' },
      { x: COL.spark2, y: 100, w: 160, h: 56, label: '🔍 10 Queries', sub: 'groupBy · join · window', grad: 'grad-spark', key: 'queries' },
      { x: COL.spark2, y: 200, w: 160, h: 56, label: '⚡ Optimize', sub: 'Cache · Broadcast · AQE', grad: 'grad-spark', key: 'optim' },
      { x: COL.spark2, y: 300, w: 160, h: 56, label: '📊 Analyze', sub: 'Benchmark · Scaling',    grad: 'grad-spark', key: 'analyze' }
    ];

    // Output nodes
    const outputs = [
      { y: 100, label: '📁 Parquet',  sub: 'trips_clean', grad: 'grad-output' },
      { y: 190, label: '📄 CSVs',     sub: '10 results',  grad: 'grad-output' },
      { y: 280, label: '📊 Charts',   sub: 'PNG files',   grad: 'grad-output' },
      { y: 368, label: '🔍 Spark UI', sub: 'DAG · Plans', grad: 'grad-output' }
    ];

    // ── Draw edges first (behind nodes) ─────────────
    const edgeGroup = this._el('g', { id: 'edges' });

    // Source → Read
    sources.forEach(src => {
      const path = this._edgePath(
        COL.sources + 150, src.y + 22,
        COL.spark, sparkNodes[0].y + 28,
        'rgba(0,212,255,0.25)', 'arrow', 2
      );
      path.setAttribute('stroke-dasharray', '6 3');
      path.setAttribute('class', `edge-source edge-to-read`);
      edgeGroup.appendChild(path);
      this._animateDash(path, 30);
    });

    // Read → Clean, Distribute
    [[0,1],[0,2]].forEach(([a,b]) => {
      const p = this._edgePath(
        sparkNodes[a].x + sparkNodes[a].w, sparkNodes[a].y + 28,
        sparkNodes[b].x, sparkNodes[b].y + 28,
        'rgba(168,85,247,0.5)', 'arrow-purple', 2
      );
      edgeGroup.appendChild(p);
    });

    // Clean/Distribute → Queries
    [[1,3],[2,3]].forEach(([a,b]) => {
      const p = this._edgePath(
        sparkNodes[a].x + sparkNodes[a].w, sparkNodes[a].y + 28,
        sparkNodes[b].x, sparkNodes[b].y + 28,
        'rgba(168,85,247,0.5)', 'arrow-purple', 2
      );
      edgeGroup.appendChild(p);
    });

    // Queries → Optimize, Analyze
    [[3,4],[3,5],[4,5]].forEach(([a,b]) => {
      const p = this._edgePath(
        sparkNodes[a].x + sparkNodes[a].w, sparkNodes[a].y + 28,
        sparkNodes[b].x, sparkNodes[b].y + 28,
        'rgba(168,85,247,0.5)', 'arrow-purple', 2
      );
      edgeGroup.appendChild(p);
    });

    // Queries/Optimize/Analyze → Outputs
    const outX = COL.out;
    const qnodes = [3, 4, 5];
    outputs.forEach((out, oi) => {
      const sn = sparkNodes[qnodes[Math.min(oi, 2)]];
      const p = this._edgePath(
        COL.spark2 + 160, sn.y + 28,
        outX, out.y + 20,
        'rgba(245,158,11,0.4)', 'arrow-amber', 2
      );
      p.setAttribute('stroke-dasharray', '5 4');
      edgeGroup.appendChild(p);
      this._animateDash(p, 20);
    });

    this.svg.appendChild(edgeGroup);

    // ── Draw source nodes ────────────────────────────
    const sourceGroup = this._el('g', { id: 'source-nodes' });
    sources.forEach(src => {
      const g = this._el('g', {
        class: 'flow-node dag-source-node',
        'data-key': src.key,
        transform: `translate(0, ${src.y - 22})`
      });

      // glow shadow
      const glow = this._el('rect', {
        x: 2, y: 2, width: 152, height: 44,
        rx: 10, ry: 10,
        fill: `url(#${src.grad})`,
        opacity: '0.2',
        filter: 'url(#glow)'
      });

      const rect = this._el('rect', {
        class: 'flow-node-rect',
        x: 0, y: 0, width: 152, height: 44,
        rx: 10, ry: 10,
        fill: '#ffffff',
        stroke: `url(#${src.grad})`,
        'stroke-width': '1.5'
      });

      const label = this._el('text', {
        x: 10, y: 17, fill: '#0f172a',
        'font-size': '12', 'font-weight': '600',
        'font-family': 'Inter, sans-serif'
      }, src.label);

      const sub = this._el('text', {
        x: 10, y: 32, fill: '#94a3b8',
        'font-size': '10',
        'font-family': 'Inter, sans-serif'
      }, src.sub);

      g.appendChild(glow);
      g.appendChild(rect);
      g.appendChild(label);
      g.appendChild(sub);

      this._addHover(g, rect, src.grad);
      sourceGroup.appendChild(g);
    });
    this.svg.appendChild(sourceGroup);

    // ── Draw Spark nodes ─────────────────────────────
    const sparkGroup = this._el('g', { id: 'spark-nodes' });

    // Spark background box
    const sparkBg = this._el('rect', {
      x: COL.spark - 16,
      y: 60,
      width: (COL.spark2 + 160 + 20) - (COL.spark - 16),
      height: H - 80,
      rx: 16, ry: 16,
      fill: 'rgba(124,58,237,0.03)',
      stroke: 'rgba(124,58,237,0.12)',
      'stroke-width': '1',
      'stroke-dasharray': '6 4'
    });
    sparkGroup.appendChild(sparkBg);

    const sparkLabel = this._el('text', {
      x: COL.spark, y: 50,
      fill: 'rgba(124,58,237,0.5)',
      'font-size': '11', 'font-weight': '700',
      'font-family': 'Inter, sans-serif',
      'letter-spacing': '1'
    }, 'APACHE SPARK (PySpark · local[*] · 4 cores)');
    sparkGroup.appendChild(sparkLabel);

    sparkNodes.forEach(node => {
      const g = this._el('g', {
        class: 'flow-node dag-spark-node',
        'data-key': node.key,
        transform: `translate(${node.x}, ${node.y})`
      });

      const shadow = this._el('rect', {
        x: 3, y: 3, width: node.w, height: node.h,
        rx: 12, ry: 12,
        fill: `url(#${node.grad})`,
        opacity: '0.3',
        filter: 'url(#glow)'
      });
      const rect = this._el('rect', {
        class: 'flow-node-rect',
        x: 0, y: 0, width: node.w, height: node.h,
        rx: 12, ry: 12,
        fill: '#ffffff',
        stroke: `url(#${node.grad})`,
        'stroke-width': '1.5'
      });
      const label = this._el('text', {
        x: 12, y: 22, fill: '#0f172a',
        'font-size': '13', 'font-weight': '700',
        'font-family': 'Inter, sans-serif'
      }, node.label);
      const sub = this._el('text', {
        x: 12, y: 38, fill: '#94a3b8',
        'font-size': '10',
        'font-family': 'Inter, sans-serif'
      }, node.sub);

      g.appendChild(shadow);
      g.appendChild(rect);
      g.appendChild(label);
      g.appendChild(sub);

      this._addHover(g, rect, node.grad);
      sparkGroup.appendChild(g);
    });
    this.svg.appendChild(sparkGroup);

    // ── Draw output nodes ────────────────────────────
    const outGroup = this._el('g', { id: 'output-nodes' });
    outputs.forEach(out => {
      const g = this._el('g', {
        class: 'flow-node dag-out-node',
        transform: `translate(${COL.out}, ${out.y})`
      });
      const rect = this._el('rect', {
        class: 'flow-node-rect',
        x: 0, y: 0, width: 130, height: 40,
        rx: 10, ry: 10,
        fill: '#ffffff',
        stroke: `url(#${out.grad})`,
        'stroke-width': '1.5'
      });
      const label = this._el('text', {
        x: 10, y: 16, fill: '#0f172a',
        'font-size': '12', 'font-weight': '600',
        'font-family': 'Inter, sans-serif'
      }, out.label);
      const sub = this._el('text', {
        x: 10, y: 30, fill: '#94a3b8',
        'font-size': '10',
        'font-family': 'Inter, sans-serif'
      }, out.sub);

      g.appendChild(rect);
      g.appendChild(label);
      g.appendChild(sub);
      this._addHover(g, rect, out.grad);
      outGroup.appendChild(g);
    });
    this.svg.appendChild(outGroup);

    // Store edge paths for particle animation
    this.edgePaths = Array.from(edgeGroup.querySelectorAll('path'));
  }

  _edgePath(x1, y1, x2, y2, stroke, marker, sw = 1.5) {
    const cpx = (x1 + x2) / 2;
    const d = `M ${x1} ${y1} C ${cpx} ${y1}, ${cpx} ${y2}, ${x2} ${y2}`;
    return this._el('path', {
      d,
      fill: 'none',
      stroke,
      'stroke-width': sw,
      'marker-end': `url(#${marker})`
    });
  }

  _animateDash(path, duration) {
    const animate = this._el('animate', {
      attributeName: 'stroke-dashoffset',
      from: '100', to: '0',
      dur: `${duration}s`,
      repeatCount: 'indefinite'
    });
    path.appendChild(animate);
  }

  _addHover(g, rect, grad) {
    g.addEventListener('mouseenter', () => {
      rect.setAttribute('fill', `url(#${grad})`);
      rect.setAttribute('fill-opacity', '0.2');
    });
    g.addEventListener('mouseleave', () => {
      rect.setAttribute('fill', '#ffffff');
      rect.removeAttribute('fill-opacity');
    });
  }

  // Floating particle animation along edges
  _startParticleAnimation() {
    const paths = Array.from(this.svg.querySelectorAll('#edges path'));
    if (!paths.length) return;

    paths.forEach((path, i) => {
      const totalLen = path.getTotalLength ? path.getTotalLength() : 0;
      if (totalLen < 10) return;

      let t = Math.random();
      const speed = 0.0008 + Math.random() * 0.001;
      const isOutput = path.getAttribute('stroke')?.includes('245,158');

      const dot = this._el('circle', {
        r: '3',
        fill: isOutput ? '#f59e0b' : '#00d4ff',
        filter: 'url(#glow)',
        opacity: '0.9'
      });
      this.svg.appendChild(dot);

      const animate = () => {
        t += speed;
        if (t > 1) t = 0;
        try {
          const pt = path.getPointAtLength(t * totalLen);
          dot.setAttribute('cx', pt.x);
          dot.setAttribute('cy', pt.y);
        } catch(e) {}
        requestAnimationFrame(animate);
      };
      setTimeout(() => animate(), i * 300);
    });
  }
}
