const express = require('express');
const WebSocket = require('ws');
const { Pool } = require('pg');
const app = express();

// === ENVIRONMENT CHECK ===
const connectionString = process.env.DATABASE_URL;
if (!connectionString) {
  console.error('CRITICAL ERROR: DATABASE_URL is missing');
  process.exit(1);
}

// === 🚀 THE "NO-STALE" CONNECTION POOL ===
const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false },
  max: 5,                             
  // 🚀 THE MAGIC FIX: Forces Node to close the connection after 5 seconds of quiet time
  idleTimeoutMillis: 5000,           
  connectionTimeoutMillis: 60000,     // Still gives Supabase plenty of time to respond
  keepAlive: true,                    // OS-level TCP keep-alive 
  keepAliveInitialDelayMillis: 2000
});

pool.on('error', (err) => console.warn('Idle DB client error (safely evicted):', err.message));

// === RETRY WRAPPER ===
async function queryWithRetry(sql, params = [], retries = 3) {
  let delay = 1000;
  for (let i = 1; i <= retries; i++) {
    try {
      const res = await pool.query(sql, params);
      return res;
    } catch (err) {
      if (i === retries) throw err;
      const jitter = Math.floor(Math.random() * 500);
      console.warn(`Query hiccup (${err.message}). Retrying in ${delay + jitter}ms...`);
      await new Promise(r => setTimeout(r, delay + jitter));
      delay *= 2; 
    }
  }
}

// === AUTOMATED DATA CLEANUP (Events AND Counts) ===
async function cleanupOldData() {
  try {
    // Correctly purges both tables beyond 30 days
    await queryWithRetry(`DELETE FROM productionevents WHERE date < NOW() - INTERVAL '30 days'`);
    await queryWithRetry(`DELETE FROM productioncounts WHERE date < NOW() - INTERVAL '30 days'`);
    console.log(`30-day database cleanup complete for events and counts.`);
  } catch (err) {
    console.error('Scheduled Data Cleanup Error:', err.message);
  }
}

setTimeout(cleanupOldData, 60000); 
setInterval(cleanupOldData, 12 * 60 * 60 * 1000);

// === CONSTANTS & TARGETS ===
const facilities = ['Sellersburg_Certified_Center', 'Williamsport_Certified_Center', 'North_Las_Vegas_Certified_Center'];
const lines = ['FTN', 'Cooler', 'Vendor', 'A-Repair'];
const dailyTargets = {
  'Sellersburg_Certified_Center': 120,
  'Williamsport_Certified_Center': 133,
  'North_Las_Vegas_Certified_Center': 80
};

let lastMilestone = 0;

// === DATE HELPERS ===
const parseDbDate = (dbDate) => {
  if (typeof dbDate === 'string') return dbDate.split('T')[0];
  const d = new Date(dbDate);
  return new Date(d.getTime() - (d.getTimezoneOffset() * 60000)).toISOString().split('T')[0];
};

function getLocalDateString() {
  const d = new Date();
  const year = d.getFullYear();
  const month = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

// ============================================================================
// === THE TRUE INSTANT RAM STATE ENGINE ===
// ============================================================================
const SYSTEM_RAM = {
  historicalData: {}, 
  hourlyRates: {},    
  peaks: {},          
  totals: {}          
};

function initRamForDate(date) {
  if (!SYSTEM_RAM.historicalData[date]) {
    SYSTEM_RAM.historicalData[date] = {};
    facilities.forEach(f => {
      SYSTEM_RAM.historicalData[date][f] = {};
      lines.forEach(l => { SYSTEM_RAM.historicalData[date][f][l] = { count: 0, timestamp: new Date().toISOString() }; });
    });
  }
  if (!SYSTEM_RAM.hourlyRates[date]) {
    SYSTEM_RAM.hourlyRates[date] = {};
    facilities.forEach(f => {
      SYSTEM_RAM.hourlyRates[date][f] = {};
      lines.forEach(l => { SYSTEM_RAM.hourlyRates[date][f][l] = Array(24).fill(0); });
    });
  }
  if (SYSTEM_RAM.totals[date] === undefined) {
    SYSTEM_RAM.totals[date] = 0;
  }
}

facilities.forEach(f => SYSTEM_RAM.peaks[f] = { peakDay: 0, peakWeekly: 0 });

app.use(express.static('public'));
app.get('/health', (req, res) => res.json({ status: 'OK', uptime: process.uptime() }));


// ============================================================================
// === DECOUPLED ENDPOINTS ===
// ============================================================================
app.get('/getCount', (req, res) => {
  const { facility, line, date = getLocalDateString() } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  initRamForDate(date);
  res.json({ count: SYSTEM_RAM.historicalData[date][facility][line].count });
});

app.get('/getHourlyRates', (req, res) => {
  const { date = getLocalDateString() } = req.query;
  initRamForDate(date);
  res.json({ hourlyRates: SYSTEM_RAM.hourlyRates[date] });
});

app.get('/getHistoricalData', (req, res) => {
  const { date } = req.query;
  if (!date) return res.status(400).json({ error: 'Date required' });
  initRamForDate(date);
  res.json({ data: SYSTEM_RAM.historicalData[date] });
});


// ============================================================================
// === INSTANT WRITE QUEUE ===
// ============================================================================
const pendingWrites = {};

app.post('/increment', (req, res) => {
  const { facility, line, date } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  if (!date) return res.status(400).json({ error: 'Date required' });

  initRamForDate(date);
  SYSTEM_RAM.historicalData[date][facility][line].count += 1;
  SYSTEM_RAM.totals[date] += 1;
  
  pendingWrites[`${facility}|${line}|${date}`] = (pendingWrites[`${facility}|${line}|${date}`] || 0) + 1; 
  
  executeBroadcast(); 
  res.json({ count: SYSTEM_RAM.historicalData[date][facility][line].count }); 
});

app.post('/decrement', (req, res) => {
  const { facility, line, date } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  if (!date) return res.status(400).json({ error: 'Date required' });

  initRamForDate(date);
  if (SYSTEM_RAM.historicalData[date][facility][line].count > 0) {
      SYSTEM_RAM.historicalData[date][facility][line].count -= 1;
      SYSTEM_RAM.totals[date] -= 1;
  }

  pendingWrites[`${facility}|${line}|${date}`] = (pendingWrites[`${facility}|${line}|${date}`] || 0) - 1; 
  executeBroadcast(); 
  res.json({ count: SYSTEM_RAM.historicalData[date][facility][line].count }); 
});

async function processBatchQueue() {
  const keys = Object.keys(pendingWrites);
  if (keys.length === 0) {
    setTimeout(processBatchQueue, 3000); 
    return;
  }

  const snapshot = { ...pendingWrites };
  for (const k of keys) delete pendingWrites[k]; 

  for (const key in snapshot) {
    const delta = snapshot[key];
    if (delta === 0) continue; 
    const [facility, line, date] = key.split('|');

    try {
      await updateCount(facility, line, delta, date);
      await checkAndUpdatePeaks(facility, date);
    } catch (err) {
      console.error('Batch write failed, keeping delta in queue to retry:', err.message);
      pendingWrites[key] = (pendingWrites[key] || 0) + delta;
    }
  }

  setTimeout(processBatchQueue, 3000); 
}
setTimeout(processBatchQueue, 3000);

// ============================================================================
// === BACKGROUND DB POLLER ===
// ============================================================================
async function syncDatabaseToRAM() {
  try {
    const activeDates = new Set();
    activeDates.add(getLocalDateString());
    wss.clients.forEach(c => { if (c.currentDate) activeDates.add(c.currentDate); });
    const datesArray = Array.from(activeDates);

    datesArray.forEach(initRamForDate);

    const peakRes = await queryWithRetry('SELECT facility, peak_day, peak_weekly FROM peakproduction', [], 2);
    if (peakRes && peakRes.rows) {
      peakRes.rows.forEach(r => {
        SYSTEM_RAM.peaks[r.facility] = { peakDay: parseInt(r.peak_day) || 0, peakWeekly: parseInt(r.peak_weekly) || 0 };
      });
    }

    const countRes = await queryWithRetry('SELECT date, facility, line, count FROM productioncounts WHERE date = ANY($1::date[])', [datesArray], 2);
    if (countRes && countRes.rows) {
      countRes.rows.forEach(r => {
        const d = parseDbDate(r.date);
        if (SYSTEM_RAM.historicalData[d] && SYSTEM_RAM.historicalData[d][r.facility]) {
          SYSTEM_RAM.historicalData[d][r.facility][r.line].count = parseInt(r.count);
        }
      });
    }

    const hourlyRes = await queryWithRetry(
      `SELECT date, facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as hourly_total 
       FROM productionevents WHERE date = ANY($1::date[]) 
       GROUP BY date, facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC')`, 
      [datesArray], 2
    );
    
    datesArray.forEach(d => {
      facilities.forEach(f => lines.forEach(l => SYSTEM_RAM.hourlyRates[d][f][l] = Array(24).fill(0)));
    });

    if (hourlyRes && hourlyRes.rows) {
      hourlyRes.rows.forEach(r => {
        const d = parseDbDate(r.date);
        if (SYSTEM_RAM.hourlyRates[d] && SYSTEM_RAM.hourlyRates[d][r.facility]) {
          SYSTEM_RAM.hourlyRates[d][r.facility][r.line][parseInt(r.hour)] = parseInt(r.hourly_total);
        }
      });
    }

    const totalsRes = await queryWithRetry('SELECT date, SUM(count) as total FROM productioncounts WHERE date = ANY($1::date[]) GROUP BY date', [datesArray], 2);
    if (totalsRes && totalsRes.rows) {
      totalsRes.rows.forEach(r => {
        SYSTEM_RAM.totals[parseDbDate(r.date)] = parseInt(r.total);
      });
    }

    executeBroadcast();
    
  } catch (err) {
    console.error('Background Sync Error:', err.message); 
  } finally {
    setTimeout(syncDatabaseToRAM, 15 * 60 * 1000); 
  }
}
setTimeout(syncDatabaseToRAM, 2000); 


// === DATABASE HELPERS ===
async function updateCount(facility, line, delta, date) {
  let client;
  let hasError = false;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO productionevents (date, facility, line, delta, timestamp) VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')`,
      [date, facility, line, delta]
    );
    await client.query(
      `INSERT INTO productioncounts (date, facility, line, count, timestamp) VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')
       ON CONFLICT (date, facility, line) DO UPDATE SET count = productioncounts.count + $4, timestamp = NOW() AT TIME ZONE 'UTC'`,
      [date, facility, line, delta]
    );
    await client.query('COMMIT');
  } catch (err) {
    hasError = true;
    if (client) {
        try { await client.query('ROLLBACK'); } catch (rbErr) {}
    }
    throw err;
  } finally {
    if (client) client.release(hasError);
  }
}

async function checkAndUpdatePeaks(facility, date) {
  try {
    const singleQuery = `
      WITH day_calc AS (
        SELECT COALESCE(SUM(count), 0) as total 
        FROM productioncounts WHERE facility = $1 AND date = $2
      ),
      week_calc AS (
        SELECT COALESCE(SUM(count), 0) as total 
        FROM productioncounts WHERE facility = $1 AND date BETWEEN $2::date - 6 AND $2::date
      )
      SELECT 
        (SELECT total FROM day_calc) as day_total,
        (SELECT total FROM week_calc) as week_total,
        peak_day, peak_weekly
      FROM peakproduction WHERE facility = $1;
    `;
    
    const result = await queryWithRetry(singleQuery, [facility, date]);
    if (!result || !result.rows.length) return;

    const row = result.rows[0];
    const currentPeakDay = parseInt(row.peak_day) || 0;
    const currentPeakWeekly = parseInt(row.peak_weekly) || 0;
    const dayTotal = parseInt(row.day_total) || 0;
    const weekTotal = parseInt(row.week_total) || 0;

    let updated = false;
    let newPeakDay = currentPeakDay;
    let newPeakWeekly = currentPeakWeekly;

    if (dayTotal > currentPeakDay) {
      newPeakDay = dayTotal;
      updated = true;
    }
    if (weekTotal > currentPeakWeekly) {
      newPeakWeekly = weekTotal;
      updated = true;
    }

    if (updated) {
      await queryWithRetry(
        'UPDATE peakproduction SET peak_day = $1, peak_weekly = $2 WHERE facility = $3',
        [newPeakDay, newPeakWeekly, facility]
      );
    }
  } catch (err) {
    console.error('Error in checkAndUpdatePeaks:', err.message);
  }
}

// === WEB SOCKET ===
const PORT = process.env.PORT || 10000;
const server = app.listen(PORT, () => console.log(`Server running on ${PORT}`));
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    try {
      const parsed = JSON.parse(message);
      if (parsed.action === 'setClientDate' && parsed.clientDate) {
        ws.currentDate = parsed.clientDate;
      } else if (parsed.action === 'requestCurrentData') {
        executeBroadcast(); 
      } else if (parsed.action === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (e) {}
  });
});

function executeBroadcast() {
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN && ws.currentDate) {
      const date = ws.currentDate;
      initRamForDate(date);

      const targetPercentages = {};
      for (const facility of facilities) {
        const facilityTotal = Object.values(SYSTEM_RAM.historicalData[date][facility]).reduce((sum, { count }) => sum + count, 0);
        const target = dailyTargets[facility];
        targetPercentages[facility] = target > 0 ? Math.round((facilityTotal / target) * 100) : 0;
      }

      const todayStr = getLocalDateString();
      let notification = null;
      if (date === todayStr) {
        const milestone = Math.floor(SYSTEM_RAM.totals[date] / 100) * 100;
        if (milestone > lastMilestone && SYSTEM_RAM.totals[date] >= milestone) {
          lastMilestone = milestone;
          notification = `Milestone Reached: Total production hit ${milestone} units!`;
        }
      }

      const messageStr = JSON.stringify({
        date,
        data: SYSTEM_RAM.historicalData[date],
        hourlyRates: SYSTEM_RAM.hourlyRates[date],
        totalProduction: SYSTEM_RAM.totals[date],
        peakProduction: SYSTEM_RAM.peaks,
        targetPercentages,
        notification
      });

      ws.send(messageStr);
    }
  });
}
