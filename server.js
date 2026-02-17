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

// === ROCK-SOLID CONNECTION POOL ===
const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false },
  max: 15,                            
  // 🚀 CRITICAL FIX: Set to 5s. Forces Node to close the connection before Supabase secretly kills it!
  idleTimeoutMillis: 5000,           
  connectionTimeoutMillis: 30000,     
  statement_timeout: 30000,           
  query_timeout: 0,                   
  keepAlive: true,                    
  keepAliveInitialDelayMillis: 2000   
});

pool.on('error', (err) => console.warn('Idle DB client error:', err.message));

// === RETRY WRAPPER ===
async function queryWithRetry(sql, params = [], retries = 3) {
  let delay = 1000;
  for (let i = 1; i <= retries; i++) {
    try {
      const res = await pool.query(sql, params);
      return res;
    } catch (err) {
      if (i === retries) throw err;
      if (err.message.includes('timeout') || err.message.includes('terminated') || err.code === 'ECONNRESET') {
        const jitter = Math.floor(Math.random() * 500);
        await new Promise(r => setTimeout(r, delay + jitter));
        delay *= 2; 
      } else {
        throw err; 
      }
    }
  }
}

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
// === 🚀 THE ZERO-HANG RAM STATE ENGINE 🚀 ===
// ============================================================================
// The screens ONLY read from this RAM object. They NEVER touch the database directly.
const SYSTEM_RAM = {
  historicalData: {}, 
  hourlyRates: {},    
  peaks: {},          
  totals: {}          
};

// Helper to ensure the RAM object has the proper structure so the frontend doesn't crash
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

// Initialize peaks with 0s
facilities.forEach(f => SYSTEM_RAM.peaks[f] = { peakDay: 0, peakWeekly: 0 });

app.use(express.static('public'));
app.get('/health', (req, res) => res.json({ status: 'OK', uptime: process.uptime() }));


// ============================================================================
// === 🚀 100% DECOUPLED ENDPOINTS (Instant Responses, 0 DB Queries) ===
// ============================================================================
// Even if 1,000 Pis refresh simultaneously, these return RAM instantly.

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
// === WRITE QUEUE (Saves clicks instantly to RAM, writes to DB later) ===
// ============================================================================
const pendingWrites = {};

app.post('/increment', (req, res) => {
  const { facility, line, date } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  if (!date) return res.status(400).json({ error: 'Date required' });

  pendingWrites[`${facility}|${line}|${date}`] = (pendingWrites[`${facility}|${line}|${date}`] || 0) + 1; 
  res.sendStatus(200); 
});

app.post('/decrement', (req, res) => {
  const { facility, line, date } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  if (!date) return res.status(400).json({ error: 'Date required' });

  pendingWrites[`${facility}|${line}|${date}`] = (pendingWrites[`${facility}|${line}|${date}`] || 0) - 1; 
  res.sendStatus(200); 
});

// Write worker
async function processBatchQueue() {
  const keys = Object.keys(pendingWrites);
  if (keys.length === 0) {
    setTimeout(processBatchQueue, 3000); 
    return;
  }

  const snapshot = { ...pendingWrites };
  for (const k of keys) delete pendingWrites[k]; 

  let changesMade = false;

  for (const key in snapshot) {
    const delta = snapshot[key];
    if (delta === 0) continue; 
    const [facility, line, date] = key.split('|');

    // Optimistic UI Update: Make it feel instant for the users by updating RAM right now
    initRamForDate(date);
    SYSTEM_RAM.historicalData[date][facility][line].count += delta;
    SYSTEM_RAM.totals[date] += delta;

    try {
      await updateCount(facility, line, delta, date);
      changesMade = true;
    } catch (err) {
      console.error('Batch write failed, saving to memory to retry later:', err.message);
      pendingWrites[key] = (pendingWrites[key] || 0) + delta;
      
      // Revert optimistic update if DB failed
      SYSTEM_RAM.historicalData[date][facility][line].count -= delta;
      SYSTEM_RAM.totals[date] -= delta;
    }
  }

  if (changesMade) executeBroadcast(); // Push the optimistic update to screens immediately
  setTimeout(processBatchQueue, 3000); 
}
setTimeout(processBatchQueue, 3000);

// ============================================================================
// === BACKGROUND DB POLLER (Keeps the RAM updated safely every 10s) ===
// ============================================================================
async function syncDatabaseToRAM() {
  try {
    const activeDates = new Set();
    activeDates.add(getLocalDateString());
    wss.clients.forEach(c => { if (c.currentDate) activeDates.add(c.currentDate); });
    const datesArray = Array.from(activeDates);

    datesArray.forEach(initRamForDate);

    // Fetch Peaks
    const peakRes = await queryWithRetry('SELECT facility, peak_day, peak_weekly FROM peakproduction', [], 2);
    peakRes.rows.forEach(r => {
      SYSTEM_RAM.peaks[r.facility] = { peakDay: parseInt(r.peak_day) || 0, peakWeekly: parseInt(r.peak_weekly) || 0 };
    });

    // Fetch Counts
    const countRes = await queryWithRetry('SELECT date, facility, line, count FROM productioncounts WHERE date = ANY($1::date[])', [datesArray], 2);
    countRes.rows.forEach(r => {
      const d = parseDbDate(r.date);
      if (SYSTEM_RAM.historicalData[d] && SYSTEM_RAM.historicalData[d][r.facility]) {
        SYSTEM_RAM.historicalData[d][r.facility][r.line].count = parseInt(r.count);
      }
    });

    // Fetch Hourly Rates
    const hourlyRes = await queryWithRetry(
      `SELECT date, facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as hourly_total 
       FROM productionevents WHERE date = ANY($1::date[]) 
       GROUP BY date, facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC')`, 
      [datesArray], 2
    );
    
    // Clear rates for active dates before repopulating
    datesArray.forEach(d => {
      facilities.forEach(f => lines.forEach(l => SYSTEM_RAM.hourlyRates[d][f][l] = Array(24).fill(0)));
    });

    hourlyRes.rows.forEach(r => {
      const d = parseDbDate(r.date);
      if (SYSTEM_RAM.hourlyRates[d] && SYSTEM_RAM.hourlyRates[d][r.facility]) {
        SYSTEM_RAM.hourlyRates[d][r.facility][r.line][parseInt(r.hour)] = parseInt(r.hourly_total);
      }
    });

    // Fetch Totals
    const totalsRes = await queryWithRetry('SELECT date, SUM(count) as total FROM productioncounts WHERE date = ANY($1::date[]) GROUP BY date', [datesArray], 2);
    totalsRes.rows.forEach(r => {
      SYSTEM_RAM.totals[parseDbDate(r.date)] = parseInt(r.total);
    });

    // Broadcast the perfectly synced data
    executeBroadcast();
    
  } catch (err) {
    // If this fails, no big deal! The frontend RAM is still serving the old data until it works again!
    console.error('Background Sync Error:', err.message); 
  } finally {
    setTimeout(syncDatabaseToRAM, 10000); 
  }
}
setTimeout(syncDatabaseToRAM, 5000); // Start the sync 5 seconds after server boots up


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
        executeBroadcast(); // Instantly broadcasts from RAM, zero DB lookup!
      } else if (parsed.action === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (e) {}
  });
});

// === RAM BROADCAST LOGIC ===
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
