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

// === ULTRA-STABLE CONNECTION POOL ===
const pool = new Pool({
  connectionString,
  ssl: { rejectUnauthorized: false },
  // STRICT LIMIT: Prevents the "MaxClientsInSessionMode" error from Supabase
  max: 10,                            
  idleTimeoutMillis: 30000,           
  connectionTimeoutMillis: 20000, // Wait 20s in Node's internal queue before throwing an error
  statement_timeout: 15000,           
  keepAlive: true,                    
  keepAliveInitialDelayMillis: 2000   
});

pool.on('error', (err, client) => {
  console.warn('Idle DB client error (expected on free tiers):', err.message);
});

pool.on('connect', () => console.log('Pool acquired a new DB connection'));
pool.on('remove', () => console.log('Pool released a DB connection'));

// === RETRY WRAPPER WITH JITTER ===
async function queryWithRetry(sql, params = [], retries = 5) {
  let delay = 1000;
  for (let i = 1; i <= retries; i++) {
    try {
      const res = await pool.query(sql, params);
      return res;
    } catch (err) {
      console.error(`DB query attempt ${i} failed:`, err.message);
      if (i === retries) throw err;
      
      // Catch network hiccups AND max client connection limits
      if (err.message.includes('timeout') || err.message.includes('terminated') || err.message.includes('Max clients reached') || err.code === 'ETIMEDOUT' || err.code === 'ENETUNREACH' || err.code === 'ECONNRESET') {
        // Add jitter to prevent concurrent queries from retrying at the exact same millisecond
        const jitter = Math.floor(Math.random() * 500);
        console.warn(`Network/Pool limit hit. Retrying in ${delay + jitter}ms...`);
        await new Promise(r => setTimeout(r, delay + jitter));
        delay *= 2; 
      } else {
        throw err; 
      }
    }
  }
}

// === AUTO DATA CLEANUP (30-DAY RETENTION FOR EVENTS ONLY) ===
async function cleanupOldData() {
  try {
    console.log('Running automated database cleanup for EVENTS older than 30 days...');
    const eventRes = await queryWithRetry(`DELETE FROM productionevents WHERE date < NOW() - INTERVAL '30 days' RETURNING id`);
    console.log(`Cleanup complete: Removed ${eventRes.rowCount || 0} old events.`);
  } catch (err) {
    console.error('Scheduled Data Cleanup Error:', err.message);
  }
}

// Run cleanup once shortly after server startup, then every 12 hours
setTimeout(cleanupOldData, 60000); 
setInterval(cleanupOldData, 12 * 60 * 60 * 1000);

// === CONSTANTS & CACHING ===
const facilities = ['Sellersburg_Certified_Center', 'Williamsport_Certified_Center', 'North_Las_Vegas_Certified_Center'];
const lines = ['FTN', 'Cooler', 'Vendor', 'A-Repair'];
const dailyTargets = {
  'Sellersburg_Certified_Center': 120,
  'Williamsport_Certified_Center': 133,
  'North_Las_Vegas_Certified_Center': 80
};

let lastMilestone = 0;

const routeCache = { counts: {} };
const CACHE_TTL_MS = 5000;

function debounce(fn, wait) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), wait);
  };
}

let isBroadcasting = false;
let pendingBroadcast = false;

async function triggerBroadcast() {
  if (isBroadcasting) { pendingBroadcast = true; return; }
  isBroadcasting = true;
  pendingBroadcast = false;

  try {
    await executeBroadcast();
  } catch (e) {
    console.error('Broadcast failed:', e.message);
  } finally {
    isBroadcasting = false;
    if (pendingBroadcast) setTimeout(triggerBroadcast, 100);
  }
}

const debouncedBroadcast = debounce(triggerBroadcast, 400);

app.use(express.static('public'));
app.get('/health', (req, res) => res.json({ status: 'OK', uptime: process.uptime() }));

// === ENDPOINTS ===
app.get('/getCount', async (req, res) => {
  const { facility, line, date = new Date().toISOString().split('T')[0] } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid facility/line' });

  const cacheKey = `${facility}_${line}_${date}`;
  const now = Date.now();
  if (routeCache.counts[cacheKey] && now - routeCache.counts[cacheKey].timestamp < CACHE_TTL_MS) {
    return res.json({ count: routeCache.counts[cacheKey].value });
  }

  try {
    const result = await queryWithRetry(
      'SELECT count FROM productioncounts WHERE date = $1 AND facility = $2 AND line = $3',
      [date, facility, line]
    );
    const count = result.rows.length ? result.rows[0].count : 0;
    routeCache.counts[cacheKey] = { value: count, timestamp: now };
    res.json({ count });
  } catch (err) {
    console.error('GetCount Error:', err.message);
    res.status(500).json({ error: 'Server error' });
  }
});

app.get('/getHourlyRates', async (req, res) => {
  const { date = new Date().toISOString().split('T')[0] } = req.query;
  if (!date) return res.status(400).json({ error: 'Date is required' });

  try {
    const result = await queryWithRetry(
      `SELECT facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as rate
       FROM productionevents
       WHERE date = $1
       GROUP BY facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC')
       ORDER BY facility, line, hour`,
      [date]
    );

    const hourlyRates = {};
    facilities.forEach(f => {
      hourlyRates[f] = {};
      lines.forEach(l => {
        hourlyRates[f][l] = Array(24).fill(0);
        const facilityRates = result.rows.filter(row => row.facility === f && row.line === l);
        facilityRates.forEach(row => {
          const hour = parseInt(row.hour);
          hourlyRates[f][l][hour] = parseInt(row.rate);
        });
      });
    });
    res.json({ hourlyRates });
  } catch (err) {
    console.error('GetHourlyRates Error:', err.message);
    res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

app.get('/getHistoricalData', async (req, res) => {
  const { date } = req.query;
  if (!date) return res.status(400).json({ error: 'Date parameter is required' });

  try {
    const resCounts = await queryWithRetry('SELECT facility, line, count FROM productioncounts WHERE date = $1', [date]);
    const data = {};
    facilities.forEach(f => {
      data[f] = {};
      lines.forEach(l => {
        const row = resCounts.rows.find(r => r.facility === f && r.line === l);
        data[f][l] = { count: row ? row.count : 0, timestamp: new Date().toISOString() };
      });
    });
    res.json({ data });
  } catch (err) {
    console.error('GetHistoricalData Error:', err.message);
    res.status(500).json({ error: 'Server error' });
  }
});

app.post('/increment', async (req, res) => {
  const { facility, line, date } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  if (!date) return res.status(400).json({ error: 'Date required' });

  try {
    await updateCount(facility, line, 1, date);
    
    const cacheKey = `${facility}_${line}_${date}`;
    if (routeCache.counts[cacheKey]) routeCache.counts[cacheKey].timestamp = 0;
    
    await checkAndUpdatePeaks(facility, date);
    
    debouncedBroadcast();
    res.sendStatus(200);
  } catch (err) {
    console.error('Increment Error:', err.message);
    res.status(500).json({ error: 'Server error' });
  }
});

app.post('/decrement', async (req, res) => {
  const { facility, line, date } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  if (!date) return res.status(400).json({ error: 'Date required' });

  try {
    await updateCount(facility, line, -1, date);
    
    const cacheKey = `${facility}_${line}_${date}`;
    if (routeCache.counts[cacheKey]) routeCache.counts[cacheKey].timestamp = 0;
    
    await recalculateAllPeaks();
    
    debouncedBroadcast();
    res.sendStatus(200);
  } catch (err) {
    console.error('Decrement Error:', err.message);
    res.status(500).json({ error: 'Server error' });
  }
});

app.post('/resetAllData', async (req, res) => {
  try {
    await queryWithRetry('TRUNCATE TABLE productioncounts, productionevents');
    await queryWithRetry('UPDATE peakproduction SET peak_day = 0, peak_weekly = 0');
    lastMilestone = 0;
    routeCache.counts = {};
    triggerBroadcast();
    res.sendStatus(200);
  } catch (err) {
    console.error('ResetAllData Error:', err.message);
    res.status(500).json({ error: 'Server error' });
  }
});

// === RECOVERY ENDPOINT ===
app.get('/forceRecalculatePeaks', async (req, res) => {
  try {
    await recalculateAllPeaks();
    debouncedBroadcast();
    res.send('✅ Peaks successfully recalculated and broadcasted based on current productioncounts table!');
  } catch (err) {
    console.error('Recovery Error:', err.message);
    res.status(500).send('Error recalculating peaks: ' + err.message);
  }
});

// === DATABASE HELPERS ===

async function updateCount(facility, line, delta, date) {
  let client;
  let hasError = false;
  try {
    client = await pool.connect();
    await client.query('BEGIN');
    await client.query(
      `INSERT INTO productionevents (date, facility, line, delta, timestamp)
       VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')`,
      [date, facility, line, delta]
    );

    const query = `
      INSERT INTO productioncounts (date, facility, line, count, timestamp)
      VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')
      ON CONFLICT (date, facility, line) 
      DO UPDATE SET 
        count = productioncounts.count + $4,
        timestamp = NOW() AT TIME ZONE 'UTC'
      RETURNING count;
    `;
    await client.query(query, [date, facility, line, delta]);
    await client.query('COMMIT');
  } catch (err) {
    hasError = true;
    if (client) {
        try { await client.query('ROLLBACK'); } catch (rbErr) { console.warn('Rollback failed:', rbErr.message); }
    }
    console.error('UpdateCount Error:', err.message);
    throw err;
  } finally {
    if (client) {
        // Permanently destroy the corrupted connection if there was a fatal network error
        client.release(hasError);
    }
  }
}

// OPTIMIZED: Condenses 3 sequential queries down to 1 using a CTE.
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
    if (!result.rows.length) return;

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

async function recalculateAllPeaks() {
  try {
    const peakQuery = `
      WITH daily_sums AS (
        SELECT facility, date, SUM(count) as daily_total
        FROM productioncounts
        GROUP BY facility, date
      ),
      weekly_sums AS (
        SELECT facility, daily_total,
               SUM(daily_total) OVER (
                 PARTITION BY facility
                 ORDER BY date
                 ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
               ) as weekly_total
        FROM daily_sums
      )
      SELECT facility, MAX(daily_total) as peak_day, MAX(weekly_total) as peak_weekly
      FROM weekly_sums GROUP BY facility;
    `;
    const res = await queryWithRetry(peakQuery);
    
    for (const f of facilities) {
      const row = res.rows.find(r => r.facility === f);
      const pd = row && row.peak_day ? parseInt(row.peak_day) : 0;
      const pw = row && row.peak_weekly ? parseInt(row.peak_weekly) : 0;
      await queryWithRetry(
        'UPDATE peakproduction SET peak_day = $1, peak_weekly = $2 WHERE facility = $3',
        [pd, pw, f]
      );
    }
  } catch (err) {
    console.error('Error recalculating peaks:', err.message);
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
        debouncedBroadcast();
      } else if (parsed.action === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (e) {
      console.error('WS message error:', e.message);
    }
  });
});

const parseDbDate = (dbDate) => {
  if (typeof dbDate === 'string') return dbDate.split('T')[0];
  const d = new Date(dbDate);
  return new Date(d.getTime() - (d.getTimezoneOffset() * 60000)).toISOString().split('T')[0];
};

// === BROADCAST ===
async function executeBroadcast() {
  const activeDates = new Set();
  wss.clients.forEach(c => {
    if (c.readyState === WebSocket.OPEN && c.currentDate) activeDates.add(c.currentDate);
  });

  if (activeDates.size === 0) return;
  const datesArray = Array.from(activeDates);

  try {
    // OPTIMIZED: Run sequentially to ensure a broadcast only uses 1 pool connection at a time.
    const peakRes = await queryWithRetry('SELECT facility, peak_day, peak_weekly FROM peakproduction');
    
    const currentRes = await queryWithRetry(
      'SELECT date, facility, line, count FROM productioncounts WHERE date = ANY($1::date[])', 
      [datesArray]
    );
    
    const hourlyRes = await queryWithRetry(
      `SELECT date, facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as hourly_total
       FROM productionevents WHERE date = ANY($1::date[])
       GROUP BY date, facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC')`,
      [datesArray]
    );
    
    const totalsRes = await queryWithRetry(
      'SELECT date, SUM(count) as total FROM productioncounts WHERE date = ANY($1::date[]) GROUP BY date', 
      [datesArray]
    );

    const peakProduction = {};
    
    facilities.forEach(f => {
      peakProduction[f] = { peakDay: 0, peakWeekly: 0 };
    });
    
    peakRes.rows.forEach(r => {
      peakProduction[r.facility] = {
        peakDay: parseInt(r.peak_day) || 0,
        peakWeekly: parseInt(r.peak_weekly) || 0
      };
    });

    for (const date of datesArray) {
      const data = {};
      facilities.forEach(f => {
        data[f] = {};
        lines.forEach(l => {
          const row = currentRes.rows.find(r => r.facility === f && r.line === l && parseDbDate(r.date) === date);
          data[f][l] = { count: row ? row.count : 0, timestamp: new Date().toISOString() };
        });
      });

      const hourlyRates = {};
      facilities.forEach(f => {
        hourlyRates[f] = {};
        lines.forEach(l => {
          hourlyRates[f][l] = Array(24).fill(0);
        });
      });

      hourlyRes.rows.forEach(row => {
        if (parseDbDate(row.date) === date) {
          const { facility, line, hourly_total, hour } = row;
          if (hourlyRates[facility] && hourlyRates[facility][line]) {
            hourlyRates[facility][line][parseInt(hour)] = parseInt(hourly_total);
          }
        }
      });

      const totalRow = totalsRes.rows.find(r => parseDbDate(r.date) === date);
      const totalProduction = totalRow ? parseInt(totalRow.total) : 0;

      const targetPercentages = {};
      for (const facility of facilities) {
        const facilityTotal = Object.values(data[facility]).reduce((sum, { count }) => sum + count, 0);
        const target = dailyTargets[facility];
        const percentage = target > 0 ? Math.round((facilityTotal / target) * 100) : 0;
        targetPercentages[facility] = percentage;
      }

      const todayStr = new Date().toISOString().split('T')[0];
      let notification = null;
      if (date === todayStr) {
        const milestone = Math.floor(totalProduction / 100) * 100;
        if (milestone > lastMilestone && totalProduction >= milestone) {
          lastMilestone = milestone;
          notification = `Milestone Reached: Total production hit ${milestone} units!`;
        }
      }

      const messageStr = JSON.stringify({
        date,
        data,
        hourlyRates,
        totalProduction,
        peakProduction,
        targetPercentages,
        notification
      });

      wss.clients.forEach(c => {
        if (c.readyState === WebSocket.OPEN && c.currentDate === date) c.send(messageStr);
      });
    }
  } catch (err) {
    console.error('Broadcast Update Error:', err.message);
  }
}
