const express = require('express');
const WebSocket = require('ws');
const pgp = require('pg-promise')(); // For retries and transaction support
const app = express();

// --- 1. STRICT ENVIRONMENT VARIABLE CHECK ---
const connectionString = process.env.DATABASE_URL;

if (!connectionString) {
  console.error('CRITICAL ERROR: DATABASE_URL environment variable is missing.');
  console.error('Please set this variable in your Render dashboard before deploying.');
  process.exit(1); // Fail fast to prevent silent connection timeouts
}

// pg-promise config with retry logic
const db = pgp(connectionString);
const queryWithRetry = async (queryText, params, retries = 3, backoff = 1000) => {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      return await db.any(queryText, params);
    } catch (err) {
      console.error(`Query attempt ${attempt} failed:`, err.stack || err);
      if ((err.message || '').includes('timeout') && attempt < retries) {
        console.warn(`Query retry ${attempt}/${retries}`);
        await new Promise(res => setTimeout(res, backoff * attempt));
      } else {
        throw err;
      }
    }
  }
};

const facilities = ['Sellersburg_Certified_Center', 'Williamsport_Certified_Center', 'North_Las_Vegas_Certified_Center'];
const lines = ['FTN', 'Cooler', 'Vendor', 'A-Repair'];
const dailyTargets = {
  'Sellersburg_Certified_Center': 120,
  'Williamsport_Certified_Center': 133,
  'North_Las_Vegas_Certified_Center': 80
};

let lastMilestone = 0;

// --- CACHING LAYER ---
const routeCache = {
  counts: {},
  historicalDates: { dates: null, timestamp: 0 }
};
const CACHE_TTL_MS = 5000; 

function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

// --- CONCURRENCY LOCK ---
let isBroadcasting = false;
let pendingBroadcast = false;

async function triggerBroadcast() {
  if (isBroadcasting) {
    pendingBroadcast = true; 
    return;
  }
  isBroadcasting = true;
  pendingBroadcast = false;

  try {
    await executeBroadcast();
  } catch (err) {
    console.error('Broadcast trigger error:', err.stack || err);
  } finally {
    isBroadcasting = false;
    if (pendingBroadcast) {
      setTimeout(triggerBroadcast, 100);
    }
  }
}

const debouncedBroadcast = debounce(triggerBroadcast, 300);  

app.use(express.static('public'));

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK', uptime: process.uptime() });
});

app.get('/getCount', async (req, res) => {
  const { facility, line, date = new Date().toISOString().split('T')[0] } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) {
    return res.status(400).json({ error: 'Invalid facility or line' });
  }
  if (!date) return res.status(400).json({ error: 'Date is required' });
  
  const cacheKey = `${facility}_${line}_${date}`;
  const now = Date.now();
  
  if (routeCache.counts[cacheKey] && (now - routeCache.counts[cacheKey].timestamp < CACHE_TTL_MS)) {
    return res.json({ count: routeCache.counts[cacheKey].value });
  }

  try {
    const result = await queryWithRetry(
      'SELECT count FROM productioncounts WHERE date = $1 AND facility = $2 AND line = $3',
      [date, facility, line]
    );
    const count = result.length > 0 ? result[0].count : 0;
    
    routeCache.counts[cacheKey] = { value: count, timestamp: now };
    res.json({ count });
  } catch (err) {
    console.error('GetCount Error:', err.stack || err);
    res.status(500).json({ error: 'Server error: ' + (err.message || 'Unknown error') });
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
        const facilityRates = result.filter(row => row.facility === f && row.line === l);
        facilityRates.forEach(row => {
          const hour = parseInt(row.hour);
          hourlyRates[f][l][hour] = parseInt(row.rate);
        });
      });
    });
    res.json({ hourlyRates });
  } catch (err) {
    console.error('GetHourlyRates Error:', err.stack || err);
    res.status(500).json({ error: 'Server error: ' + (err.message || 'Unknown error') });
  }
});

app.get('/getHistoricalDates', async (req, res) => {
  const now = Date.now();
  if (routeCache.historicalDates.dates && (now - routeCache.historicalDates.timestamp < 60000)) {
    return res.json({ dates: routeCache.historicalDates.dates });
  }

  try {
    const result = await queryWithRetry('SELECT DISTINCT date FROM productioncounts ORDER BY date DESC');
    const dates = result.map(row => {
       const d = new Date(row.date);
       return new Date(d.getTime() - (d.getTimezoneOffset() * 60000)).toISOString().split('T')[0];
    });
    
    routeCache.historicalDates = { dates, timestamp: now };
    res.json({ dates });
  } catch (err) {
    console.error('GetHistoricalDates Error:', err.stack || err);
    res.status(500).json({ error: 'Server error: ' + (err.message || 'Unknown error') });
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
        const row = resCounts.find(r => r.facility === f && r.line === l);
        data[f][l] = { count: row ? row.count : 0, timestamp: new Date().toISOString() };
      });
    });
    res.json({ data });
  } catch (err) {
    console.error('GetHistoricalData Error:', err.stack || err);
    res.status(500).json({ error: 'Server error: ' + (err.message || 'Unknown error') });
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
    
    debouncedBroadcast();
    res.sendStatus(200);
  } catch (err) {
    console.error('Increment Error:', err.stack || err);
    res.status(500).json({ error: 'Server error: ' + (err.message || 'Unknown error') });
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
    
    debouncedBroadcast();
    res.sendStatus(200);
  } catch (err) {
    console.error('Decrement Error:', err.stack || err);
    res.status(500).json({ error: 'Server error: ' + (err.message || 'Unknown error') });
  }
});

app.post('/resetAllData', async (req, res) => {
  try {
    await db.tx(async t => {
      await t.none('TRUNCATE TABLE productioncounts, productionevents');
    });
    lastMilestone = 0;
    routeCache.counts = {};
    routeCache.historicalDates = { dates: null, timestamp: 0 };
    triggerBroadcast();
    res.sendStatus(200);
  } catch (err) {
    console.error('ResetAllData Error:', err.stack || err);
    res.status(500).json({ error: 'Server error: ' + (err.message || 'Unknown error') });
  }
});

async function updateCount(facility, line, delta, date) {
  try {
    await db.tx(async t => {
      await t.none(
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
      await t.any(query, [date, facility, line, delta]);
    });
  } catch (err) {
    console.error('UpdateCount Error:', err.stack || err);
    throw err;
  }
}

// WebSocket server
const PORT = process.env.PORT || 10000;
const server = app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  ws.on('message', async (message) => {
    try {
      const parsedMessage = JSON.parse(message);
      
      if (parsedMessage.action === 'setClientDate' && parsedMessage.clientDate) {
        ws.currentDate = parsedMessage.clientDate;
      } else if (parsedMessage.action === 'requestCurrentData') {
        debouncedBroadcast();
      } else if (parsedMessage.action === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (err) {
      console.error('WebSocket Error:', err.stack || err);
    }
  });
});

// Helper to reliably format Postgres date responses back to YYYY-MM-DD strings
const parseDbDate = (dbDate) => {
  if (typeof dbDate === 'string') return dbDate.split('T')[0];
  const d = new Date(dbDate);
  return new Date(d.getTime() - (d.getTimezoneOffset() * 60000)).toISOString().split('T')[0];
};

// --- 2. BATCHED BROADCAST QUERY (NO N+1 LOOPS) ---
async function executeBroadcast() {
  const activeDates = new Set();
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.currentDate) {
      activeDates.add(client.currentDate);
    }
  });

  if (activeDates.size === 0) return;
  const datesArray = Array.from(activeDates);
  
  try {
    await db.tx(async t => {
      // Fetch Global / Peak Stats (Independent of active dates)
      const peakDayRes = await queryWithRetry(
        `SELECT facility, date, SUM(count) as daily_total
         FROM productioncounts GROUP BY facility, date ORDER BY facility, daily_total DESC`
      );
      
      const allDailyRes = await queryWithRetry(
        `SELECT facility, date, SUM(count) as daily_total
         FROM productioncounts GROUP BY facility, date ORDER BY facility, date`
      );

      const peakProduction = {};
      facilities.forEach(f => {
        const facilityRows = peakDayRes.filter(row => row.facility === f);
        const peakDay = facilityRows.length > 0 ? Math.max(...facilityRows.map(row => parseInt(row.daily_total))) : 0;

        const facilityDailyTotals = allDailyRes
          .filter(row => row.facility === f)
          .map(row => ({ date: parseDbDate(row.date), daily_total: parseInt(row.daily_total) }))
          .sort((a, b) => a.date.localeCompare(b.date));

        let maxWeeklySum = 0;
        if (facilityDailyTotals.length > 0) {
          const availableDays = Math.min(facilityDailyTotals.length, 7);
          for (let i = 0; i <= facilityDailyTotals.length - availableDays; i++) {
            const weekSum = facilityDailyTotals.slice(i, i + availableDays).reduce((sum, day) => sum + day.daily_total, 0);
            maxWeeklySum = Math.max(maxWeeklySum, weekSum);
          }
        }
        peakProduction[f] = { peakDay, peakWeekly: maxWeeklySum };
      });

      // Batched Queries: Fetch everything for ALL active dates in just 3 queries
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

      // Filter and structure data for each specific active date
      for (const date of datesArray) {
        const data = {};
        facilities.forEach(f => {
          data[f] = {};
          lines.forEach(l => {
            const row = currentRes.find(r => r.facility === f && r.line === l && parseDbDate(r.date) === date);
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
        
        hourlyRes.forEach(row => {
          if (parseDbDate(row.date) === date) {
            const { facility, line, hourly_total, hour } = row;
            if (hourlyRates[facility] && hourlyRates[facility][line]) {
              hourlyRates[facility][line][parseInt(hour)] = parseInt(hourly_total);
            }
          }
        });

        const totalRow = totalsRes.find(r => parseDbDate(r.date) === date);
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
          date: date,
          data,
          hourlyRates,
          totalProduction,
          peakProduction,
          targetPercentages,
          notification
        });
        
        wss.clients.forEach((c) => {
          if (c.readyState === WebSocket.OPEN && c.currentDate === date) {
            c.send(messageStr);
          }
        });
      }
    });
  } catch (err) {
    console.error('Broadcast Update Error:', err.stack || err);
  } 
}
