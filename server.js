const express = require('express');
const WebSocket = require('ws');
const { Pool } = require('pg');
const app = express();

// PostgreSQL configuration (Supabase Pooler)
const connectionString = process.env.DATABASE_URL || 'postgresql://postgres.kwwfilgkxzvrcxurkpng:ENsy2GrmOFokLBh2@aws-1-us-east-2.pooler.supabase.com:5432/postgres';

const pool = new Pool({
  connectionString: connectionString,
  ssl: { rejectUnauthorized: false }, 
  max: 10,
  // OPTIMIZATION: Keep idle connections for 30s to reduce SSL handshake overhead
  idleTimeoutMillis: 30000, 
  connectionTimeoutMillis: 5000
});

const facilities = ['Sellersburg_Certified_Center', 'Williamsport_Certified_Center', 'North_Las_Vegas_Certified_Center'];
const cache = {};  
// OPTIMIZATION: Reduce cache to 30s for "live" feel
const CACHE_TTL = 30000;  

function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}
// OPTIMIZATION: 1-second debounce for instant UI updates
const debouncedBroadcast = debounce(broadcastUpdate, 1000);  

const lines = ['FTN', 'Cooler', 'Vendor', 'A-Repair'];
const dailyTargets = {
  'Sellersburg_Certified_Center': 120,
  'Williamsport_Certified_Center': 133,
  'North_Las_Vegas_Certified_Center': 80
};
let currentDate = null; 
let lastMilestone = 0;

app.use(express.static('public'));

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK', uptime: process.uptime() });
});

// HTTP endpoint to get the current count
app.get('/getCount', async (req, res) => {
  const { facility, line, date = currentDate } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) {
    return res.status(400).json({ error: 'Invalid facility or line' });
  }
  if (!date) {
    return res.status(400).json({ error: 'Date is required' });
  }
  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT count FROM ProductionCounts WHERE Date = $1 AND Facility = $2 AND Line = $3',
      [date, facility, line]
    );
    const count = result.rows.length > 0 ? result.rows[0].count : 0;
    res.json({ count });
  } catch (err) {
    console.error('GetCount Error:', err);
    res.status(500).json({ error: 'Server error' });
  } finally {
    client.release();
  }
});

// HTTP endpoint to get hourly production rates
app.get('/getHourlyRates', async (req, res) => {
  const { date = currentDate } = req.query;
  if (!date) return res.status(400).json({ error: 'Date is required' });

  const client = await pool.connect();
  try {
    const result = await client.query(
      `SELECT facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as rate
       FROM ProductionEvents
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
          const rate = parseInt(row.rate);
          hourlyRates[f][l][hour] = rate;
        });
      });
    });
    res.json({ hourlyRates });
  } catch (err) {
    console.error('GetHourlyRates Error:', err);
    res.status(500).json({ error: 'Server error' });
  } finally {
    client.release();
  }
});

app.get('/getHistoricalDates', async (req, res) => {
  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT DISTINCT date FROM ProductionCounts ORDER BY date DESC'
    );
    const dates = result.rows.map(row => row.date.toISOString().split('T')[0]);
    res.json({ dates });
  } catch (err) {
    console.error('GetHistoricalDates Error:', err);
    res.status(500).json({ error: 'Server error' });
  } finally {
    client.release();
  }
});

app.get('/getHistoricalData', async (req, res) => {
  const { date } = req.query;
  if (!date) return res.status(400).json({ error: 'Date parameter is required' });

  const client = await pool.connect();
  try {
    const resCounts = await client.query(
      'SELECT facility, line, count FROM ProductionCounts WHERE Date = $1',
      [date]
    );
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
    console.error('GetHistoricalData Error:', err);
    res.status(500).json({ error: 'Server error' });
  } finally {
    client.release();
  }
});

// HTTP endpoints for ESP32
app.post('/increment', async (req, res) => {
  const { facility, line, date } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  if (!date) return res.status(400).json({ error: 'Date required' });

  try {
    await updateCount(facility, line, 1, date);
    debouncedBroadcast();
    res.sendStatus(200);
  } catch (err) {
    console.error('Increment Error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

app.post('/decrement', async (req, res) => {
  const { facility, line, date } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) return res.status(400).json({ error: 'Invalid input' });
  if (!date) return res.status(400).json({ error: 'Date required' });

  try {
    await updateCount(facility, line, -1, date);
    debouncedBroadcast();
    res.sendStatus(200);
  } catch (err) {
    console.error('Decrement Error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

app.post('/resetAllData', async (req, res) => {
  const client = await pool.connect();
  try {
    await client.query('TRUNCATE TABLE ProductionCounts, ProductionEvents');
    lastMilestone = 0;
    broadcastUpdate();
    res.sendStatus(200);
  } catch (err) {
    console.error('ResetAllData Error:', err);
    res.status(500).json({ error: 'Server error' });
  } finally {
    client.release();
  }
});

// OPTIMIZATION: Atomic Upsert to prevent race conditions
async function updateCount(facility, line, delta, date) {
  const client = await pool.connect();
  try {
    // 1. Log the event
    await client.query(
      `INSERT INTO ProductionEvents (date, facility, line, delta, timestamp)
       VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')`,
      [date, facility, line, delta]
    );

    // 2. Atomic UPSERT (Requires UNIQUE constraint on Date, Facility, Line)
    const query = `
      INSERT INTO ProductionCounts (Date, Facility, Line, Count, Timestamp)
      VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')
      ON CONFLICT (Date, Facility, Line) 
      DO UPDATE SET 
        Count = ProductionCounts.Count + $4,
        Timestamp = NOW() AT TIME ZONE 'UTC'
      RETURNING Count;
    `;
    await client.query(query, [date, facility, line, delta]);
    
  } catch (err) {
    console.error('UpdateCount Error:', err);
    throw err;
  } finally {
    client.release();
  }
}

// WebSocket server
const server = app.listen(10000, () => {
  console.log(`Server running at https://production-counter.onrender.com`);
});
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
  ws.on('message', async (message) => {
    try {
      const parsedMessage = JSON.parse(message);
      if (parsedMessage.action === 'setClientDate' && parsedMessage.clientDate) {
        currentDate = parsedMessage.clientDate;
        await broadcastUpdate();
      } else if (parsedMessage.action === 'requestCurrentData') {
        await broadcastUpdate();
      }
    } catch (err) {
      console.error('WebSocket Error:', err);
    }
  });
});

async function broadcastUpdate() {
  if (!currentDate) return;
  
  const client = await pool.connect();
  try {
    // OPTIMIZATION: Fetch Aggregates instead of Raw Rows to save CPU/Memory
    const [currentRes, hourlyRes, totalsRes, peakDayRes, allDailyRes] = await Promise.all([
      client.query('SELECT facility, line, count FROM ProductionCounts WHERE Date = $1', [currentDate]),
      // Optimized: DB sums the hours
      client.query(
        `SELECT facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as hourly_total
         FROM ProductionEvents WHERE date = $1
         GROUP BY facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC')`, 
        [currentDate]
      ),
      client.query('SELECT SUM(count) as total FROM ProductionCounts WHERE Date = $1', [currentDate]),
      client.query(
        `SELECT facility, date, SUM(count) as daily_total
         FROM ProductionCounts GROUP BY facility, date ORDER BY facility, daily_total DESC`
      ),
      client.query(
        `SELECT facility, date, SUM(count) as daily_total
         FROM ProductionCounts GROUP BY facility, date ORDER BY facility, date`
      )
    ]);

    const data = {};
    facilities.forEach(f => {
      data[f] = {};
      lines.forEach(l => {
        const row = currentRes.rows.find(r => r.facility === f && r.line === l);
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
    
    // Process optimized hourly data
    hourlyRes.rows.forEach(row => {
      const { facility, line, hourly_total, hour } = row;
      if (hourlyRates[facility] && hourlyRates[facility][line]) {
        hourlyRates[facility][line][parseInt(hour)] = parseInt(hourly_total);
      }
    });

    const totalProduction = totalsRes.rows[0]?.total || 0;

    const peakProduction = {};
    facilities.forEach(f => {
      const facilityRows = peakDayRes.rows.filter(row => row.facility === f);
      const peakDay = facilityRows.length > 0 ? Math.max(...facilityRows.map(row => parseInt(row.daily_total))) : 0;

      const facilityDailyTotals = allDailyRes.rows
        .filter(row => row.facility === f)
        .map(row => ({ date: new Date(row.date).toISOString().split('T')[0], daily_total: parseInt(row.daily_total) }))
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

    const targetPercentages = {};
    for (const facility of facilities) {
      const facilityTotal = Object.values(data[facility]).reduce((sum, { count }) => sum + count, 0);
      const target = dailyTargets[facility];
      const percentage = target > 0 ? Math.round((facilityTotal / target) * 100) : 0;
      targetPercentages[facility] = percentage;
    }

    const milestone = Math.floor(totalProduction / 100) * 100;
    let notification = null;
    if (milestone > lastMilestone && totalProduction >= milestone) {
      lastMilestone = milestone;
      notification = `Milestone Reached: Total production hit ${milestone} units!`;
    }

    const message = {
      date: currentDate,
      data,
      hourlyRates,
      totalProduction,
      peakProduction,
      targetPercentages,
      notification
    };
    
    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(JSON.stringify(message));
      }
    });
  } catch (err) {
    console.error('Broadcast Update Error:', err);
  } finally {
    client.release();
  }
}
