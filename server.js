const express = require('express');
const WebSocket = require('ws');
const { Pool } = require('pg');
const app = express();

// PostgreSQL configuration (Supabase Pooler)
const connectionString = process.env.DATABASE_URL || 'postgresql://postgres.kwwfilgkxzvrcxurkpng:ENsy2GrmOFokLBh2@aws-1-us-east-2.pooler.supabase.com:6543/postgres';

// Optimized for Supabase PgBouncer (Port 6543)
const pool = new Pool({
  connectionString: connectionString,
  ssl: { rejectUnauthorized: false }, 
  max: 15, // Reduced from 20 to respect Supabase free tier PgBouncer limits
  idleTimeoutMillis: 10000, // Reduced to 10s so Node drops idle connections BEFORE Supabase kills them
  connectionTimeoutMillis: 15000, 
  statement_timeout: 10000 // Force Postgres to drop hanging queries after 10s
});

// CRITICAL: Catch silent idle connection drops from Supabase to prevent app crashes
pool.on('error', (err, client) => {
  console.error('Unexpected error on idle client:', err.message);
  // pg-pool will automatically remove this dead client and safely create a new one
});

const facilities = ['Sellersburg_Certified_Center', 'Williamsport_Certified_Center', 'North_Las_Vegas_Certified_Center'];
const lines = ['FTN', 'Cooler', 'Vendor', 'A-Repair'];
const dailyTargets = {
  'Sellersburg_Certified_Center': 120,
  'Williamsport_Certified_Center': 133,
  'North_Las_Vegas_Certified_Center': 80
};

let lastMilestone = 0;

function debounce(func, wait) {
  let timeout;
  return function(...args) {
    clearTimeout(timeout);
    timeout = setTimeout(() => func(...args), wait);
  };
}

// OPTIMIZATION: 300ms debounce for near-instant UI updates without database flooding
const debouncedBroadcast = debounce(broadcastUpdate, 300);  

app.use(express.static('public'));

app.get('/health', (req, res) => {
  res.status(200).json({ status: 'OK', uptime: process.uptime() });
});

// HTTP endpoint to get the current count
app.get('/getCount', async (req, res) => {
  const { facility, line, date = new Date().toISOString().split('T')[0] } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) {
    return res.status(400).json({ error: 'Invalid facility or line' });
  }
  if (!date) {
    return res.status(400).json({ error: 'Date is required' });
  }
  try {
    const result = await pool.query(
      'SELECT count FROM productioncounts WHERE date = $1 AND facility = $2 AND line = $3',
      [date, facility, line]
    );
    const count = result.rows.length > 0 ? result.rows[0].count : 0;
    res.json({ count });
  } catch (err) {
    console.error('GetCount Error:', err.message, err.stack);
    res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

// HTTP endpoint to get hourly production rates
app.get('/getHourlyRates', async (req, res) => {
  const { date = new Date().toISOString().split('T')[0] } = req.query;
  if (!date) return res.status(400).json({ error: 'Date is required' });

  try {
    const result = await pool.query(
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
          const rate = parseInt(row.rate);
          hourlyRates[f][l][hour] = rate;
        });
      });
    });
    res.json({ hourlyRates });
  } catch (err) {
    console.error('GetHourlyRates Error:', err.message, err.stack);
    res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

app.get('/getHistoricalDates', async (req, res) => {
  try {
    const result = await pool.query(
      'SELECT DISTINCT date FROM productioncounts ORDER BY date DESC'
    );
    const dates = result.rows.map(row => new Date(row.date).toISOString().split('T')[0]);
    res.json({ dates });
  } catch (err) {
    console.error('GetHistoricalDates Error:', err.message, err.stack);
    res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

app.get('/getHistoricalData', async (req, res) => {
  const { date } = req.query;
  if (!date) return res.status(400).json({ error: 'Date parameter is required' });

  try {
    const resCounts = await pool.query(
      'SELECT facility, line, count FROM productioncounts WHERE date = $1',
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
    console.error('GetHistoricalData Error:', err.message, err.stack);
    res.status(500).json({ error: 'Server error: ' + err.message });
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
    console.error('Increment Error:', err.message, err.stack);
    res.status(500).json({ error: 'Server error: ' + err.message });
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
    console.error('Decrement Error:', err.message, err.stack);
    res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

app.post('/resetAllData', async (req, res) => {
  try {
    await pool.query('TRUNCATE TABLE productioncounts, productionevents');
    lastMilestone = 0;
    broadcastUpdate();
    res.sendStatus(200);
  } catch (err) {
    console.error('ResetAllData Error:', err.message, err.stack);
    res.status(500).json({ error: 'Server error: ' + err.message });
  }
});

async function updateCount(facility, line, delta, date) {
  let client;
  try {
    client = await pool.connect();
    
    // Start a transaction block to safely hold the PgBouncer connection
    await client.query('BEGIN');
    
    // 1. Log the event
    await client.query(
      `INSERT INTO productionevents (date, facility, line, delta, timestamp)
       VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')`,
      [date, facility, line, delta]
    );

    // 2. Atomic UPSERT (Requires UNIQUE constraint on date, facility, line)
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
    if (client) await client.query('ROLLBACK');
    console.error('UpdateCount Error:', err.message, err.stack);
    throw err;
  } finally {
    if (client) client.release();
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
        // Save date state individually per client so multiple users can view different dates
        ws.currentDate = parsedMessage.clientDate;
      } else if (parsedMessage.action === 'requestCurrentData') {
        // Debounce this to prevent pool exhaustion on mass page loads
        debouncedBroadcast();
      } else if (parsedMessage.action === 'ping') {
        ws.send(JSON.stringify({ type: 'pong' }));
      }
    } catch (err) {
      console.error('WebSocket Error:', err.message, err.stack);
    }
  });
});

async function broadcastUpdate() {
  // Identify all unique dates currently being requested by connected active clients
  const activeDates = new Set();
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN && client.currentDate) {
      activeDates.add(client.currentDate);
    }
  });

  if (activeDates.size === 0) return;
  
  let client;
  try {
    client = await pool.connect();

    // Await generic peak/all-daily data sequentially to prevent protocol hanging
    const peakDayRes = await client.query(
      `SELECT facility, date, SUM(count) as daily_total
       FROM productioncounts GROUP BY facility, date ORDER BY facility, daily_total DESC`
    );
    
    const allDailyRes = await client.query(
      `SELECT facility, date, SUM(count) as daily_total
       FROM productioncounts GROUP BY facility, date ORDER BY facility, date`
    );

    // Calculate peakProduction mapping once per broadcast
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

    // 2. Iterate over each unique active date requested by clients
    for (const date of activeDates) {
      
      // Await specific date queries sequentially
      const currentRes = await client.query('SELECT facility, line, count FROM productioncounts WHERE date = $1', [date]);
      
      const hourlyRes = await client.query(
        `SELECT facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as hourly_total
         FROM productionevents WHERE date = $1
         GROUP BY facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC')`, 
        [date]
      );
      
      const totalsRes = await client.query('SELECT SUM(count) as total FROM productioncounts WHERE date = $1', [date]);

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
      
      hourlyRes.rows.forEach(row => {
        const { facility, line, hourly_total, hour } = row;
        if (hourlyRates[facility] && hourlyRates[facility][line]) {
          hourlyRates[facility][line][parseInt(hour)] = parseInt(hourly_total);
        }
      });

      const totalProduction = totalsRes.rows[0]?.total || 0;

      const targetPercentages = {};
      for (const facility of facilities) {
        const facilityTotal = Object.values(data[facility]).reduce((sum, { count }) => sum + count, 0);
        const target = dailyTargets[facility];
        const percentage = target > 0 ? Math.round((facilityTotal / target) * 100) : 0;
        targetPercentages[facility] = percentage;
      }

      // Check milestones only for the current day to avoid historical spam
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
      
      // Target only clients whose specific date matches the data we just generated
      wss.clients.forEach((c) => {
        if (c.readyState === WebSocket.OPEN && c.currentDate === date) {
          c.send(messageStr);
        }
      });
    }
  } catch (err) {
    console.error('Broadcast Update Error:', err.message, err.stack);
  } finally {
    if (client) {
      client.release();
    }
  }
}
