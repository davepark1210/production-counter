const express = require('express');
const WebSocket = require('ws');
const { Pool } = require('pg');
const app = express();

// PostgreSQL configuration (Neon.tech)
const pool = new Pool({
  host: process.env.PGHOST || 'ep-falling-tree-a58of1lj-pooler.us-east-2.aws.neon.tech',
  user: process.env.PGUSER || 'neondb_owner',
  password: process.env.PGPASSWORD || 'npg_4Vw0dKkJeILi',
  database: process.env.PGDATABASE || 'neondb',
  port: process.env.PGPORT || 5432,
  ssl: { rejectUnauthorized: false }
});

const facilities = ['Sellersburg_Certified_Center', 'Williamsport_Certified_Center', 'North_Las_Vegas_Certified_Center'];
const lines = ['FTN', 'VV'];
let currentDate = new Date().toISOString().split('T')[0]; // Initial date, will be updated by client
let lastMilestone = 0;

// Serve static files
app.use(express.static('public'));

// HTTP endpoint to get the current count for a facility and line
app.get('/getCount', async (req, res) => {
  const { facility, line, date = currentDate } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) {
    console.log(`Invalid facility or line: ${facility}, ${line}`);
    return res.status(400).json({ error: 'Invalid facility or line' });
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

// HTTP endpoint to get hourly production rates (in UTC)
app.get('/getHourlyRates', async (req, res) => {
  const { date = currentDate } = req.query;
  const client = await pool.connect();
  try {
    console.log(`Fetching hourly rates for date: ${date}`);
    const result = await client.query(
      `SELECT facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as rate
       FROM ProductionEvents
       WHERE date = $1
       GROUP BY facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC')
       ORDER BY facility, line, hour`,
      [date]
    );
    console.log('Hourly rates query result (raw rows):', result.rows);

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
          console.log(`Hourly rate for ${f}, ${l}, UTC hour ${hour}: ${rate}`);
        });
      });
    });
    console.log('Constructed hourlyRates (UTC):', hourlyRates);

    res.json({ hourlyRates });
  } catch (err) {
    console.error('GetHourlyRates Error:', err);
    res.status(500).json({ error: 'Server error' });
  } finally {
    client.release();
  }
});

// HTTP endpoint to get historical dates
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

// HTTP endpoint to get historical data for a specific date
app.get('/getHistoricalData', async (req, res) => {
  const { date } = req.query;
  if (!date) {
    return res.status(400).json({ error: 'Date parameter is required' });
  }
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
  const { facility, line } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) {
    console.log(`Invalid facility or line: ${facility}, ${line}`);
    return res.sendStatus(400);
  }
  try {
    await updateCount(facility, line, 1);
    broadcastUpdate();
    res.sendStatus(200);
  } catch (err) {
    console.error('Increment Error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

app.post('/decrement', async (req, res) => {
  const { facility, line } = req.query;
  if (!facilities.includes(facility) || !lines.includes(line)) {
    console.log(`Invalid facility or line: ${facility}, ${line}`);
    return res.sendStatus(400);
  }
  try {
    await updateCount(facility, line, -1);
    broadcastUpdate();
    res.sendStatus(200);
  } catch (err) {
    console.error('Decrement Error:', err);
    res.status(500).json({ error: 'Server error' });
  }
});

// Reset all data endpoint
app.post('/resetAllData', async (req, res) => {
  const client = await pool.connect();
  try {
    // Truncate ProductionCounts and ProductionEvents tables
    await client.query('TRUNCATE TABLE ProductionCounts, ProductionEvents');
    lastMilestone = 0;
    console.log('All data reset successfully');
    broadcastUpdate();
    res.sendStatus(200);
  } catch (err) {
    console.error('ResetAllData Error:', err);
    res.status(500).json({ error: 'Server error' });
  } finally {
    client.release();
  }
});

// Update count in database and log the event
async function updateCount(facility, line, delta) {
  const client = await pool.connect();
  try {
    // Store timestamp in UTC
    console.log(`Inserting into ProductionEvents with UTC timestamp: NOW() AT TIME ZONE 'UTC'`);
    await client.query(
      `INSERT INTO ProductionEvents (date, facility, line, delta, timestamp)
       VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')`,
      [currentDate, facility, line, delta]
    );

    console.log(`Updating count for ${facility}, ${line}, delta: ${delta}`);
    const res = await client.query(
      'SELECT Count FROM ProductionCounts WHERE Date = $1 AND Facility = $2 AND Line = $3',
      [currentDate, facility, line]
    );
    console.log('Query result:', res.rows);
    if (res.rows.length > 0) {
      const newCount = Math.max(res.rows[0].count + delta, 0);
      await client.query(
        `UPDATE ProductionCounts SET Count = $1, Timestamp = NOW() AT TIME ZONE 'UTC'
         WHERE Date = $2 AND Facility = $3 AND Line = $4`,
        [newCount, currentDate, facility, line]
      );
      console.log(`Updated count to ${newCount}`);
    } else {
      const initialCount = delta > 0 ? delta : 0;
      await client.query(
        `INSERT INTO ProductionCounts (Date, Facility, Line, Count, Timestamp)
         VALUES ($1, $2, $3, $4, NOW() AT TIME ZONE 'UTC')`,
        [currentDate, facility, line, initialCount]
      );
      console.log(`Inserted new count: ${initialCount}`);
    }
  } catch (err) {
    console.error('Increment/Decrement Error:', err);
    throw err;
  } finally {
    client.release();
  }
}

// WebSocket server (Render expects port 10000 for HTTP and WebSocket)
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
        console.log(`Updated currentDate to client's date: ${currentDate}`);
        // Send initial data after setting the date
        const data = await getCurrentData();
        const hourlyRates = await getHourlyRates();
        const totalProduction = await getTotalDailyProduction();
        const peakProduction = await getPeakProduction();
        ws.send(JSON.stringify({ date: currentDate, data, hourlyRates, totalProduction, peakProduction }));
      }
      if (parsedMessage.action === 'requestCurrentData') {
        const data = await getCurrentData();
        const hourlyRates = await getHourlyRates();
        const totalProduction = await getTotalDailyProduction();
        const peakProduction = await getPeakProduction();
        ws.send(JSON.stringify({ date: currentDate, data, hourlyRates, totalProduction, peakProduction }));
      }
    } catch (err) {
      console.error('Failed to parse WebSocket message:', err);
    }
  });
});

async function getCurrentData() {
  const client = await pool.connect();
  try {
    console.log('Fetching current data from database for date:', currentDate);
    const res = await client.query(
      'SELECT facility, line, count FROM ProductionCounts WHERE Date = $1',
      [currentDate]
    );
    console.log('Database query result:', res.rows);
    const data = {};
    facilities.forEach(f => {
      data[f] = {};
      lines.forEach(l => {
        const row = res.rows.find(r => r.facility === f && r.line === l);
        data[f][l] = { count: row ? row.count : 0, timestamp: new Date().toISOString() };
        console.log(`Count for ${f}, ${l}: ${data[f][l].count}`);
      });
    });
    console.log('Constructed data:', data);
    return data;
  } catch (err) {
    console.error('Error in getCurrentData:', err);
    throw err;
  } finally {
    client.release();
  }
}

async function getHourlyRates(date = currentDate) {
  const client = await pool.connect();
  try {
    console.log(`Fetching hourly rates for date: ${date}`);
    const result = await client.query(
      `SELECT facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC') as hour, SUM(delta) as rate
       FROM ProductionEvents
       WHERE date = $1
       GROUP BY facility, line, EXTRACT(HOUR FROM timestamp AT TIME ZONE 'UTC')
       ORDER BY facility, line, hour`,
      [date]
    );
    console.log('Hourly rates query result (raw rows):', result.rows);

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
          console.log(`Hourly rate for ${f}, ${l}, UTC hour ${hour}: ${rate}`);
        });
      });
    });
    console.log('Constructed hourlyRates (UTC):', hourlyRates);

    return hourlyRates;
  } catch (err) {
    console.error('Error in getHourlyRates:', err);
    throw err;
  } finally {
    client.release();
  }
}

async function getTotalDailyProduction() {
  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT SUM(count) as total FROM ProductionCounts WHERE Date = $1',
      [currentDate]
    );
    const total = result.rows[0]?.total || 0;
    console.log(`Total daily production: ${total}`);
    return total;
  } catch (err) {
    console.error('GetTotalDailyProduction Error:', err);
    throw err;
  } finally {
    client.release();
  }
}

async function getPeakProduction() {
  const client = await pool.connect();
  try {
    // Calculate peak daily production (highest single-day total for each facility)
    const peakDayResult = await client.query(
      `SELECT facility, date, SUM(count) as daily_total
       FROM ProductionCounts
       GROUP BY facility, date
       ORDER BY facility, daily_total DESC`
    );

    // Calculate peak production for the last 7 days
    const weekStart = new Date(currentDate);
    weekStart.setDate(weekStart.getDate() - 7);
    const weekStartDate = weekStart.toISOString().split('T')[0];

    const weeklyResult = await client.query(
      `SELECT facility, MAX(daily_total) as peak_weekly
       FROM (
         SELECT facility, SUM(count) as daily_total
         FROM ProductionCounts
         WHERE Date >= $1 AND Date <= $2
         GROUP BY facility, date
       ) as daily_totals
       GROUP BY facility`,
      [weekStartDate, currentDate]
    );

    const peakProduction = {};
    facilities.forEach(f => {
      // Find the highest daily total for this facility
      const facilityRows = peakDayResult.rows.filter(row => row.facility === f);
      const peakDay = facilityRows.length > 0 ? Math.max(...facilityRows.map(row => parseInt(row.daily_total))) : 0;

      const weeklyRow = weeklyResult.rows.find(row => row.facility === f);
      peakProduction[f] = {
        peakDay: peakDay,
        peakWeekly: weeklyRow ? parseInt(weeklyRow.peak_weekly) : 0
      };
    });

    console.log('Peak production:', peakProduction);
    return peakProduction;
  } catch (err) {
    console.error('GetPeakProduction Error:', err);
    throw err;
  } finally {
    client.release();
  }
}

async function broadcastUpdate() {
  const data = await getCurrentData();
  const hourlyRates = await getHourlyRates();
  const totalProduction = await getTotalDailyProduction();
  const peakProduction = await getPeakProduction();
  
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
    notification
  };
  console.log('Broadcasting update:', JSON.stringify(message));
  wss.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(message));
    }
  });
}
