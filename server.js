const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const Redis = require('ioredis');
const path = require('path');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// Redis instances (Dedicated instances required for Pub/Sub subscriber)
const redis = new Redis({ host: '127.0.0.1', port: 6379 });
const redisSub = new Redis({ host: '127.0.0.1', port: 6379 });

redis.on('connect', () => console.log('✅ Connected to Redis Master'));
redis.on('error', (err) => console.error('❌ Redis Error:', err));

// --- FEATURE 1: CACHING ---
app.get('/api/user-profile', async (req, res) => {
  const cacheKey = 'user:profile:1001';
  const start = Date.now();

  try {
    const cachedData = await redis.get(cacheKey);

    if (cachedData) {
      return res.json({
        source: 'CACHE (Redis)',
        latency: `${Date.now() - start}ms`,
        data: JSON.parse(cachedData)
      });
    }

    // Simulate slow DB query
    await new Promise((resolve) => setTimeout(resolve, 1500));
    
    const dbData = { id: 1001, name: 'Sarah Connor', role: 'Security Lead', location: 'Austin, TX' };

    // Cache for 10 seconds
    await redis.setex(cacheKey, 10, JSON.stringify(dbData));

    return res.json({
      source: 'DATABASE (Simulated 1.5s delay)',
      latency: `${Date.now() - start}ms`,
      data: dbData
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// --- FEATURE 2: RATE LIMITING ---
const RATE_LIMIT_WINDOW = 10; // seconds
const MAX_REQUESTS = 5;

const rateLimiter = async (req, res, next) => {
  const ip = req.ip || '127.0.0.1';
  const key = `ratelimit:${ip}`;

  const current = await redis.incr(key);

  if (current === 1) {
    await redis.expire(key, RATE_LIMIT_WINDOW);
  }

  const ttl = await redis.ttl(key);

  if (current > MAX_REQUESTS) {
    return res.status(429).json({
      error: 'Too Many Requests',
      limit: MAX_REQUESTS,
      current,
      ttl: ttl > 0 ? ttl : 0
    });
  }

  req.rateLimit = { limit: MAX_REQUESTS, current, ttl };
  next();
};

app.get('/api/rate-limited-action', rateLimiter, (req, res) => {
  res.json({
    message: 'Action successful!',
    rateLimit: req.rateLimit
  });
});

// --- FEATURE 3: PUB/SUB ---
// Subscribe to Redis Channel
redisSub.subscribe('live-notifications', (err) => {
  if (err) console.error('Failed to subscribe:', err);
});

// Forward Redis messages to WebSocket clients
redisSub.on('message', (channel, message) => {
  if (channel === 'live-notifications') {
    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message);
      }
    });
  }
});

app.post('/api/publish', async (req, res) => {
  const { message } = req.body;
  if (!message) return res.status(400).json({ error: 'Message required' });

  const payload = JSON.stringify({
    text: message,
    timestamp: new Date().toLocaleTimeString()
  });

  await redis.publish('live-notifications', payload);
  res.json({ success: true, published: payload });
});

const PORT = 3000;
server.listen(PORT, () => console.log(`🚀 Server running on http://localhost:${PORT}`));