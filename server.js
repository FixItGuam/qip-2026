const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const multer = require('multer');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const db = require('./db');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.json());
app.use(express.static('public'));
app.use('/uploads', express.static(path.join(__dirname, 'uploads'), {
  fallthrough: false,
  maxAge: '1h',
  setHeaders(res){
    res.setHeader('X-Content-Type-Options', 'nosniff');
  }
}));


const CHAT_UPLOAD_ROOT = path.join(__dirname, 'uploads', '.chat-upload-parts');
const CHAT_UPLOAD_FINAL_ROOT = path.join(__dirname, 'uploads', 'chat-files');
const CHAT_UPLOAD_MAX_SIZE = 100 * 1024 * 1024;
const CHAT_UPLOAD_MAX_CHUNK_SIZE = 1024 * 1024;
const CHAT_UPLOAD_TTL_MS = 24 * 60 * 60 * 1000;

fs.mkdirSync(CHAT_UPLOAD_ROOT, { recursive: true });
fs.mkdirSync(CHAT_UPLOAD_FINAL_ROOT, { recursive: true });

function safeUploadId(value){
  const id = String(value || '');
  if(!/^[a-zA-Z0-9-]{12,128}$/.test(id)) return null;
  return id;
}

function sanitizeUploadFilename(name){
  const base = path.basename(String(name || 'file.bin'))
    .replace(/[\u0000-\u001f<>:"/\\|?*]+/g, '_')
    .trim();
  return (base || 'file.bin').slice(0, 180);
}

function uploadPartDir(uploadId){
  return path.join(CHAT_UPLOAD_ROOT, uploadId);
}

function uploadMetadataPath(uploadId){
  return path.join(uploadPartDir(uploadId), 'meta.json');
}

function readUploadMetadata(uploadId){
  const metaPath = uploadMetadataPath(uploadId);
  if(!fs.existsSync(metaPath)) return null;
  return JSON.parse(fs.readFileSync(metaPath, 'utf8'));
}

function writeUploadMetadata(uploadId, meta){
  fs.writeFileSync(
    uploadMetadataPath(uploadId),
    JSON.stringify(meta, null, 2)
  );
}

function getNextChunkIndex(uploadId, totalChunks){
  const dir = uploadPartDir(uploadId);
  let index = 0;
  while(index < totalChunks && fs.existsSync(path.join(dir, `${index}.part`))){
    index += 1;
  }
  return index;
}

function receivedUploadBytes(uploadId, totalChunks){
  const dir = uploadPartDir(uploadId);
  let total = 0;
  for(let index = 0; index < totalChunks; index += 1){
    const part = path.join(dir, `${index}.part`);
    if(!fs.existsSync(part)) break;
    total += fs.statSync(part).size;
  }
  return total;
}

function removeUploadParts(uploadId){
  fs.rmSync(uploadPartDir(uploadId), { recursive: true, force: true });
}

app.post('/api/chat-upload/init', express.json({ limit: '32kb' }), (req, res) => {
  try{
    const uploadId = safeUploadId(req.body?.uploadId);
    const name = sanitizeUploadFilename(req.body?.name);
    const size = Number(req.body?.size);
    const chunkSize = Number(req.body?.chunkSize);
    const totalChunks = Number(req.body?.totalChunks);
    const type = String(req.body?.type || 'application/octet-stream').slice(0, 120);
    const sha256 = String(req.body?.sha256 || '').toLowerCase();
    const metadata = req.body?.metadata && typeof req.body.metadata === 'object'
      ? req.body.metadata
      : {};

    if(!uploadId) return res.status(400).json({ error: 'Некорректный uploadId' });
    if(!Number.isInteger(size) || size < 0 || size > CHAT_UPLOAD_MAX_SIZE){
      return res.status(400).json({ error: 'Некорректный размер файла' });
    }
    if(!Number.isInteger(chunkSize) || chunkSize < 64 * 1024 || chunkSize > CHAT_UPLOAD_MAX_CHUNK_SIZE){
      return res.status(400).json({ error: 'Некорректный размер чанка' });
    }
    if(sha256 && !/^[a-f0-9]{64}$/.test(sha256)){
      return res.status(400).json({ error: 'Некорректный SHA-256' });
    }
    if(!Number.isInteger(totalChunks) || totalChunks < 1 || totalChunks > 4096){
      return res.status(400).json({ error: 'Некорректное число чанков' });
    }
    if(Math.ceil(size / chunkSize) !== totalChunks && !(size === 0 && totalChunks === 1)){
      return res.status(400).json({ error: 'Размер файла не совпадает с числом чанков' });
    }

    const dir = uploadPartDir(uploadId);
    fs.mkdirSync(dir, { recursive: true });

    const existing = readUploadMetadata(uploadId);
    if(existing){
      if(existing.size !== size || existing.chunkSize !== chunkSize || existing.totalChunks !== totalChunks){
        return res.status(409).json({ error: 'Параметры возобновляемой загрузки не совпадают' });
      }
    } else {
      writeUploadMetadata(uploadId, {
        uploadId,
        name,
        size,
        type,
        chunkSize,
        totalChunks,
        sha256: sha256 || null,
        metadata,
        createdAt: Date.now(),
        updatedAt: Date.now()
      });
    }

    const nextChunkIndex = getNextChunkIndex(uploadId, totalChunks);
    res.json({
      ok: true,
      uploadId,
      nextChunkIndex,
      receivedBytes: receivedUploadBytes(uploadId, totalChunks)
    });
  } catch(error){
    console.error('chat-upload init:', error);
    res.status(500).json({ error: 'Не удалось начать загрузку' });
  }
});

app.post(
  '/api/chat-upload/chunk',
  express.raw({ type: 'application/octet-stream', limit: `${CHAT_UPLOAD_MAX_CHUNK_SIZE}b` }),
  (req, res) => {
    try{
      const uploadId = safeUploadId(req.query.uploadId);
      const index = Number(req.query.index);

      if(!uploadId) return res.status(400).json({ error: 'Некорректный uploadId' });
      const meta = readUploadMetadata(uploadId);
      if(!meta) return res.status(404).json({ error: 'Сессия загрузки не найдена' });
      if(!Number.isInteger(index) || index < 0 || index >= meta.totalChunks){
        return res.status(400).json({ error: 'Некорректный индекс чанка' });
      }

      const expectedIndex = getNextChunkIndex(uploadId, meta.totalChunks);
      if(index < expectedIndex){
        return res.json({
          ok: true,
          duplicate: true,
          nextChunkIndex: expectedIndex,
          receivedBytes: receivedUploadBytes(uploadId, meta.totalChunks)
        });
      }
      if(index !== expectedIndex){
        return res.status(409).json({
          error: 'Нарушен порядок чанков',
          expectedIndex
        });
      }

      const body = Buffer.isBuffer(req.body) ? req.body : Buffer.from(req.body || []);
      const expectedSize = index === meta.totalChunks - 1
        ? meta.size - (index * meta.chunkSize)
        : meta.chunkSize;

      if(body.length !== expectedSize){
        return res.status(400).json({
          error: `Неверный размер чанка: ${body.length}, ожидалось ${expectedSize}`
        });
      }

      const dir = uploadPartDir(uploadId);
      const tempPath = path.join(dir, `${index}.part.tmp`);
      const finalPath = path.join(dir, `${index}.part`);
      fs.writeFileSync(tempPath, body);
      fs.renameSync(tempPath, finalPath);

      meta.updatedAt = Date.now();
      writeUploadMetadata(uploadId, meta);

      res.json({
        ok: true,
        nextChunkIndex: index + 1,
        receivedBytes: Math.min(meta.size, ((index + 1) * meta.chunkSize))
      });
    } catch(error){
      console.error('chat-upload chunk:', error);
      res.status(500).json({ error: 'Не удалось сохранить чанк' });
    }
  }
);


app.get('/api/chat-files/download/:storedName', (req, res) => {
  const storedName = path.basename(String(req.params.storedName || ''));
  if(!/^[a-zA-Z0-9._-]+$/.test(storedName)){
    return res.status(400).send('Invalid filename');
  }

  const filePath = path.join(CHAT_UPLOAD_FINAL_ROOT, storedName);
  if(!fs.existsSync(filePath)){
    return res.status(404).send('File not found');
  }

  const requestedName = sanitizeUploadFilename(req.query.name || storedName);
  res.download(filePath, requestedName);
});

app.post('/api/chat-upload/complete', express.json({ limit: '8kb' }), async (req, res) => {
  try{
    const uploadId = safeUploadId(req.body?.uploadId);
    if(!uploadId) return res.status(400).json({ error: 'Некорректный uploadId' });

    const meta = readUploadMetadata(uploadId);
    if(!meta) return res.status(404).json({ error: 'Сессия загрузки не найдена' });

    const nextChunkIndex = getNextChunkIndex(uploadId, meta.totalChunks);
    if(nextChunkIndex !== meta.totalChunks){
      return res.status(409).json({
        error: 'Загрузка ещё не завершена',
        nextChunkIndex
      });
    }

    const extension = path.extname(meta.name).slice(0, 16);
    const finalName = `${Date.now()}-${crypto.randomBytes(8).toString('hex')}${extension}`;
    const finalPath = path.join(CHAT_UPLOAD_FINAL_ROOT, finalName);
    const output = fs.createWriteStream(finalPath, { flags: 'wx' });

    for(let index = 0; index < meta.totalChunks; index += 1){
      const partPath = path.join(uploadPartDir(uploadId), `${index}.part`);
      await new Promise((resolve, reject) => {
        const input = fs.createReadStream(partPath);
        input.on('error', reject);
        input.on('end', resolve);
        input.pipe(output, { end: false });
      });
    }

    await new Promise((resolve, reject) => {
      output.end(resolve);
      output.on('error', reject);
    });

    const finalSize = fs.statSync(finalPath).size;
    if(finalSize !== meta.size){
      fs.rmSync(finalPath, { force: true });
      return res.status(500).json({ error: 'Размер собранного файла не совпадает' });
    }

    let finalSha256 = null;
    if(meta.sha256){
      finalSha256 = await new Promise((resolve, reject) => {
        const hash = crypto.createHash('sha256');
        const input = fs.createReadStream(finalPath);
        input.on('error', reject);
        input.on('data', chunk => hash.update(chunk));
        input.on('end', () => resolve(hash.digest('hex')));
      });

      if(finalSha256 !== meta.sha256){
        fs.rmSync(finalPath, { force: true });
        return res.status(409).json({
          error: 'SHA-256 не совпадает. Файл повреждён при передаче.'
        });
      }
    }

    removeUploadParts(uploadId);

    res.json({
      ok: true,
      path: `/uploads/chat-files/${finalName}`,
      previewPath: `/uploads/chat-files/${finalName}`,
      downloadPath: `/api/chat-files/download/${encodeURIComponent(finalName)}?name=${encodeURIComponent(meta.name)}`,
      name: meta.name,
      size: meta.size,
      type: meta.type,
      sha256: finalSha256 || meta.sha256 || null,
      metadata: meta.metadata || {}
    });
  } catch(error){
    console.error('chat-upload complete:', error);
    res.status(500).json({ error: 'Не удалось собрать файл' });
  }
});

app.post('/api/chat-upload/abort', express.json({ limit: '8kb' }), (req, res) => {
  const uploadId = safeUploadId(req.body?.uploadId);
  if(!uploadId) return res.status(400).json({ error: 'Некорректный uploadId' });
  removeUploadParts(uploadId);
  res.json({ ok: true });
});

setInterval(() => {
  try{
    const now = Date.now();
    for(const entry of fs.readdirSync(CHAT_UPLOAD_ROOT, { withFileTypes: true })){
      if(!entry.isDirectory()) continue;
      const id = entry.name;
      const meta = readUploadMetadata(id);
      const updatedAt = Number(meta?.updatedAt || meta?.createdAt || 0);
      if(!updatedAt || now - updatedAt > CHAT_UPLOAD_TTL_MS){
        removeUploadParts(id);
      }
    }
  } catch(error){
    console.warn('chat-upload cleanup:', error.message);
  }
}, 60 * 60 * 1000).unref();

app.get('/api/health', (req, res) => res.json({ ok: true, build: '35.5.1', mediaPolish: true, mp3Artwork: true, contextualToolbar: true, xstatusMarquee: true, xstatusPhrases: true, presenceFix: true, countryFlags: true, compactChatHeader: true, cleanCountryHeader: true, inlineXstatusHeader: true, flatCountryFlag: true }));

// --- Загрузка своих аватарок ---
const AVATAR_UPLOAD_DIR = path.join(__dirname, 'public', 'uploads', 'avatars');
fs.mkdirSync(AVATAR_UPLOAD_DIR, { recursive: true });

const avatarStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, AVATAR_UPLOAD_DIR),
  filename: (req, file, cb) => {
    const ext = (path.extname(file.originalname) || '.jpg').toLowerCase();
    const rand = Date.now() + '-' + Math.round(Math.random() * 1e9);
    cb(null, `avatar-${rand}${ext}`);
  }
});
const uploadAvatar = multer({
  storage: avatarStorage,
  limits: { fileSize: 3 * 1024 * 1024 }, // 3MB
  fileFilter: (req, file, cb) => {
    if (!/^image\/(png|jpe?g|webp|gif)$/.test(file.mimetype)) {
      return cb(new Error('Разрешены только изображения (jpg, png, webp, gif)'));
    }
    cb(null, true);
  }
});

// --- Загрузка файлов, отправляемых в чате ---
const CHAT_FILE_DIR = path.join(__dirname, 'public', 'uploads', 'files');
fs.mkdirSync(CHAT_FILE_DIR, { recursive: true });

const chatFileStorage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, CHAT_FILE_DIR),
  filename: (req, file, cb) => {
    const ext = path.extname(file.originalname) || '';
    const rand = Date.now() + '-' + Math.round(Math.random() * 1e9);
    cb(null, `file-${rand}${ext}`);
  }
});
const uploadChatFile = multer({
  storage: chatFileStorage,
  limits: { fileSize: 15 * 1024 * 1024 } // 15MB
});

// nickname -> socket.id (только для тех, кто сейчас онлайн)
const online = new Map();
// nickname -> 'online' | 'away' | 'busy' | 'invisible'
const userStatus = new Map();
// nickname -> номер xStatus (1-32) или null
const userXStatus = new Map();
const userCountry = new Map();
const countryLookupCache = new Map();

function normalizeCountryCode(value) {
  const code = String(value || '').trim().toUpperCase();
  return /^[A-Z]{2}$/.test(code) && code !== 'XX' ? code : null;
}

function clientIpFromSocket(socket) {
  const forwarded = socket.handshake.headers['x-forwarded-for'];
  let ip = Array.isArray(forwarded) ? forwarded[0] : String(forwarded || '').split(',')[0];
  ip = (ip || socket.handshake.address || '').trim().replace(/^::ffff:/, '');
  return ip;
}

function countryFromProxyHeaders(socket) {
  const headers = socket.handshake.headers || {};
  const candidates = [
    headers['cf-ipcountry'],
    headers['x-vercel-ip-country'],
    headers['cloudfront-viewer-country'],
    headers['x-appengine-country'],
    headers['x-country-code']
  ];
  for (const value of candidates) {
    const code = normalizeCountryCode(value);
    if (code) return { code, country: null };
  }
  return null;
}

function isPublicIp(ip) {
  if (!ip) return false;
  if (ip === '::1' || ip === '127.0.0.1') return false;
  if (/^10\./.test(ip) || /^192\.168\./.test(ip) || /^169\.254\./.test(ip)) return false;
  const match = ip.match(/^172\.(\d+)\./);
  if (match && Number(match[1]) >= 16 && Number(match[1]) <= 31) return false;
  return true;
}

async function resolveSocketCountry(socket) {
  const headerCountry = countryFromProxyHeaders(socket);
  if (headerCountry) return headerCountry;
  const ip = clientIpFromSocket(socket);
  if (!isPublicIp(ip) || typeof fetch !== 'function') return null;
  if (countryLookupCache.has(ip)) return countryLookupCache.get(ip);
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 2500);
  try {
    const response = await fetch(`https://ipwho.is/${encodeURIComponent(ip)}?fields=success,country,country_code`, { signal: controller.signal });
    if (!response.ok) return null;
    const data = await response.json();
    const code = normalizeCountryCode(data.country_code);
    const result = data.success !== false && code ? { code, country: String(data.country || '').slice(0, 80) || null } : null;
    countryLookupCache.set(ip, result);
    if (countryLookupCache.size > 1000) countryLookupCache.delete(countryLookupCache.keys().next().value);
    return result;
  } catch (_error) {
    return null;
  } finally {
    clearTimeout(timeout);
  }
}


const DEFAULT_QIP_AVATAR = 'toolbar-icons/qip-logo-mascot.png';

function cleanNickname(value) {
  return String(value || '')
    .replace(/[\u0000-\u001f\u007f]/g, '')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 24);
}

function nicknameError(nickname) {
  if (nickname.length < 2) return 'Ник должен быть от 2 до 24 символов';
  if (!/^[\p{L}\p{N} _.-]+$/u.test(nickname)) {
    return 'В нике разрешены буквы, цифры, пробел, точка, дефис и подчёркивание';
  }
  return null;
}

function nicknameExists(nickname, exceptUserId = null) {
  if (exceptUserId) {
    return Boolean(db.prepare(
      'SELECT 1 FROM users WHERE lower(nickname) = lower(?) AND id != ?'
    ).get(nickname, exceptUserId));
  }
  return Boolean(db.prepare(
    'SELECT 1 FROM users WHERE lower(nickname) = lower(?)'
  ).get(nickname));
}


function validatePassword(value) {
  const password = String(value || '');
  if (password.length < 6) return 'Пароль должен содержать минимум 6 символов';
  if (password.length > 128) return 'Пароль слишком длинный';
  return null;
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(password, salt, 64).toString('hex');
  return `scrypt$${salt}$${hash}`;
}

function verifyPassword(password, storedHash) {
  if (!storedHash || !storedHash.startsWith('scrypt$')) return false;
  const [, salt, expectedHex] = storedHash.split('$');
  if (!salt || !expectedHex) return false;

  const actual = crypto.scryptSync(password, salt, 64);
  const expected = Buffer.from(expectedHex, 'hex');
  return actual.length === expected.length && crypto.timingSafeEqual(actual, expected);
}

function createUniqueQipNumber() {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const raw = crypto.randomInt(0, 1_000_000_000).toString().padStart(9, '0');
    const formatted = `${raw.slice(0, 3)}-${raw.slice(3, 6)}-${raw.slice(6, 9)}`;
    if (!db.prepare('SELECT 1 FROM users WHERE qip_number = ?').get(formatted)) {
      return formatted;
    }
  }
  throw new Error('Не удалось создать уникальный QIP-номер');
}

function ensureLegacyQipNumbers() {
  const rows = db.prepare(
    "SELECT id FROM users WHERE qip_number IS NULL OR qip_number = ''"
  ).all();
  const update = db.prepare('UPDATE users SET qip_number = ? WHERE id = ?');
  const transaction = db.transaction(() => {
    rows.forEach(row => update.run(createUniqueQipNumber(), row.id));
  });
  transaction();
}

ensureLegacyQipNumbers();

// --- REST: список контактов ---
app.get('/api/users', (req, res) => {
  const users = db.prepare('SELECT id, nickname, avatar_emoji, qip_number, about, last_seen FROM users').all();
  const result = users.map(u => {
    const isConnected = online.has(u.nickname);
    const rawStatus = isConnected ? (userStatus.get(u.nickname) || 'online') : 'offline';
    // invisible: для всех остальных выглядит как offline
    const publicStatus = rawStatus === 'invisible' ? 'offline' : rawStatus;
    return {
      ...u,
      online: isConnected && publicStatus !== 'offline',
      status: publicStatus,
      xstatus: userXStatus.get(u.nickname) || null,
      country_code: userCountry.get(u.nickname)?.code || null,
      country: userCountry.get(u.nickname)?.country || null
    };
  });
  res.json(result);
});

// --- REST: регистрация только по уникальному нику ---
app.post('/api/register', (req, res) => {
  const nickname = cleanNickname(req.body.nickname);
  console.log('[register]', nickname || '(empty)');
  const password = String(req.body.password || '');
  const validationError = nicknameError(nickname);
  const passwordValidationError = validatePassword(password);
  if (validationError) {
    return res.status(400).json({ error: validationError });
  }
  if (passwordValidationError) {
    return res.status(400).json({ error: passwordValidationError });
  }
  if (nicknameExists(nickname)) {
    return res.status(409).json({ error: 'Этот ник уже занят' });
  }

  try {
    const qipNumber = createUniqueQipNumber();
    const passwordHash = hashPassword(password);
    const info = db.prepare(`
      INSERT INTO users (nickname, avatar_emoji, qip_number, about, password_hash, created_at)
      VALUES (?, ?, ?, '', ?, ?)
    `).run(nickname, DEFAULT_QIP_AVATAR, qipNumber, passwordHash, Date.now());

    const user = db.prepare(`
      SELECT id, nickname, avatar_emoji, qip_number, about, created_at, last_seen
      FROM users WHERE id = ?
    `).get(info.lastInsertRowid);
    res.status(201).json(user);
  } catch (error) {
    if (String(error.message).includes('UNIQUE')) {
      return res.status(409).json({ error: 'Этот ник уже занят' });
    }
    console.error(error);
    res.status(500).json({ error: 'Не удалось зарегистрировать пользователя' });
  }
});

// Вход по нику и паролю.
app.post('/api/login', (req, res) => {
  const nickname = cleanNickname(req.body.nickname);
  console.log('[login]', nickname || '(empty)');
  const password = String(req.body.password || '');

  const user = db.prepare(
    'SELECT * FROM users WHERE lower(nickname) = lower(?)'
  ).get(nickname);

  if (!user) {
    return res.status(401).json({ error: 'Неверный ник или пароль' });
  }

  // Старые аккаунты, созданные до паролей, могут задать пароль в профиле
  // из уже сохранённой локальной сессии.
  if (!user.password_hash) {
    return res.status(403).json({
      error: 'Для этого старого аккаунта пароль ещё не установлен. Открой его в прежнем браузере и задай пароль в профиле.'
    });
  }

  if (!verifyPassword(password, user.password_hash)) {
    return res.status(401).json({ error: 'Неверный ник или пароль' });
  }

  const safeUser = db.prepare(`
    SELECT id, nickname, avatar_emoji, qip_number, about, created_at, last_seen
    FROM users WHERE id = ?
  `).get(user.id);

  res.json(safeUser);
});

// Возврат локальной сессии после перезагрузки браузера.
app.get('/api/session/:userId', (req, res) => {
  const user = db.prepare(`
    SELECT id, nickname, avatar_emoji, qip_number, about, created_at, last_seen
    FROM users WHERE id = ?
  `).get(req.params.userId);
  if (!user) return res.status(404).json({ error: 'Пользователь не найден' });
  res.json(user);
});

// Изменение ника и информации о себе.
app.post('/api/profile', (req, res) => {
  const userId = Number(req.body.userId);
  const nickname = cleanNickname(req.body.nickname);
  const about = String(req.body.about || '').trim().slice(0, 500);
  const newPassword = String(req.body.password || '');

  if (!Number.isInteger(userId) || userId < 1) {
    return res.status(400).json({ error: 'Некорректный пользователь' });
  }

  const oldUser = db.prepare('SELECT * FROM users WHERE id = ?').get(userId);
  if (!oldUser) return res.status(404).json({ error: 'Пользователь не найден' });

  const validationError = nicknameError(nickname);
  if (validationError) return res.status(400).json({ error: validationError });
  if (nicknameExists(nickname, userId)) {
    return res.status(409).json({ error: 'Этот ник уже занят' });
  }

  if (newPassword) {
    const passwordValidationError = validatePassword(newPassword);
    if (passwordValidationError) {
      return res.status(400).json({ error: passwordValidationError });
    }
    db.prepare(
      'UPDATE users SET nickname = ?, about = ?, password_hash = ? WHERE id = ?'
    ).run(nickname, about, hashPassword(newPassword), userId);
  } else {
    db.prepare('UPDATE users SET nickname = ?, about = ? WHERE id = ?')
      .run(nickname, about, userId);
  }

  if (oldUser.nickname !== nickname) {
    if (online.has(oldUser.nickname)) {
      const socketId = online.get(oldUser.nickname);
      online.delete(oldUser.nickname);
      online.set(nickname, socketId);
    }
    if (userStatus.has(oldUser.nickname)) {
      const status = userStatus.get(oldUser.nickname);
      userStatus.delete(oldUser.nickname);
      userStatus.set(nickname, status);
    }
    if (userXStatus.has(oldUser.nickname)) {
      const xstatus = userXStatus.get(oldUser.nickname);
      userXStatus.delete(oldUser.nickname);
      userXStatus.set(nickname, normalizedXStatus);
    }
  }

  const user = db.prepare(`
    SELECT id, nickname, avatar_emoji, qip_number, about, created_at, last_seen
    FROM users WHERE id = ?
  `).get(userId);
  io.emit('profile_updated', user);
  io.emit('presence');
  res.json(user);
});

// --- REST: смена аватарки пользователя (из готового пака) ---
app.post('/api/set-avatar', (req, res) => {
  const { userId, avatar } = req.body;
  if (!userId || !avatar || !/^kolobki\/KolobkiQIP2005_\d{3}\.webp$/.test(avatar)) {
    return res.status(400).json({ error: 'Некорректная аватарка' });
  }
  db.prepare('UPDATE users SET avatar_emoji = ? WHERE id = ?').run(avatar, userId);
  io.emit('presence');
  res.json({ ok: true, avatar });
});

// --- REST: загрузка своей картинки как аватарки ---
app.post('/api/upload-avatar', (req, res) => {
  uploadAvatar.single('avatar')(req, res, (err) => {
    if (err) {
      return res.status(400).json({ error: err.message || 'Не удалось загрузить файл' });
    }
    const { userId } = req.body;
    if (!userId || !req.file) {
      return res.status(400).json({ error: 'Файл не получен' });
    }
    const avatarPath = `uploads/avatars/${req.file.filename}`;
    db.prepare('UPDATE users SET avatar_emoji = ? WHERE id = ?').run(avatarPath, userId);
    io.emit('presence');
    res.json({ ok: true, avatar: avatarPath });
  });
});

// --- REST: загрузка файла для отправки в чате ---
app.post('/api/upload-chat-file', (req, res) => {
  uploadChatFile.single('file')(req, res, (err) => {
    if (err) {
      return res.status(400).json({ error: err.message || 'Не удалось загрузить файл' });
    }
    if (!req.file) {
      return res.status(400).json({ error: 'Файл не получен' });
    }
    res.json({
      ok: true,
      path: `uploads/files/${req.file.filename}`,
      name: req.file.originalname,
      size: req.file.size
    });
  });
});

// --- REST: история сообщений между двумя пользователями ---
app.get('/api/history/:userId/:peerId', (req, res) => {
  const { userId, peerId } = req.params;
  const rows = db.prepare(`
    SELECT * FROM messages
    WHERE (from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?)
    ORDER BY created_at ASC
    LIMIT 200
  `).all(userId, peerId, peerId, userId);
  res.json(rows);
});

// --- Socket.io: realtime ---
io.on('connection', (socket) => {
  let currentNickname = null;
  let currentUserId = null;

  socket.on('identify', ({ nickname, userId }) => {
    if (currentNickname && currentNickname !== nickname) {
      online.delete(currentNickname);
      userStatus.delete(currentNickname);
      userXStatus.delete(currentNickname);
    }
    currentNickname = nickname;
    currentUserId = userId;
    online.set(nickname, socket.id);
    userStatus.set(nickname, 'online');
    db.prepare('UPDATE users SET last_seen = ? WHERE id = ?').run(Date.now(), userId);
    const headerCountry = countryFromProxyHeaders(socket);
    if (headerCountry) userCountry.set(nickname, headerCountry);
    io.emit('presence', { nickname, online: true });
    resolveSocketCountry(socket).then(country => {
      if (!country || currentNickname !== nickname) return;
      userCountry.set(nickname, country);
      io.emit('presence', { nickname, online: true, country_code: country.code });
    });
  });

  socket.on('set_status', ({ nickname, status }) => {
    if (['online', 'away', 'busy', 'invisible', 'ffc', 'na', 'occupied', 'lunch'].includes(status)) {
      userStatus.set(nickname, status);
      io.emit('presence', { nickname, status });
    }
  });

  socket.on('set_xstatus', ({ nickname, xstatus }) => {
    const VALID_XSTATUS = new Set([
      'music','coffee','beer','eating','sleeping','tired','sick','angry','funny','thinking',
      'typing','phone','tv','games','surfing','business','engineering','college','party',
      'friends','shopping','camera','duck','at','mode','china1','china4','china5','de1','de2','de3'
    ]);
    const normalizeCaption = (value) => String(value || '').replace(/\s+/g, ' ').trim().slice(0, 120);
    const isCustom = typeof xstatus === 'string' && xstatus.startsWith('custom:') && normalizeCaption(xstatus.slice(7)).length > 0;
    let isCombo = false;
    let normalizedXStatus = xstatus;
    if (typeof xstatus === 'string' && xstatus.includes('|')) {
      const separator = xstatus.indexOf('|');
      const iconPart = xstatus.slice(0, separator);
      const captionPart = normalizeCaption(xstatus.slice(separator + 1));
      isCombo = VALID_XSTATUS.has(iconPart) && captionPart.length > 0;
      if (isCombo) normalizedXStatus = `${iconPart}|${captionPart}`;
    } else if (isCustom) {
      normalizedXStatus = `custom:${normalizeCaption(xstatus.slice(7))}`;
    }
    if (xstatus === null) {
      userXStatus.delete(nickname);
    } else if (typeof xstatus === 'string' && (VALID_XSTATUS.has(xstatus) || isCustom || isCombo)) {
      userXStatus.set(nickname, normalizedXStatus);
    } else {
      return;
    }
    io.emit('presence', { nickname, xstatus: xstatus === null ? null : normalizedXStatus });
  });

  socket.on('typing', ({ toNickname, fromNickname, isTyping }) => {
    const targetSocket = online.get(toNickname);
    if (targetSocket) {
      io.to(targetSocket).emit('typing', { fromNickname, isTyping });
    }
  });

  socket.on('message', ({ toId, toNickname, fromId, fromNickname, body }) => {
    const now = Date.now();
    const info = db.prepare(
      'INSERT INTO messages (from_id, to_id, body, created_at) VALUES (?, ?, ?, ?)'
    ).run(fromId, toId, body, now);

    const payload = {
      id: info.lastInsertRowid,
      from_id: fromId,
      from_nickname: fromNickname,
      to_id: toId,
      body,
      created_at: now
    };

    // отправляем получателю, если онлайн
    const targetSocket = online.get(toNickname);
    if (targetSocket) {
      io.to(targetSocket).emit('message', payload);
    }
    // эхо отправителю (для мультивкладок/подтверждения)
    socket.emit('message:sent', payload);
  });

  socket.on('disconnect', () => {
    if (currentNickname) {
      online.delete(currentNickname);
      userStatus.delete(currentNickname);
      userXStatus.delete(currentNickname);
      userCountry.delete(currentNickname);
      db.prepare('UPDATE users SET last_seen = ? WHERE id = ?').run(Date.now(), currentUserId);
      io.emit('presence', { nickname: currentNickname, online: false });
    }
  });
});

const PORT = process.env.PORT || 3210;
server.listen(PORT, () => console.log(`QIP-messenger запущен на порту ${PORT}`));
