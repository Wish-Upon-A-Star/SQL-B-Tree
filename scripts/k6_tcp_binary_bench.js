import exec from 'k6/execution';
import { Counter, Rate, Trend } from 'k6/metrics';
import { check } from 'k6';
import tcp from 'k6/x/tcp';

const REQUEST_MAGIC = 0x31505451;
const RESPONSE_MAGIC = 0x31505452;
const PROTOCOL_VERSION = 1;

const OP_PING = 1;
const OP_SQL = 2;
const OP_CLOSE = 3;

const STATUS_OK = 0;

const HOST = __ENV.K6_TCP_HOST || '127.0.0.1';
const PORT = Number(__ENV.K6_TCP_PORT || '9090');
const MODE = __ENV.K6_TCP_MODE || 'ping';
const REQUEST_TIMEOUT_MS = Number(__ENV.K6_TCP_TIMEOUT_MS || '30000');
const SQL_TEXT = __ENV.K6_TCP_SQL || 'SELECT * FROM case_basic_users WHERE id = 2;';
const QUERIES_FILE = __ENV.K6_TCP_SQL_FILE || '';
const LOG_EVERY = Number(__ENV.K6_TCP_LOG_EVERY || '1000');

const QUERIES = loadQueries();

const roundTripMs = new Trend('tcp_binary_round_trip_ms', true);
const connectMs = new Trend('tcp_binary_connect_ms', true);
const responseBytes = new Trend('tcp_binary_response_bytes', true);
const protocolChecks = new Rate('tcp_binary_protocol_checks');
const transportErrors = new Counter('tcp_binary_transport_errors');
const protocolErrors = new Counter('tcp_binary_protocol_errors');

export const options = {
  vus: Number(__ENV.K6_VUS || '16'),
  duration: __ENV.K6_DURATION || '30s',
  thresholds: {
    tcp_binary_protocol_checks: ['rate>0.999'],
    tcp_binary_round_trip_ms: ['p(95)<2000'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'min', 'max'],
};

let conn = null;

function loadQueries() {
  let text;
  if (!QUERIES_FILE) {
    return [
      'SELECT * FROM case_basic_users WHERE id = 2;',
      'SELECT * FROM case_basic_users WHERE id = 3;',
      "SELECT * FROM case_basic_users WHERE email = 'alice@example.com';",
      'SELECT id, email FROM case_basic_users WHERE id BETWEEN 1 AND 3;',
    ];
  }

  text = open(QUERIES_FILE);
  return text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0 && !line.startsWith('#'));
}

function toAsciiBytes(text) {
  const bytes = new Array(text.length);
  for (let i = 0; i < text.length; i += 1) {
    bytes[i] = text.charCodeAt(i) & 0xff;
  }
  return bytes;
}

function fromAsciiBytes(bytes) {
  let out = '';
  for (let i = 0; i < bytes.length; i += 1) {
    out += String.fromCharCode(bytes[i]);
  }
  return out;
}

function writeU16LE(buffer, offset, value) {
  buffer[offset] = value & 0xff;
  buffer[offset + 1] = (value >>> 8) & 0xff;
}

function writeU32LE(buffer, offset, value) {
  buffer[offset] = value & 0xff;
  buffer[offset + 1] = (value >>> 8) & 0xff;
  buffer[offset + 2] = (value >>> 16) & 0xff;
  buffer[offset + 3] = (value >>> 24) & 0xff;
}

function readU16LE(buffer, offset) {
  return (buffer[offset] | (buffer[offset + 1] << 8)) >>> 0;
}

function readU32LE(buffer, offset) {
  return (
    buffer[offset] |
    (buffer[offset + 1] << 8) |
    (buffer[offset + 2] << 16) |
    (buffer[offset + 3] << 24)
  ) >>> 0;
}

function buildRequestFrame(op, requestId, payloadText) {
  const requestIdBytes = toAsciiBytes(requestId);
  const payloadBytes = payloadText ? toAsciiBytes(payloadText) : [];
  const headerSize = 20;
  const totalSize = headerSize + requestIdBytes.length + payloadBytes.length;
  const frame = new Array(totalSize).fill(0);
  let cursor = headerSize;

  writeU32LE(frame, 0, REQUEST_MAGIC);
  writeU16LE(frame, 4, PROTOCOL_VERSION);
  writeU16LE(frame, 6, op);
  writeU32LE(frame, 8, 0);
  writeU16LE(frame, 12, requestIdBytes.length);
  writeU16LE(frame, 14, 0);
  writeU32LE(frame, 16, payloadBytes.length);

  for (let i = 0; i < requestIdBytes.length; i += 1) {
    frame[cursor + i] = requestIdBytes[i];
  }
  cursor += requestIdBytes.length;
  for (let i = 0; i < payloadBytes.length; i += 1) {
    frame[cursor + i] = payloadBytes[i];
  }
  return frame;
}

function readExactBytes(connection, size) {
  const chunks = [];
  let remaining = size;

  while (remaining > 0) {
    const chunk = tcp.read(connection, remaining, REQUEST_TIMEOUT_MS);
    if (!chunk || chunk.length === 0) {
      return null;
    }
    chunks.push(chunk);
    remaining -= chunk.length;
  }

  if (chunks.length === 1) {
    return chunks[0];
  }

  const merged = new Array(size);
  let offset = 0;
  for (let i = 0; i < chunks.length; i += 1) {
    const chunk = chunks[i];
    for (let j = 0; j < chunk.length; j += 1) {
      merged[offset + j] = chunk[j];
    }
    offset += chunk.length;
  }
  return merged;
}

function readResponseFrame(connection) {
  const headerBytes = readExactBytes(connection, 32);
  let bodyBytes;
  let errorBytes;
  let requestIdBytes;
  let cursor = 0;
  let requestIdLen;
  let bodyLen;
  let errorLen;

  if (!headerBytes) {
    return null;
  }

  if (readU32LE(headerBytes, 0) !== RESPONSE_MAGIC) {
    protocolErrors.add(1);
    return null;
  }
  if (readU16LE(headerBytes, 4) !== PROTOCOL_VERSION) {
    protocolErrors.add(1);
    return null;
  }

  requestIdLen = readU16LE(headerBytes, 12);
  bodyLen = readU32LE(headerBytes, 16);
  errorLen = readU32LE(headerBytes, 20);

  requestIdBytes = readExactBytes(connection, requestIdLen);
  if (requestIdLen > 0 && !requestIdBytes) {
    transportErrors.add(1);
    return null;
  }
  bodyBytes = readExactBytes(connection, bodyLen);
  if (bodyLen > 0 && !bodyBytes) {
    transportErrors.add(1);
    return null;
  }
  errorBytes = readExactBytes(connection, errorLen);
  if (errorLen > 0 && !errorBytes) {
    transportErrors.add(1);
    return null;
  }

  cursor += requestIdLen + bodyLen + errorLen;
  responseBytes.add(32 + cursor);

  return {
    status: readU16LE(headerBytes, 6),
    flags: readU32LE(headerBytes, 8),
    bodyFormat: readU16LE(headerBytes, 14),
    rowCount: readU32LE(headerBytes, 24),
    affectedCount: readU32LE(headerBytes, 28),
    requestId: requestIdBytes ? fromAsciiBytes(requestIdBytes) : '',
    bodyText: bodyBytes ? fromAsciiBytes(bodyBytes) : '',
    errorText: errorBytes ? fromAsciiBytes(errorBytes) : '',
  };
}

function getConnection() {
  const started = Date.now();
  if (conn) {
    return conn;
  }
  conn = tcp.connect(`${HOST}:${PORT}`, REQUEST_TIMEOUT_MS);
  connectMs.add(Date.now() - started);
  return conn;
}

function nextSql(iteration) {
  if (MODE === 'sql') {
    return SQL_TEXT;
  }
  return QUERIES[iteration % QUERIES.length];
}

function runOnce() {
  const iteration = exec.scenario.iterationInTest;
  const requestId = `vu${__VU}-it${iteration}`;
  const sql = nextSql(iteration);
  const op = MODE === 'ping' ? OP_PING : OP_SQL;
  const frame = buildRequestFrame(op, requestId, op === OP_SQL ? sql : '');
  const started = Date.now();
  const connection = getConnection();
  let response;
  let ok;

  try {
    tcp.write(connection, frame, REQUEST_TIMEOUT_MS);
    response = readResponseFrame(connection);
  } catch (error) {
    transportErrors.add(1);
    throw error;
  }

  roundTripMs.add(Date.now() - started, { mode: MODE });

  ok = check(response, {
    'response exists': (value) => value !== null,
    'response id matches': (value) => value && value.requestId === requestId,
    'response status ok': (value) => value && value.status === STATUS_OK,
    'response ok flag set': (value) => value && (value.flags & 1) === 1,
  });

  protocolChecks.add(ok ? 1 : 0);
  if (!ok) {
    protocolErrors.add(1);
  }

  if ((iteration + 1) % LOG_EVERY === 0) {
    console.log(
      `[phase=${MODE}] vus=${__ENV.K6_VUS || '16'} completed=${iteration + 1} ` +
        `rtt_ms=${Date.now() - started} row_count=${response ? response.rowCount : -1}`,
    );
  }
}

export default function () {
  runOnce();
}

export function handleSummary(data) {
  return {
    stdout:
      `\n[k6-tcp] mode=${MODE} host=${HOST}:${PORT}\n` +
      `iterations=${data.metrics.iterations ? data.metrics.iterations.count : 0}\n` +
      `protocol_error_count=${data.metrics.tcp_binary_protocol_errors ? data.metrics.tcp_binary_protocol_errors.count : 0}\n` +
      `transport_error_count=${data.metrics.tcp_binary_transport_errors ? data.metrics.tcp_binary_transport_errors.count : 0}\n`,
  };
}
