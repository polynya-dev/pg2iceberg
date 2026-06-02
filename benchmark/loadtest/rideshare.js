// k6 load test for the pg2iceberg benchmark. Runs LOCALLY on the
// operator's laptop and drives the HTTP shim (public ALB) inside the
// VPC. Stock k6 — no xk6 build required.
//
// The action mix mirrors example/single/simulate.py:
//   new rider 10% | new driver 5% | request 35% | complete 35% | cancel 15%
//
// Env knobs:
//   TARGET          base URL of the shim ALB (required)   e.g. http://my-alb-123.elb.amazonaws.com
//   BENCH_RATE      target requests/sec (default 100)
//   BENCH_DURATION  steady-state duration, the "X minutes" (default 15m)
//   BENCH_VUS       pre-allocated VUs (default 100)
//
// Usage:
//   TARGET=http://<alb> BENCH_DURATION=15m BENCH_RATE=200 k6 run rideshare.js
import http from 'k6/http';
import { check } from 'k6';
import { Counter } from 'k6/metrics';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';

const TARGET = __ENV.TARGET || 'http://localhost:8080';
const RATE = parseInt(__ENV.BENCH_RATE || '100', 10);
const DURATION = __ENV.BENCH_DURATION || '15m';
const VUS = parseInt(__ENV.BENCH_VUS || '100', 10);

export const options = {
  scenarios: {
    rideshare: {
      executor: 'constant-arrival-rate',
      rate: RATE,
      timeUnit: '1s',
      duration: DURATION,
      preAllocatedVUs: VUS,
      maxVUs: VUS * 4,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

const cRider = new Counter('act_new_rider');
const cDriver = new Counter('act_new_driver');
const cRequest = new Counter('act_request_ride');
const cComplete = new Counter('act_complete_ride');
const cCancel = new Counter('act_cancel_ride');

function post(path) {
  const res = http.post(`${TARGET}${path}`, null, { tags: { action: path } });
  check(res, { 'status 200': (r) => r.status === 200 });
  return res;
}

// Weighted action picker — cumulative thresholds match the weights above.
export default function () {
  const r = Math.random();
  if (r < 0.10) { post('/riders'); cRider.add(1); }
  else if (r < 0.15) { post('/drivers'); cDriver.add(1); }
  else if (r < 0.50) { post('/rides/request'); cRequest.add(1); }
  else if (r < 0.85) { post('/rides/complete'); cComplete.add(1); }
  else { post('/rides/cancel'); cCancel.add(1); }
}

export function handleSummary(data) {
  return {
    'out/summary.json': JSON.stringify(data, null, 2),
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
  };
}
