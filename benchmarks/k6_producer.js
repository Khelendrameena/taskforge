/* k6 producer benchmark: hammers POST /enqueue on benchmarks/demo_api.py.
 *
 * Measures the HTTP enqueue path (JSON parsing + idempotent Lua enqueue).
 * Start the API first:  python benchmarks/demo_api.py
 * Run:  k6 run benchmarks/k6_producer.js
 *
 * This benchmarks enqueue throughput/latency only -- workers are not
 * involved. Pair it with benchmarks/load_test.py for end-to-end numbers.
 */
import http from 'k6/http';
import { check } from 'k6';

export const options = {
  stages: [
    { duration: '10s', target: 10 },  // warm up
    { duration: '20s', target: 40 },  // sustain
    { duration: '10s', target: 0 },   // ramp down
  ],
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<500'],
  },
};

const URL = __ENV.DEMO_API_URL || 'http://localhost:8080/enqueue';

export default function () {
  const res = http.post(
    URL,
    JSON.stringify({
      fn: 'benchmark_noop',
      payload: { vu: __VU, iter: __ITER },
      idempotency_key: `k6-${__VU}-${__ITER}`, // unique per request: no dedup
      tenant: 'k6',
    }),
    { headers: { 'Content-Type': 'application/json' } },
  );
  check(res, { 'accepted (202)': (r) => r.status === 202 });
}
