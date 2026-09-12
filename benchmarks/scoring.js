import grpc from 'k6/net/grpc';
import { check } from 'k6';
import { SharedArray } from 'k6/data';
import { Rate, Counter } from 'k6/metrics';

const client = new grpc.Client();
client.load(['../contracts'], 'fraud.proto');
const inputs = new SharedArray('transactions', () => open('../data/transactions.jsonl').trim().split('\n').map(JSON.parse));
const errors = new Rate('scoring_errors');
const successes = new Counter('successful_scores');
const demoResponses = new Counter('demo_model_responses');
const rate = Number(__ENV.RATE || 5000);
export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: { scoring: { executor: 'constant-arrival-rate', rate, timeUnit: '1s',
    duration: __ENV.DURATION || '15m', preAllocatedVUs: 200, maxVUs: 1000 } },
  thresholds: {
    grpc_req_duration: ['p(99)<100'], scoring_errors: ['rate<0.001'],
    dropped_iterations: ['count==0'], successful_scores: [`rate>=${rate * 0.999}`],
  },
};
let connected = false;
export default function () {
  try {
    if (!connected) {
      client.connect(__ENV.TARGET || 'localhost:50051', { plaintext: true, timeout: '5s' });
      connected = true;
    }
    const row = inputs[Math.floor(Math.random() * inputs.length)];
    const result = client.invoke('fraud.v1.FraudScorer/Score', {
      entityId: row.entity_id, transactionId: row.transaction_id,
    }, { timeout: '2s' });
    if (result.status === grpc.StatusOK && (result.message.demoModel || result.message.demo_model)) demoResponses.add(1);
    const ok = check(result, {
      'status OK': r => r.status === grpc.StatusOK,
      'trained model when required': r => __ENV.REQUIRE_TRAINED === '0' ||
        (r.status === grpc.StatusOK &&
          String(r.message.modelVersion || r.message.model_version || '').startsWith('xgb-')),
    });
    errors.add(!ok);
    if (ok) successes.add(1);
  } catch (_) {
    errors.add(true);
    connected = false;
  }
}
