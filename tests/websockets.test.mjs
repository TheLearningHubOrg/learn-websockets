import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';

describe('RFC 6455 Handshake Sec-WebSocket-Accept Hash Calculation', () => {
  function generateAcceptKey(clientKey) {
    const MAGIC_GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
    return crypto
      .createHash('sha1')
      .update(clientKey + MAGIC_GUID)
      .digest('base64');
  }

  it('matches official RFC 6455 specification test vector', () => {
    // Official test vector from RFC 6455 Section 1.3
    const clientKey = 'dGhlIHNhbXBsZSBub25jZQ==';
    const expectedAccept = 's3pPLMBiTxaQ9kYGzzhZRbK+xOo=';
    assert.equal(generateAcceptKey(clientKey), expectedAccept);
  });
});

describe('RFC 6455 4-Byte XOR Bitwise Masking & Unmasking', () => {
  function applyXorMask(payloadBuffer, maskKeyBuffer) {
    const result = Buffer.alloc(payloadBuffer.length);
    for (let i = 0; i < payloadBuffer.length; i++) {
      result[i] = payloadBuffer[i] ^ maskKeyBuffer[i % 4];
    }
    return result;
  }

  it('proves that XOR masking is symmetric and losslessly reversible', () => {
    const originalText = 'Hello WebSocket World!';
    const rawPayload = Buffer.from(originalText, 'utf8');
    const maskKey = Buffer.from([0x37, 0xfa, 0x21, 0x3d]); // 4-byte random key

    const maskedPayload = applyXorMask(rawPayload, maskKey);
    assert.notEqual(maskedPayload.toString('utf8'), originalText);

    // Unmask using identical key
    const unmaskedPayload = applyXorMask(maskedPayload, maskKey);
    assert.equal(unmaskedPayload.toString('utf8'), originalText);
  });
});

describe('Exponential Backoff with Full Jitter', () => {
  function calculateBackoffWithJitter(attempt, baseDelay = 1000, maxDelay = 30000) {
    const exponential = Math.min(maxDelay, baseDelay * Math.pow(2, attempt));
    return Math.floor(Math.random() * exponential);
  }

  it('respects maximum delay boundaries and generates positive durations', () => {
    for (let attempt = 0; attempt < 10; attempt++) {
      const delay = calculateBackoffWithJitter(attempt, 500, 10000);
      assert.ok(delay >= 0);
      assert.ok(delay <= 10000);
    }
  });
});

describe('Sliding Window Message Deduplication', () => {
  class MessageDeduplicator {
    constructor(windowSize = 3) {
      this.windowSize = windowSize;
      this.seenIds = new Set();
      this.orderQueue = [];
    }

    isDuplicate(messageId) {
      if (this.seenIds.has(messageId)) {
        return true;
      }
      this.seenIds.add(messageId);
      this.orderQueue.push(messageId);

      if (this.orderQueue.length > this.windowSize) {
        const oldest = this.orderQueue.shift();
        this.seenIds.delete(oldest);
      }
      return false;
    }
  }

  it('identifies duplicates and evicts oldest entries past window size', () => {
    const dedup = new MessageDeduplicator(3);

    assert.equal(dedup.isDuplicate('msg_1'), false);
    assert.equal(dedup.isDuplicate('msg_2'), false);
    assert.equal(dedup.isDuplicate('msg_3'), false);

    // Immediate duplicate
    assert.equal(dedup.isDuplicate('msg_2'), true);

    // Push 4th message -> evicts msg_1
    assert.equal(dedup.isDuplicate('msg_4'), false);

    // msg_1 is now evicted from window, so it is treated as new
    assert.equal(dedup.isDuplicate('msg_1'), false);
  });
});
