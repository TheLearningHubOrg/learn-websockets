import type { WebSocket } from 'ws';

export interface ExtWebSocket extends WebSocket {
  isAlive: boolean;
  clientId: string;
  room?: string;
}

export interface WsMessage {
  type: 'ping' | 'pong' | 'join' | 'chat' | 'system' | 'broadcast';
  sender?: string;
  room?: string;
  payload?: any;
  timestamp: number;
}
