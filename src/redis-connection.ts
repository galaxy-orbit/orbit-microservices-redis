import { RespParser, RespEncoder, type RespValue } from './resp-protocol';
import type { Socket } from 'bun';

export interface RedisConnectionOptions {
  host: string;
  port: number;
  password?: string;
  db?: number;
  connectTimeout?: number;
  commandTimeout?: number;
  retryAttempts?: number;
  retryDelay?: number;
  keepAlive?: boolean;
}

type CommandCallback = (err: Error | null, result: RespValue) => void;

interface PendingCommand {
  resolve: (value: RespValue) => void;
  reject: (error: Error) => void;
  timeout?: ReturnType<typeof setTimeout>;
}

export class RedisConnection {
  private options: Required<RedisConnectionOptions>;
  private socket: Socket<any> | null = null;
  private parser: RespParser;
  private pendingCommands: PendingCommand[] = [];
  private isConnected: boolean = false;
  private isConnecting: boolean = false;
  private reconnectAttempts: number = 0;
  private messageHandler?: (channel: string, message: string) => void;
  private pmessageHandler?: (pattern: string, channel: string, message: string) => void;
  private subscribeHandler?: (channel: string, count: number) => void;
  private unsubscribeHandler?: (channel: string, count: number) => void;
  private errorHandler?: (error: Error) => void;
  private closeHandler?: () => void;
  private reconnectHandler?: () => void;
  private subscribedChannels: Set<string> = new Set();
  private subscribedPatterns: Set<string> = new Set();

  constructor(options: Partial<RedisConnectionOptions> = {}) {
    this.options = {
      host: options.host || 'localhost',
      port: options.port || 6379,
      password: options.password || '',
      db: options.db || 0,
      connectTimeout: options.connectTimeout || 10000,
      commandTimeout: options.commandTimeout || 30000,
      retryAttempts: options.retryAttempts || 10,
      retryDelay: options.retryDelay || 1000,
      keepAlive: options.keepAlive !== false,
    };
    this.parser = new RespParser();
  }

  async connect(): Promise<void> {
    if (this.isConnected) return;
    if (this.isConnecting) {
      await this.waitForConnection();
      return;
    }

    this.isConnecting = true;

    try {
      await this.createConnection();
      
      if (this.options.password) {
        await this.sendCommand('AUTH', this.options.password);
      }
      
      if (this.options.db !== 0) {
        await this.sendCommand('SELECT', this.options.db);
      }

      this.isConnected = true;
      this.isConnecting = false;
      this.reconnectAttempts = 0;
    } catch (error) {
      this.isConnecting = false;
      throw error;
    }
  }

  private async createConnection(): Promise<void> {
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error('Connection timeout'));
      }, this.options.connectTimeout);

      Bun.connect({
        hostname: this.options.host,
        port: this.options.port,
        socket: {
          open: (socket) => {
            clearTimeout(timeout);
            this.socket = socket;
            resolve();
          },
          data: (socket, data) => {
            this.handleData(data);
          },
          error: (socket, error) => {
            clearTimeout(timeout);
            this.handleError(error);
            reject(error);
          },
          close: () => {
            this.handleClose();
          },
          drain: () => {},
        },
      }).catch((err) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  private async waitForConnection(): Promise<void> {
    return new Promise((resolve) => {
      const check = setInterval(() => {
        if (this.isConnected || !this.isConnecting) {
          clearInterval(check);
          resolve();
        }
      }, 50);
    });
  }

  private handleData(data: Buffer): void {
    this.parser.append(data);
    
    const results = this.parser.parseAll();
    
    for (const result of results) {
      if (Array.isArray(result) && result.length >= 2) {
        const type = result[0] as string;
        
        if (['message', 'pmessage', 'subscribe', 'unsubscribe', 'psubscribe', 'punsubscribe'].includes(type)) {
          this.handlePubSubMessage(result);
          continue;
        }
      }
      
      const pending = this.pendingCommands.shift();
      if (pending) {
        if (pending.timeout) {
          clearTimeout(pending.timeout);
        }
        
        if (result instanceof Error) {
          pending.reject(result);
        } else {
          pending.resolve(result);
        }
      }
    }
  }

  private handlePubSubMessage(data: RespValue[]): void {
    const type = data[0] as string;
    
    switch (type) {
      case 'message':
        if (this.messageHandler) {
          this.messageHandler(data[1] as string, data[2] as string);
        }
        break;
      
      case 'pmessage':
        if (this.pmessageHandler) {
          this.pmessageHandler(data[1] as string, data[2] as string, data[3] as string);
        }
        break;
      
      case 'subscribe':
      case 'psubscribe':
        if (this.subscribeHandler) {
          this.subscribeHandler(data[1] as string, data[2] as number);
        }
        break;
      
      case 'unsubscribe':
      case 'punsubscribe':
        if (this.unsubscribeHandler) {
          this.unsubscribeHandler(data[1] as string, data[2] as number);
        }
        break;
    }
  }

  private handleError(error: Error): void {
    this.errorHandler?.(error);
    
    for (const pending of this.pendingCommands) {
      if (pending.timeout) {
        clearTimeout(pending.timeout);
      }
      pending.reject(error);
    }
    this.pendingCommands = [];
  }

  private handleClose(): void {
    this.isConnected = false;
    this.socket = null;
    this.closeHandler?.();
    
    if (this.reconnectAttempts < this.options.retryAttempts) {
      this.scheduleReconnect();
    } else {
      const error = new Error('Max reconnection attempts reached');
      for (const pending of this.pendingCommands) {
        if (pending.timeout) {
          clearTimeout(pending.timeout);
        }
        pending.reject(error);
      }
      this.pendingCommands = [];
    }
  }

  private scheduleReconnect(): void {
    const delay = this.options.retryDelay * Math.pow(2, this.reconnectAttempts);
    this.reconnectAttempts++;
    
    setTimeout(async () => {
      try {
        await this.connect();
        
        for (const channel of this.subscribedChannels) {
          await this.subscribe(channel);
        }
        for (const pattern of this.subscribedPatterns) {
          await this.psubscribe(pattern);
        }
        
        this.reconnectHandler?.();
      } catch {
        if (this.reconnectAttempts < this.options.retryAttempts) {
          this.scheduleReconnect();
        }
      }
    }, delay);
  }

  async sendCommand(command: string, ...args: (string | number)[]): Promise<RespValue> {
    if (!this.isConnected || !this.socket) {
      await this.connect();
    }

    return new Promise((resolve, reject) => {
      const pending: PendingCommand = { resolve, reject };
      
      pending.timeout = setTimeout(() => {
        const index = this.pendingCommands.indexOf(pending);
        if (index !== -1) {
          this.pendingCommands.splice(index, 1);
        }
        reject(new Error('Command timeout'));
      }, this.options.commandTimeout);

      this.pendingCommands.push(pending);
      
      const buffer = RespEncoder.encodeCommand(command, ...args);
      this.socket!.write(buffer);
    });
  }

  async subscribe(channel: string): Promise<void> {
    this.subscribedChannels.add(channel);
    await this.sendCommand('SUBSCRIBE', channel);
  }

  async unsubscribe(channel: string): Promise<void> {
    this.subscribedChannels.delete(channel);
    await this.sendCommand('UNSUBSCRIBE', channel);
  }

  async psubscribe(pattern: string): Promise<void> {
    this.subscribedPatterns.add(pattern);
    await this.sendCommand('PSUBSCRIBE', pattern);
  }

  async punsubscribe(pattern: string): Promise<void> {
    this.subscribedPatterns.delete(pattern);
    await this.sendCommand('PUNSUBSCRIBE', pattern);
  }

  async publish(channel: string, message: string): Promise<number> {
    const result = await this.sendCommand('PUBLISH', channel, message);
    return result as number;
  }

  async ping(): Promise<string> {
    const result = await this.sendCommand('PING');
    return result as string;
  }

  async quit(): Promise<void> {
    if (this.socket) {
      await this.sendCommand('QUIT');
      this.socket.end();
      this.socket = null;
    }
    this.isConnected = false;
    this.subscribedChannels.clear();
    this.subscribedPatterns.clear();
  }

  onMessage(handler: (channel: string, message: string) => void): void {
    this.messageHandler = handler;
  }

  onPMessage(handler: (pattern: string, channel: string, message: string) => void): void {
    this.pmessageHandler = handler;
  }

  onSubscribe(handler: (channel: string, count: number) => void): void {
    this.subscribeHandler = handler;
  }

  onUnsubscribe(handler: (channel: string, count: number) => void): void {
    this.unsubscribeHandler = handler;
  }

  onError(handler: (error: Error) => void): void {
    this.errorHandler = handler;
  }

  onClose(handler: () => void): void {
    this.closeHandler = handler;
  }

  onReconnect(handler: () => void): void {
    this.reconnectHandler = handler;
  }

  get connected(): boolean {
    return this.isConnected;
  }

  get subscribedChannelCount(): number {
    return this.subscribedChannels.size;
  }

  get subscribedPatternCount(): number {
    return this.subscribedPatterns.size;
  }
}
