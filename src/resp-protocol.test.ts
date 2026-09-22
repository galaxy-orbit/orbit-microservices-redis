import { describe, test, expect } from 'bun:test';
import { RespParser, RespEncoder, parsePubSubMessage } from './resp-protocol';

describe('RespParser', () => {
  test('parses simple strings', () => {
    const parser = new RespParser();
    parser.append(Buffer.from('+OK\r\n'));
    expect(parser.parse()).toBe('OK');
  });

  test('parses integers', () => {
    const parser = new RespParser();
    parser.append(Buffer.from(':42\r\n'));
    expect(parser.parse()).toBe(42);
  });

  test('parses errors as Error objects', () => {
    const parser = new RespParser();
    parser.append(Buffer.from('-WRONGTYPE unknown\r\n'));
    const value = parser.parse();
    expect(value).toBeInstanceOf(Error);
    expect((value as Error).message).toBe('WRONGTYPE unknown');
  });

  test('parses bulk strings including empty and null', () => {
    const parser = new RespParser();
    parser.append(Buffer.from('$5\r\norbit\r\n$0\r\n\r\n$-1\r\n'));
    expect(parser.parse()).toBe('orbit');
    expect(parser.parse()).toBe('');
    expect(parser.parse()).toBeNull();
  });

  test('parses nested arrays', () => {
    const parser = new RespParser();
    parser.append(Buffer.from('*2\r\n$3\r\nget\r\n*1\r\n$3\r\nkey\r\n'));
    expect(parser.parse()).toEqual(['get', ['key']]);
  });

  test('buffers partial messages until complete', () => {
    const parser = new RespParser();
    parser.append(Buffer.from('$5\r\nhel'));
    expect(parser.parse()).toBeNull();
    parser.append(Buffer.from('lo\r\n'));
    expect(parser.parse()).toBe('hello');
  });

  test('parseAll drains every complete message', () => {
    const parser = new RespParser();
    parser.append(Buffer.from(':1\r\n:2\r\n:3\r\n'));
    expect(parser.parseAll()).toEqual([1, 2, 3]);
  });

  test('throws on unknown type byte', () => {
    const parser = new RespParser();
    parser.append(Buffer.from('?bad\r\n'));
    expect(() => parser.parse()).toThrow(/Unknown RESP type/);
  });
});

describe('RespEncoder', () => {
  test('encodeCommand builds an array of bulk strings', () => {
    const buf = RespEncoder.encodeCommand('GET', 'user:1');
    expect(buf.toString()).toBe('*2\r\n$3\r\nGET\r\n$6\r\nuser:1\r\n');
  });

  test('encodeSimpleString / encodeError / encodeInteger', () => {
    expect(RespEncoder.encodeSimpleString('OK')).toBe('+OK\r\n');
    expect(RespEncoder.encodeError('WRONGTYPE')).toBe('-WRONGTYPE\r\n');
    expect(RespEncoder.encodeInteger(7)).toBe(':7\r\n');
  });

  test('encodeBulkString and null', () => {
    expect(RespEncoder.encodeBulkString('orbit')).toBe('$5\r\norbit\r\n');
    expect(RespEncoder.encodeBulkString(null)).toBe('$-1\r\n');
  });

  test('encodeArray with mixed values and null', () => {
    expect(RespEncoder.encodeArray(['a', 2, null])).toBe(
      '*3\r\n$1\r\na\r\n:2\r\n$-1\r\n'
    );
  });
});

describe('parsePubSubMessage', () => {
  test('extracts message from pubsub array', () => {
    const result = parsePubSubMessage(['message', 'channel:1', 'payload'] as any);
    expect(result).toMatchObject({ type: 'message' });
  });
});
