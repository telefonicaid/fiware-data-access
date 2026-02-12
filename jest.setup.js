// Jest setup file to provide minimal Web APIs required by undici/testcontainers
// Must run before test files import modules that depend on undici.
import { Readable } from 'node:stream';
import { TextEncoder, TextDecoder } from 'node:util';

if (typeof globalThis.File === 'undefined') {
  globalThis.File = class File {
    constructor(bits, filename, options = {}) {
      this.bits = bits;
      this.name = filename;
      this.type = options.type || '';
      this.lastModified = options.lastModified || Date.now();
    }
    get size() {
      return this.bits.reduce((acc, bit) => {
        if (typeof bit === 'string')
          return acc + new TextEncoder().encode(bit).length;
        if (bit instanceof ArrayBuffer) return acc + bit.byteLength;
        if (ArrayBuffer.isView(bit)) return acc + bit.byteLength;
        return acc + (bit && bit.length ? bit.length : 0);
      }, 0);
    }
    async text() {
      const parts = [];
      for (const bit of this.bits) {
        if (typeof bit === 'string') parts.push(bit);
        else if (bit instanceof ArrayBuffer)
          parts.push(new TextDecoder().decode(bit));
        else parts.push(String(bit));
      }
      return parts.join('');
    }
    slice() {
      return new File(this.bits, this.name, { type: this.type });
    }
    stream() {
      return Readable.from(this.bits);
    }
  };
}

if (typeof globalThis.Blob === 'undefined') {
  globalThis.Blob = class Blob {
    constructor(bits, options = {}) {
      this.bits = bits;
      this.type = options.type || '';
    }
    get size() {
      return this.bits.reduce((acc, bit) => {
        if (typeof bit === 'string')
          return acc + new TextEncoder().encode(bit).length;
        if (bit instanceof ArrayBuffer) return acc + bit.byteLength;
        if (ArrayBuffer.isView(bit)) return acc + bit.byteLength;
        return acc + (bit && bit.length ? bit.length : 0);
      }, 0);
    }
    async text() {
      const parts = [];
      for (const bit of this.bits) {
        if (typeof bit === 'string') parts.push(bit);
        else if (bit instanceof ArrayBuffer)
          parts.push(new TextDecoder().decode(bit));
        else parts.push(String(bit));
      }
      return parts.join('');
    }
    slice() {
      return new Blob(this.bits, { type: this.type });
    }
    stream() {
      return Readable.from(this.bits);
    }
  };
}

if (typeof globalThis.TextEncoder === 'undefined')
  globalThis.TextEncoder = TextEncoder;
if (typeof globalThis.TextDecoder === 'undefined')
  globalThis.TextDecoder = TextDecoder;
