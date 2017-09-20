/* eslint no-undefined: off, init-declarations: off */
const moment = require('moment');
const { Socket } = require('net');
const stringify = require('json-stringify-safe');
const { Writable } = require('stream');

class BunyanFluentStream extends Writable {
  constructor (options) {
    super({ objectMode: true });
    this.host = (options || {}).host || 'localhost';
    this.port = (options || {}).port || 24224;
  }

  _write (chunk, enc, cb) {
    return this._writev([{ chunk }], cb);
  }

  _writev (chunks, cb) {
    const onError = (err) => {
      this._socket = null;
      return cb(err);
    };
    if (this._socket) {
      this._socket.once('error', onError);
      let tag;
      try {
        const ch0 = chunks[0].chunk;
        tag = `bunyan.${ch0.name}@${ch0.hostname}:${ch0.pid}`;
      } catch (err) {
        // drop the whole buffer
        this._socket.removeListener('error', onError);
        return cb(err);
      }
      const record = [tag, chunks.map(({ chunk }) => {
        let data;
        try {
          const time = moment.utc(chunk.time).unix();
          data = [time, chunk];
        } catch (err) {
          // drop the chunk and move on
        }
        return data;
      })];
      let out;
      try {
        out = stringify(record);
      } catch (err) {
        // drop the buffer
        this._socket.removeListener('error', onError);
        return cb(err);
      }
      return this._socket.write(out, () => {
        this._socket.removeListener('error', onError);
        return cb();
      });
    }
    // socket is not already open
    this._socket = new Socket();
    this._socket.once('error', onError);
    return this._socket.connect(this.port, this.host, () => {
      this._socket.unref();
      this._socket.removeListener('error', onError);
      this._writev(chunks, cb);
    });
  }

  _final (cb) {
    if (this._socket) {
      const onError = (err) => {
        this._socket = null;
        return cb(err);
      };
      this._socket.once('error', onError);
      this._socket.once('close', (hadError) => {
        this._socket.removeListener('error', onError);
        this._socket = null;
        if (!hadError) {
          return cb();
        }
        return null;
      });
      this._socket.end();
    }
  }
}

module.exports = BunyanFluentStream;
