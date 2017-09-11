/**
 * Module dependencies
 */

var path = require('path');
var Writable = require('stream').Writable;
var Transform = require('stream').Transform;
var concat = require('concat-stream');
var _ = require('lodash');
_.defaultsDeep = require('merge-defaults');
var oss = require('ali-oss');
var knox = require('knox');
var S3MultipartUpload = require('knox-mpu-alt');
var S3Lister = require('s3-lister');
var mime = require('mime');

/**
 * skipper-s3
 *
 * @param  {Object} globalOpts
 * @return {Object}
 */

module.exports = function SkipperS3(globalOpts) {
  globalOpts = globalOpts || {};

  // console.log('S3 adapter was instantiated...');

  var adapter = {

    read: function (fd, cb) {

      var prefix = fd;

      var store = oss({
        accessKeyId: globalOpts.key,
        accessKeySecret: globalOpts.secret,
        bucket: globalOpts.bucket,
        region: globalOpts.region || undefined
      });

      // Build a noop transform stream that will pump the S3 output through
      var __transform__ = new Transform();
      __transform__._transform = function (chunk, encoding, callback) {
        return callback(null, chunk);
      };

      var getStreamResult = yield store.getStream(prefix);

      // Handle explicit s3res errors

      // check whether we got an actual file stream:
      if (getStreamResult.status < 300) {
        getStreamResult.stream.once('error', function (err) {
          __transform__.emit('error', err);
        });
        getStreamResult.stream.pipe(__transform__);
      } else {
        var err = new Error();
        err.status = getStreamResult.status;
        err.headers = getStreamResult.headers;
        err.message = 'Non-200 status code returned from Aliyun for requested file.';
        __transform__.emit('error', err);
      }

      if (cb) {
        var firedCb;
        __transform__.once('error', function (err) {
          if (firedCb) return;
          firedCb = true;
          cb(err);
        });
        __transform__.pipe(concat(function (data) {
          if (firedCb) return;
          firedCb = true;
          cb(null, data);
        }));
      }

      return __transform__;
    },

    rm: function (fd, cb) {
      var store = oss({
        accessKeyId: globalOpts.key,
        accessKeySecret: globalOpts.secret,
        bucket: globalOpts.bucket,
        region: globalOpts.region || undefined
      });

      var result = yield store.delete(fd);

      if (result.status == 200) {
        cb();
      } else {
        cb({
          statusCode: result.status,
          message: result
        });
      }
    },
    ls: function (dirname, cb) {
      var store = oss({
        accessKeyId: globalOpts.key,
        accessKeySecret: globalOpts.secret,
        bucket: globalOpts.bucket,
        region: globalOpts.region || undefined
      });

      // Strip leading slash from dirname to form prefix
      var prefix = dirname.replace(/^\//, '');

      try {
        var listResult = yield store.list({
          prefix: prefix
        });
        cb(null, listResult.objects);
      } catch (err) {
        cb(err, null);
      }
    },

    receive: AliyunReceiver
  };

  return adapter;

  /**
   * A simple receiver for Skipper that writes Upstreams to
   * S3 to the configured bucket at the configured path.
   *
   * Includes a garbage-collection mechanism for failed
   * uploads.
   *
   * @param  {Object} options
   * @return {Stream.Writable}
   */
  function AliyunReceiver(options) {
    options = options || {};
    options = _.defaults(options, globalOpts);

    var receiver__ = Writable({
      objectMode: true
    });

    receiver__._write = function onFile(__newFile, encoding, next) {
      var store = oss({
        accessKeyId: options.key,
        accessKeySecret: options.secret,
        bucket: options.bucket,
        region: globalOpts.region || undefined
      });
      try {
        var object = yield store.putStream(__newFile.fd, __newFile);
        next();
      } catch (err) {
        return next({
          incoming: __newFile,
          code: 'E_WRITE',
          stack: typeof err === 'object' ? err.stack : new Error(err),
          name: typeof err === 'object' ? err.name : err,
          message: typeof err === 'object' ? err.message : err
        });
      }
    };

    return receiver__;
  }
};


