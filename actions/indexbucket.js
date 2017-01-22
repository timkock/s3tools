'use strict';

// dependencies
var debug = require('debug')('s3tools:actions:indexbucket')
  , inquirer = require('inquirer')
  , mkdirp = require('mkdirp')
  , moment = require('moment')
  , AWS = require('aws-sdk')
  , bytes = require('bytes')
  , async = require('async')
  , path = require('path')
  , uuid = require('uuid')
  , _ = require('lodash')
  , fs = require('fs');

// components
var Util = require(path.join(__appdir, 'util'));


////////////
// ACTION //
////////////

exports = module.exports = {
    name: 'indexbucket',
    meta: {
        title: 'index bucket',
        description: 'makes an index of a chosen bucket'
    },
    command: function (args, done) {
        debug('command');
        return done(new Error('not implemented'));
    },
    interactive: function (args, done) {

        // variables
        var S3 = new AWS.S3();
        var taskid = uuid.v1();
        var timestamp = moment().format('YYYYMMDDHHmmSS');
        var tasks = [];

        // list buckets
        tasks.push(function (wcb) {
            S3.listBuckets(function(err, data) {
                if(err) return wcb(err);
                wcb(null, data.Buckets);
            });
        });

        // pick bucket
        tasks.push(function (buckets, wcb) {
            inquirer.prompt({
                type: 'list',
                name: 'bucket',
                message: 'pick a bucket',
                choices: _.map(buckets, 'Name')
            }).then(function (answer) {
                wcb(null, answer.bucket);
            });
        });

        // create temporary directory
        tasks.push(function (bucket, wcb) {
            var taskdir = path.resolve(__appdir, '.tmp', [timestamp, bucket, taskid].join('-'));
            mkdirp(taskdir, function (err) {
                if(err) return wcb(err);
                wcb(null, taskdir, bucket);
            });
        });

        // list bucket contents
        tasks.push(function (dir, bucket, wcb) {

            // state
            var totalitems = 0;
            var totalsize = 0;
            var start = process.hrtime();
            
            // recursiver walker
            var list = function (params, dcb) {
                var batchstart = process.hrtime();
                S3.listObjectsV2(params, function (err, data) {
                    if(err) return dcb(err);

                    var batchdata = data.Contents;
                    totalsize +=  _.reduce(batchdata, function (total, n) {
                        return total + n.Size;
                    }, 0);
                    totalitems += data.Contents.length;

                    fs.appendFile(path.join(dir, +moment()+'.json'), JSON.stringify(batchdata), function (err) {
                        if(err) return dcb(err);
                        debug('BATCH', totalitems, bytes(totalsize), Util.logspeed(batchstart));
                        if(data.IsTruncated) {
                            fs.appendFile(path.join(dir, 'tokens.log'), JSON.stringify({
                                ContinuationToken: data.NextContinuationToken,
                                items: totalitems,
                                size: totalsize
                            })+'\n', { flags: 'a' }, function (err) {
                                if(err) return dcb(err);
                                list(_.merge(params, { ContinuationToken: data.NextContinuationToken }), dcb);    
                            });
                        } else {
                            dcb(null, {
                                items: totalitems,
                                size: totalsize
                            });
                        }
                    });

                });
               
            };

            // initiate & consolidate
            list({ Bucket: bucket }, function (err, result) {
                if(err) return wcb(err);
                console.log('\n');
                console.log(result.items, bytes(result.size), Util.logspeed(start));
                wcb(null, dir, bucket);
            });
            
        });

        // move bucket data to data folder
        tasks.push(function (dir, bucket, wcb) {
            var targetPath = path.join(__appdir, 'data', 'buckets');
            mkdirp(targetPath, function (err) {
                if(err) return wcb(err);
                fs.rename(dir, path.join(targetPath, bucket), function (err) {
                    if(err) return wcb(err);
                    wcb();
                });
            });
        });

        // execute waterfall
        async.waterfall(tasks, done);

    },
    steps: [
    ]
};
