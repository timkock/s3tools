'use strict';

// dependencies
var AWS = require('aws-sdk')
  , path = require('path')
  , bytes = require('bytes')
  , async = require('async')
  , mkdirp = require('mkdirp')
  , moment = require('moment')
  , uuid = require('node-uuid')
  , inquirer = require('inquirer')
  , _ = require('lodash')
  , fs = require('fs');


/////////////
// HELPERS //
/////////////

var helpers = {
    filesize: function (items) {
        return _.reduce(items, function (total, n) {
            return total + n.Size;
        }, 0);
    },
    logspeed: function (start) {
        var end = process.hrtime(start);
        return [end[0], String(end[1]/1000000000).slice(2)].join('.');
    }
};


////////////
// ACTION //
////////////

exports = module.exports = {
    name: 'listbucket',
    meta: {
        title: 'index bucket',
        description: 'makes an index of a chosen bucket'
    },
    command: function (args, done) {
        return done(new Error('not implemented'));
    },
    interactive: function (args, done) {

        // variables
        var S3 = new AWS.S3();
        var taskid = uuid.v1();
        var timestamp = moment().format('YYYYMMDDHHmmSS');
        var tasks = [];
        
        // create temporary directory
        tasks.push(function (wcb) {
            var taskdir = path.resolve(__appdir, '.tmp', [timestamp, taskid].join('-'));
            mkdirp(taskdir, function (err) {
                if(err) return wcb(err);
                wcb(null, taskdir);
            });
        });

        // list buckets
        tasks.push(function (dir, wcb) {
            S3.listBuckets(function(err, data) {
                if(err) return wcb(err);
                wcb(null, dir, data.Buckets);
            });
        });

        // pick bucket
        tasks.push(function (dir, buckets, wcb) {
            inquirer.prompt({
                type: 'list',
                name: 'bucket',
                message: 'pick a bucket',
                choices: _.map(buckets, 'Name')
            }).then(function (answer) {
                wcb(null, dir, answer.bucket);
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
                    totalsize += helpers.filesize(batchdata);
                    totalitems += data.Contents.length;

                    fs.appendFile(path.join(dir, +moment()+'.json'), JSON.stringify(batchdata), function (err) {
                        if(err) return dcb(err);
                        console.log('BATCH', totalitems, bytes(totalsize), helpers.logspeed(batchstart));
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
                console.log(result.items, bytes(result.size), helpers.logspeed(start));
                wcb();
            });
            
        });

        async.waterfall(tasks, done);

    },
    steps: [
    ]
};
