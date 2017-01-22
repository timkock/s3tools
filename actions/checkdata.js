'use strict';

// dependencies
var debug = require('debug')('s3tools:actions:checkdata')
  , inquirer = require('inquirer')
  , redis = require('redis')
  , bytes = require('bytes')
  , async = require('async')
  , glob = require('glob')
  , path = require('path')
  , _ = require('lodash')
  , fs = require('fs');

// components
var Util = require(path.join(__appdir, 'util'));


////////////
// ACTION //
////////////

exports = module.exports = {
    name: 'checkdata',
    meta: {
        title: 'check data',
        description: 'check indexed bucket stats & duplicates'
    },
    command: function (args, done) {
        debug('command');
        return done(new Error('not implemented'));
    },
    interactive: function (args, done) {

        // state
        var tasks = [];

        // chose bucket
        tasks.push(function (wcb) {
            fs.readdir(path.join(__appdir, 'data', 'buckets'), function (err, files) {
                if(err) return wcb(err);
                inquirer.prompt({
                    type: 'list',
                    name: 'bucket',
                    message: 'pick a bucket',
                    choices: files
                }).then(function (answer) {
                    wcb(null, answer.bucket);
                });
            });
        });

        // get all files in bucket
        tasks.push(function (bucket, wcb) {
            glob(path.join(__appdir, 'data', 'buckets', bucket, '*.json'), function (err, files) {
                if(err) return wcb(err);
                wcb(null, bucket, _.map(files, function (file) {
                    return {
                        path: file,
                        name: path.basename(file),
                        timestamp: path.basename(file).replace(path.extname(file), '')
                    };
                })); 
            });
        });

        // parse files
        tasks.push(function (bucket, files, wcb) {
            var START = process.hrtime();
            var RCLIENT = redis.createClient();
            async.parallelLimit(_.map(files, function (file) {
                return function (plcb) {
                    fs.readFile(file.path, function (err, data) {
                        if(err) return wcb(err);
                        data = JSON.parse(data);   
                       
                        // build hashmap
                        var multi = RCLIENT.multi();
                        _.forEach(data, function (r) {
                            var hash = Util.hash([path.basename(r.Key), r.Size].join('|'));
                            multi.SADD([bucket,hash].join(':'), JSON.stringify(r));
                        });

                        // conclude batch with stats
                        multi.exec(function (err, replies) {
                            if(err) return plcb(err);
                            plcb(null, {
                                replies: _.sum(replies),
                                bytes: _.sum(_.map(data, 'Size')),
                                files: data.length
                            });  
                        });

                    });
                };
            }), 4, function (err, results) {
                if(err) return wcb(err);

                // STATS
                console.log('PARSED', Util.logspeed(START), bytes(_.sum(_.map(results, 'bytes'))), _.sum(_.map(results, 'files')), 'files');
                
                // DUPLICATES
                RCLIENT.KEYS([bucket,'*'].join(':'), function (err, results) {
                    if(err) return wcb(err);
                    async.parallelLimit(_.map(results, function (r) {
                        return function (plcb) {
                            RCLIENT.SMEMBERS(r, function (err, values) {
                                if(err) return plcb(err);
                                values = _.map(values, JSON.parse);
                                plcb(null, values.length > 1 ? (values.length-1)*_.head(values).Size : 0);
                            });
                        };
                    }), 4, function (err, duplicatebytes) {
                        if(err) return wcb(err);

                        // echo duplicates
                        console.log('DUPLICATE', Util.logspeed(START), bytes(_.sum(duplicatebytes)));

                        // close rclient & wcb
                        RCLIENT.end(true);
                        return wcb(null, files.length);    

                    });
                });
                
            });
        });


        // execute waterfall
        async.waterfall(tasks, done);

    },
    steps: [
    ]
};
