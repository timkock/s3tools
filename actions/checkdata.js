'use strict';

// dependencies
var debug = require('debug')('s3tools:actions:checkdata')
  , inquirer = require('inquirer')
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
                wcb(null, _.map(files, function (file) {
                    return {
                        path: file,
                        name: path.basename(file),
                        timestamp: path.basename(file).replace(path.extname(file), '')
                    };
                })); 
            });
        });

        // parse files
        tasks.push(function (files, wcb) {
            var START = process.hrtime();
            var hashmap = {};
            async.parallelLimit(_.map(files, function (file) {
                return function (plcb) {
                    fs.readFile(file.path, function (err, data) {
                        if(err) return wcb(err);
                        data = JSON.parse(data);   
                                              
                        // hashmap keys
                        _.forEach(data, function (r) {
                            var hash = Util.hash([path.basename(r.Key), r.Size].join('|'));
                            if(!hashmap[hash]) hashmap[hash] = [];
                            hashmap[hash].push(r);
                        });

                        // conclude batch with stats
                        plcb(null, {
                            bytes: _.sum(_.map(data, 'Size')),
                            files: data.length
                        });  

                    });
                };
            }), 4, function (err, results) {
                if(err) return wcb(err);

                // STATS
                console.log('PARSED', Util.logspeed(START), bytes(_.sum(_.map(results, 'bytes'))), _.sum(_.map(results, 'files')), 'files');
                
                // DUPLICATES
                var duplicates = _.filter(hashmap, function (r) {
                    return r.length > 1;
                });
                console.log('DUPLICATE', bytes(_.reduce(duplicates, function (total, n) {
                    return total + ((n.length-1)*_.head(n).Size);
                }, 0)));

                return wcb(null, files.length);
                
            });
        });


        // execute waterfall
        async.waterfall(tasks, done);

    },
    steps: [
    ]
};
