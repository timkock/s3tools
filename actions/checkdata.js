'use strict';

// dependencies
var debug = require('debug')('s3tools')
  , inquirer = require('inquirer')
  , bytes = require('bytes')
  , async = require('async')
  , glob = require('glob')
  , path = require('path')
  , _ = require('lodash')
  , fs = require('fs');


/////////////
// HELPERS //
/////////////

var helpers = {
    logspeed: function (start) {
        var end = process.hrtime(start);
        return [end[0], String(end[1]/1000000000).slice(2)].join('.')+'s';
    }
};


////////////
// ACTION //
////////////

exports = module.exports = {
    name: 'checkdata',
    meta: {
        title: 'check data',
        description: 'checks indexed bucket data'
    },
    command: function (args, done) {
        return done(new Error('not implemented'));
    },
    interactive: function (args, done) {
        
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
            var start = process.hrtime();
            async.parallelLimit(_.map(files, function (file) {
                return function (plcb) {
                    fs.readFile(file.path, function (err, data) {
                        if(err) return wcb(err);
                        data = JSON.parse(data); 
                        plcb(null, {
                            bytes: _.sum(_.map(data, 'Size')),
                            files: data.length
                        });  
                    });
                };
            }), 4, function (err, results) {
                if(err) return wcb(err);
                console.log('PARSED', helpers.logspeed(start), bytes(_.sum(_.map(results, 'bytes'))), _.sum(_.map(results, 'files')), 'files');
                return wcb(null, files.length);
            });
        });
        
        // execute waterfall
        async.waterfall(tasks, done);

    },
    steps: [


    ]
};
