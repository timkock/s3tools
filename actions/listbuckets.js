'use strict';

// dependencies
var debug = require('debug')('s3tools:actions:listbuckets')
  , AWS = require('aws-sdk')
  , _ = require('lodash');


////////////
// ACTION //
////////////

exports = module.exports = {
    name: 'listbuckets',
    meta: {
        title: 'list buckets',
        description: 'lists aws s3 buckets'
    },
    command: function (args, done) {
        debug('command');
        return done(new Error('not implemented'));
    },
    interactive: function (args, done) {
        debug('interactive');
        return done(new Error('not implemented'));
    },
    steps: [
        function (done) {
            var s3 = new AWS.S3();
            s3.listBuckets(function(err, data) {
                if(err) return done(err);
                done(null, data.Buckets);
            });
        },
        function (buckets, done) {
            _.forEach(buckets, function (b) {
                console.log('Bucket: ', b.Name, ' : ', b.CreationDate);
            });
            done();
        }
    ]
};
