'use strict';

// dependencies
var AWS = require('aws-sdk')
  , _ = require('lodash');

exports = module.exports = {
    name: 'listbuckets',
    meta: {
        title: 'list buckets',
        description: 'lists aws s3 buckets'
    },
    command: function (args, done) {
        return done(new Error('not implemented'));
    },
    interactive: function (args, done) {
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
