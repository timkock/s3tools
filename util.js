'use strict';

// dependencies
var xxhash = require('xxhash');

// constants
var SEED = 0xCAFECAFE;


////////////
// MODULE //
////////////

exports = module.exports = {
    logspeed: function (start) {
        var end = process.hrtime(start);
        return [end[0], String(end[1]/1000000000).slice(2)].join('.')+'s';
    },
    hash: function (input) {
        return xxhash.hash(Buffer.from(input, 'utf8'), SEED, 'hex');
    }
};
