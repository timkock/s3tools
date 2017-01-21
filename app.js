'use strict';

// dependencies
var debug = require('debug')('s3tools')
  , program = require('commander')
  , request = require('request')
  , async = require('async')
  , path = require('path')
  , glob = require('glob')
  , _ = require('lodash');

// globals
global.__config = require('./config.json');
global.__appenv = process.env.NODE_ENV || 'development';
global.__debug = process.env.DEBUG || false;
global.__appdir = __dirname;
require('colors');


////////////
// EVENTS //
////////////

// gracefull shutdown
function grace(code) { process.exit(code); }

// error handler
function handle (err, severe) {
    debug(severe ? 'SEVERE' : '', 'error'.bold.red.inverse, err.message, '\n');
    if(severe) throw err;
}

// handle exit events
process.on('exit', grace);

// handle system signals
['SIGINT', 'SIGTERM'].forEach(function (signal) {
    process.on(signal, function () {
        console.log('\nshutdown from signal {SIGNAL}'.replace('{SIGNAL}', signal).bold.red.inverse);
        return grace(1);
    });
});


//////////
// UTIL //
//////////

function getActions (done) {
    glob(path.join(__dirname, 'actions', '*.js'), function (err, files) {
        if(err) return done(err);
        done(null, _.chain(files)
            .map(function (a) {
                try {
                    var mod = require(a);
                    return _.merge(mod, {
                        path: a
                    });
                } catch (e) {
                    handle(e, false);
                    return {};
                }
            })
            .filter(function (a) { return _.keys(a).length > 0; })
            .value());
    });
}


//////////
// MAIN //
//////////

// parse argv
program
    .version(require(path.join(__appdir, 'package.json')).version)
    .option('-l, --list', 'list available actions')
    .option('-i, --interactive <action>', 'call action interactively')
    .option('-c, --cli <action>', 'call action via cli')
    .option('-e, --execute <action>', 'call action steps')
    .parse(process.argv.length > 4 ? _.dropRight(process.argv, process.argv.length-4) : process.argv);

// debug cutoff arguments
var cutoff = process.argv.length > 4 ? _.drop(process.argv, 4) : [];
debug('cutoff arguments', cutoff);

// get actions
getActions(function (err, actions) {
    if(err) return handle(err, true);

    // state
    var action;

    /* LIST ACTIONS */

    if(program.list) {

        // output choice
        console.log('\nLIST ACTIONS'.bold.white.inverse, 'node app -l');
        console.log('');

        // list actions
        _.forEach(actions, function (a, i) {
            console.log('ACTION {i}'.replace('{i}', i).green, a.name.bold, a.path.gray);
        });

    }

    /* EXECUTE CLI */

    if(program.cli) {

        // output choice
        console.log('\nACTION CLI'.bold.white.inverse, 'node app -c {action}'.replace('{action}', program.cli));
        console.log('');

        // execute if present
        action = _.find(actions, { name: program.cli });
        if(action) {
            action.command(cutoff, function (err, output) {
                if(err) handle(err, true);
                console.log(output);
            });
        } else {
            debug('action not found', program.cli);
        }

    }

    /* EXECUTE INTERACTIVE */

    if(program.interactive) {

        // output choice
        console.log('\nACTION INTERACTIVE'.bold.white.inverse, 'node app -i {action}'.replace('{action}', program.interactive));
        console.log('');

        // execute if present
        action = _.find(actions, { name: program.interactive });
        if(action) {
            action.interactive(cutoff, function (err, output) {
                if(err) handle(err, true);

                request.post({
                    url: __config.slack,
                    form: {
                        payload: JSON.stringify({
                            username: 's3tools',
                            text: 's3tools done',
                            icon_emoji: ':beer:'
                        })
                    }
                }, function(err, res, body) {
                    if (err) return handle(err);
                    console.log(output, body);
                });
                
            });
        } else {
            debug('action not found', program.interactive);
        }

    }

    /* EXECUTE ACTION */

    if(program.execute) {

        // output choice
        console.log('\nEXECUTE ACTION'.bold.white.inverse, 'node app -e {action}'.replace('{action}', program.execute));
        console.log('');

        // execute if present
        action = _.find(actions, { name: program.execute });
        if(action) {
            async.waterfall(action.steps, function (err, data) {
                if(err) handle(err, true);
                if(data) console.log(data);
            });
        } else {
            debug('action not found', program.execute);
        }
        
    }

});
    