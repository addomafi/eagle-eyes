'use strict';

let path = require('path')
let Eagleeyes = require(path.join(__dirname, '.', 'eagleEyes.js'))

console.log('Loading function');

exports.handler = function(event, context, callback) {
    var eagle = new Eagleeyes()
    eagle.process().then(results => {
      console.log(JSON.stringify(results));
      callback(null, "Success");
    }).catch(err => {
      callback(err, "Error");
    });
};
