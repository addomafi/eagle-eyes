#!/usr/bin/env node

/**
 * Created by adautomartins on 10/05/17.
 */

var path = require('path')
var Eagleeyes = require(path.join(__dirname, '..', 'eagleEyes.js'))
var eagle = new Eagleeyes()

eagle.process({testList:["AWOsYe8anYfu7GUPBgBjK"]}).then(results => {
  console.log(JSON.stringify(results));
}).catch(err => {
  console.log(err)
});
