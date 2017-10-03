#!/usr/bin/env node

/**
 * Created by adautomartins on 10/05/17.
 */

let EventSource = require('eventsource')
let elkcli = require('elasticsearch')
let uuidv1 = require('uuid/v1')
let moment = require('moment')
let _ = require('lodash')

var es = new EventSource('http://osb.console.green.prd.smiles.local.br:7070/turbine.stream?cluster=cb-osb-prd')

var elk = new elkcli.Client({
  host: 'http://search-es-prd-alarms-v6q6n66yg6bohxk3wthxbgkvnm.us-east-1.es.amazonaws.com',
  log: 'warning'
});

var buffer = []
es.onmessage = function(event) {
  var metric = JSON.parse(event.data)
  var type = metric.type
  if (metric.type !== "meta") {
    _.forOwn(metric.latencyExecute, (value, key) => {
      if (key.indexOf('.') > -1)
        delete metric.latencyExecute[key]
    })
    delete metric.latencyTotal
    delete metric.latencyTotal_mean
    delete metric.group
    delete metric.type
    metric.timestamp = moment().format('x')

    buffer.push({ index:  { _index: "hystrix-osb-detail-prod-", _type: type, _id: uuidv1() } })
    buffer.push(metric)

    if (buffer.length == 200) {
      elk.bulk({
        body: buffer
      }, function (error, response) {
        if (error) {
          console.log(error);
        }
      });

      buffer = []
    }
  }
}
