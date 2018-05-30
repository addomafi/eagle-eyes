let es = require('elasticsearch')
let _ = require('lodash')
let extend = require('extend')
let moment = require('moment-timezone')
let request = require('request-promise-native')
// request.defaults({'proxy': '10.2.3.41:3128'})
// const proxy = require('proxy-agent');
var PromiseBB = require("bluebird");

var eagleeyes = function() {
  var self = this
  self.sourceES = new es.Client({
    host: process.env.SOURCE_ELK_HOST,
		// createNodeAgent: () => proxy('http://10.2.3.41:3128'),
    log: 'warning'
  });

  self.targetES = new es.Client({
    host: process.env.TARGET_ELK_HOST,
		// createNodeAgent: () => proxy('http://10.2.3.41:3128'),
    log: 'warning'
  });

  self.timelion = {
    url: `https://${process.env.TARGET_ELK_HOST}/_plugin/kibana/api/timelion/run`,
    headers: {
      "kbn-version": "5.3.2"
    }
  };

  self.pagerduty = {
    url: process.env.PG_URL,
    json: {
      "routing_key": process.env.PG_KEY
    }
  };

  self.options = {
    concurrency: parseInt(process.env.CONCURRENCY)
  };

  self._init = function() {
    return new Promise((resolve, reject) => {
      if (!self.config || process.hrtime(self.config.loadTime)[0] > 60) {
        self.sourceES.search({
          index: ".eagle-eyes-control",
          type: "delayed-alarms",
          body: {
            "query": {
              "range": {
                "timeFrame": {
                  "gte": moment().subtract(1, 'minutes').format('x'),
                  "lte": moment().format('x'),
                  "format": "epoch_millis"
                }
              }
            },
            "size": 1000
          }
        }).then(delayed => {
          self.sourceES.search({
            index: ".eagle-eyes",
            type: "alarms",
            body: {
              "size": 1000
            }
          }).then(body => {
            var alarms = [];
            body.hits.hits.forEach(function(item) {
              alarms.push(extend({
                _id: item["_id"],
                name: item["_source"].name,
                description: item["_source"].description,
                version: item["_source"].version
              }, JSON.parse(item["_source"].configJSON)));
            });

            // Remove delayed alarms
            if (delayed.hits.hits.length > 0) {
              // If has an item with _id = ALL dont check metrics for alarms
              if (_.filter(delayed.hits.hits, ['_id', 'ALL']).length == 0) {
                alarms = _.pullAllWith(alarms, delayed.hits.hits, function(a, b) {
                  return b["_id"] === a["_id"] || b["_id"] === a.group
                });
              } else {
                alarms = []
              }
            }

            var now = moment().tz("America/Sao_Paulo");
						// Remove alarms that in on outage window
						_.filter(alarms, function(alarm) {
							if (alarm.outage) {
                var start = moment.tz(`${now.format("YYYY-MM-DD")}T${alarm.outage.start}`, "America/Sao_Paulo");
								var end = moment.tz(`${now.format("YYYY-MM-DD")}T${alarm.outage.end}`, "America/Sao_Paulo");

                // Check if need to adjust the day
                if (start.isAfter(end)) {
                  if (end.isBefore(now)) {
                    end = end.add(1, 'day')
                  } else {
                    start = start.subtract(1, 'day')
                  }

                }
                console.log(`Check outage window now: ${now.format("YYYY-MM-DDTHH:mm")} start: ${start.format("YYYY-MM-DDTHH:mm")} end: ${end.format("YYYY-MM-DDTHH:mm")}`)
								return !now.isBetween(start, end)
							}
							return true;
						})

            resolve({
              loadTime: process.hrtime(),
              alarms: alarms
            });
          }, err => {
            reject(error.message);
          })
        }, err => {
          reject(error.message);
        });
      } else {
        resolve(self.config);
      }
    });
  }

  self._sendAlarm = function(alarm) {
    var template = function(tpl, args) {
      var keys = Object.keys(args),
        fn = new Function(...keys,
          'return `' + tpl.replace(/`/g, '\\`') + '`');
      return fn(...keys.map(x => args[x]));
    };

    return new Promise((resolve, reject) => {
      request.post(extend(true, {
        "json": {
          "payload": {
            "summary": `${alarm.alarm.name} - ${template(alarm.alarm.description, alarm.alarm)}`,
            "timestamp": moment().subtract(1, 'minutes').format("YYYY-MM-DDTHH:mm:ss.SSSZ"),
            "source": alarm.alarm.name,
            "severity": "critical",
            "group": alarm.alarm.group,
            "class": alarm.alarm.type
          },
          "event_action": "trigger"
        }
      }, this.pagerduty)).then(body => {
        resolve(body);
      }).catch(err => {
        reject(err);
      });
    });
  }

  self._checkResponseTime = function(options) {
    var rangeFilter = {};
    rangeFilter[options.timestamp] = {
      "gte": moment().subtract(1, 'minutes').subtract(options.period.value, options.period.type).format('x'),
      "lte": moment().subtract(1, 'minutes').format('x'),
      "format": "epoch_millis"
    };

    return new Promise((resolve, reject) => {
      self.targetES.search({
        index: options.index,
        body: {
          "size": 0,
          "query": {
            "bool": {
              "must": [{
                  "query_string": {
                    "analyze_wildcard": true,
                    "query": options.query
                  }
                },
                {
                  "range": rangeFilter
                }
              ],
              "must_not": []
            }
          },
          "_source": {
            "excludes": []
          },
          "aggs": {
            "timeline": {
              "date_histogram": {
                "field": options.timestamp,
                "interval": "1m",
                "time_zone": "America/Sao_Paulo",
                "min_doc_count": 1
              },
              "aggs": {
                "responseTime": {
                  "extended_stats": {
                    "field": "value"
                  }
                }
              }
            }
          }
        }
      }).then(function(body) {
        var details = [];
        if (body && body.aggregations) {
          body.aggregations.timeline.buckets.forEach(function(item) {
            if (item.responseTime["std_deviation"] > options.threshold["std_deviation"]) {
              details.push({
                "metric": "std_deviation",
                "value": Math.round(item.responseTime["std_deviation"])
              });
            } else if (item.responseTime["avg"] > options.threshold["avg"]) {
              details.push({
                "metric": "avg",
                "value": Math.round(item.responseTime["avg"])
              });
            } else if (item.responseTime["max"] > options.threshold["max"]) {
              details.push({
                "metric": "max",
                "value": Math.round(item.responseTime["max"])
              });
            }
          });

          resolve({
            alarm: options,
            details: details,
            send: details.length >= options.period.value
          });
        } else {
          resolve({
            alarm: options,
            details: details,
            send: false
          });
        }
      }, function(error) {
        console.log(error);
        reject(error.message);
      });
    });
  }

  self._checkErrorRate = function(options) {
    // TODO performance optimization
    // .es(index=${options.index}, q='${options.queryErrors}', metric=${options.metric}, timefield=${options.timestamp}).divide(.es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp})).multiply(100).label('CURRENT'), .es(index=${options.index}, q='_type: /osb-error-.*/${options.queryErrors}', metric=${options.metric}, timefield=${options.timestamp}, offset=-1w).divide(.es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp}, offset=-1w)).multiply(100).label('OFFSET'), .es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp}).divide(.es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp}, offset=-15m)).subtract(1).multiply(100).label('REQUEST_RATE')
    return new Promise((resolve, reject) => {
      request.post(extend({
        json: {
          "sheet": [
            `.es(index=${options.index}, q='${options.queryErrors}', metric=${options.metric}, timefield=${options.timestamp}).divide(.es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp})).multiply(100).label('CURRENT')`
          ],
          "extended": {
            "es": {
              "filter": {
                "bool": {
                  "must": [{
                    "query_string": {
                      "analyze_wildcard": true,
                      "query": options.query
                    }
                  }]
                }
              }
            }
          },
          "time": {
            "from": moment().subtract(2, 'minutes').subtract(options.period.value, options.period.type).format("YYYY-MM-DDTHH:mm:ss.SSSZ"),
            "interval": "1m",
            "mode": "absolute",
            "timezone": "America/Sao_Paulo",
            "to": moment().subtract(1, 'minutes').format("YYYY-MM-DDTHH:mm:ss.SSSZ")
          }
        }
      }, this.timelion)).then(body => {
        var data = {
          "current": _.fromPairs(_.find(body.sheet[0].list, ['label', 'CURRENT']).data)
          // "offset" : _.fromPairs(_.find(body.sheet[0].list, ['label', 'OFFSET']).data),
          // "requestRate" : _.fromPairs(_.find(body.sheet[0].list, ['label', 'REQUEST_RATE']).data)
        };

        var details = [];
        Object.keys(data.current).forEach(function(item) {
          if (data.current[item] > options.threshold.rate) {
            details.push({
              "metric": "errorRate",
              "value": Math.round(data.current[item])
            });
          }
        });

        resolve({
          alarm: options,
          details: details,
          send: details.length >= options.period.value
        });
      }).catch(error => {
        reject(error.message);
      });
    });
  }

  self._checkoutVariation = function(options) {
    // TODO performance optimization
    // .es(index=${options.index}, q='${options.queryErrors}', metric=${options.metric}, timefield=${options.timestamp}).divide(.es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp})).multiply(100).label('CURRENT'), .es(index=${options.index}, q='_type: /osb-error-.*/${options.queryErrors}', metric=${options.metric}, timefield=${options.timestamp}, offset=-1w).divide(.es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp}, offset=-1w)).multiply(100).label('OFFSET'), .es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp}).divide(.es(index=${options.index}, q='*', metric=${options.metric}, timefield=${options.timestamp}, offset=-15m)).subtract(1).multiply(100).label('REQUEST_RATE')
    return new Promise((resolve, reject) => {
      request.post(extend({
        "json": {
          "sheet": [
            `.es(index=${options.index}, q='${options.query}', metric=${options.metric}, timefield=${options.timestamp}).divide(.es(index=${options.index}, q='${options.query}', metric=${options.metric}, timefield=${options.timestamp}, offset=-1w).sum(.es(index=${options.index}, q='${options.query}', metric=${options.metric}, timefield=${options.timestamp}, offset=-2w)).sum(.es(index=${options.index}, q='${options.query}', metric=${options.metric}, timefield=${options.timestamp}, offset=-3w)).sum(.es(index=${options.index}, q='${options.query}', metric=${options.metric}, timefield=${options.timestamp}, offset=-4w)).divide(4)).subtract(1).multiply(100).label('CURRENT')`
          ],
          "extended": {
            "es": {
              "filter": {
                "bool": {
                  "must": [{
                    "query_string": {
                      "analyze_wildcard": true,
                      "query": "*"
                    }
                  }]
                }
              }
            }
          },
          "time": {
            "from": moment().subtract(2, 'minutes').subtract((options.period.value*6), options.period.type).format("YYYY-MM-DDTHH:mm:ss.SSSZ"),
            "interval": `${options.period.value}${(options.period.type === 'minutes' ? 'm' : (options.period.type === 'hour' ? 'h' : 's'))}`,
            "mode": "absolute",
            "timezone": "America/Sao_Paulo",
            "to": moment().subtract(1, 'minutes').format("YYYY-MM-DDTHH:mm:ss.SSSZ")
          }
        }
      }, this.timelion)).then(body => {
        var data = {
          "current": _.fromPairs([_.head(_.takeRight(_.find(body.sheet[0].list, ['label', 'CURRENT']).data,2))])
        };

        var details = [];
        Object.keys(data.current).forEach(function(item) {
					if ((options.threshold.rate > 0 && data.current[item] > options.threshold.rate) || (options.threshold.rate < 0 && data.current[item] < options.threshold.rate)) {
						details.push({
							"metric": "checkoutVariation",
							"value": Math.round(data.current[item])
						});
					}
        });

        resolve({
          alarm: options,
          details: details,
          send: details.length > 0
        });
      }).catch(error => {
        reject(error.message);
      });
    });
  }
}

eagleeyes.prototype.process = function(options) {
  var self = this
  return new Promise((resolve, reject) => {
    this._init().then(config => {
      PromiseBB.map(config.alarms, function(item) {
        // set defaults
        item = extend({
          "timestamp": "@timestamp",
          "queryErrors": "*",
          "metric": "count"
        }, item);

        if (options && options.testList && options.testList.indexOf(item["_id"]) > -1) {
          return new Promise(resolve => {
            resolve({
              alarm: item,
              details: {},
              send: true
            });
          });
        } else {
          if ("RESPONSE_TIME" === item.type) {
            return self._checkResponseTime(item);
          } else if ("ERROR_OCCURRENCES" === item.type) {
            return self._checkErrorRate(item);
          } else if ("CHECKOUT_VARIATION" === item.type) {
            return self._checkoutVariation(item);
          }
        }
      }, {
        concurrency: self.options.concurrency
      }).then(results => {
        var alarms = _.filter(results, 'send');
        if (alarms.length > 0) {
          alarms.forEach(alarm => {
            this._sendAlarm(alarm).then(body => {
              console.log('Alarm was sent... ' + JSON.stringify(body));
            }).catch(err => {
              console.log('Was identified an error during the triggering of an alarm... ' + JSON.stringify(err));
            });
          });

          // Set delay
          var body = []
          alarms.forEach(function(item) {
            body.push({
              index: {
                _index: ".eagle-eyes-control",
                _type: "delayed-alarms",
                _id: item.alarm["_id"]
              }
            });
            body.push({
              "timeFrame": {
                "gte": moment().format('x'),
                "lte": moment().add(item.alarm.period.value, item.alarm.period.type).format('x')
              }
            });
          });

          // Save delay
          this.sourceES.bulk({
            body: body
          }, function(error, response) {
            console.log(JSON.stringify(response));
            if (error) {
              console.log(error);
            }
          });
        }
        resolve(results);
      }).catch(err => {
        reject(err);
      });
    }).catch(err => {
      reject(err);
    });
  });
}

module.exports = eagleeyes
