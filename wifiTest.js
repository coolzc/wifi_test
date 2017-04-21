const request = require('request')
const moment = require('moment-timezone')
const TIMEZONE_SHANGHAI = 'Asia/Shanghai'
const _ = require('underscore')
const MongoClient = require('mongodb').MongoClient
const logger = require('winston')
const async = require('async')

const center = {}

const openIds = ['oXPlvwnUoo6cHZC89sRP84cK6oTw',
  'oXPlvwt4syGammDpWUuTkL3eUO7o',
  'oXPlvwjaeiycQB6FmpGeVovXa5Mc',
  'oXPlvwjaeiycQB6FmpGeVovXa5Mc',
  'oXPlvwtgNTHcr2gk4xJYNz1m-Qwk',
  'oXPlvwrODdscXvjhVjfftub2ImX8',
  'oXPlvwoI2cnEkSprAAwBQgDm_PNg',
  'oXPlvwiObc1IifzBYzoQlN4pPfYQ',
  'oXPlvwog0zqAM5B7pCm3YcQtgti0'
]

const url = 'http://127.0.0.1:3000/wechat/wx8d560f3d3922e0ce?signature=858a6519cc0665695c606fc3240eacd5c533c614&echostr=8276881011589770367&timestamp=1490758778&nonce=1053605085'

function getFormatDate(mm, dd) {
  return new Date(2017, mm, dd)
}

function getXmlText(openId, createdTime, type) {
  let myXMLText = ''
  if (type && type === 'wifi') {
    myXMLText = `<xml> <ToUserName><![CDATA[toUser]]></ToUserName> <FromUserName><![CDATA[${openId}]]></FromUserName> <CreateTime>${createdTime}</CreateTime> <MsgType><![CDATA[event]]></MsgType> <Event><![CDATA[WifiConnected]]></Event> <ConnectTime>0</ConnectTime><ExpireTime>0</ExpireTime><VendorId>![CDATA[3001224419]]</VendorId><ShopId>![CDATA[PlaceId]]</ShopId><DeviceNo>![CDATA[DeviceNo]]</DeviceNo></xml>`
  } else if (type && type === 'subscribe') {
    myXMLText = `<xml><ToUserName><![CDATA[toUser]]></ToUserName><FromUserName><![CDATA[${openId}]]></FromUserName><CreateTime>${createdTime}</CreateTime><MsgType><![CDATA[event]]></MsgType><Event><![CDATA[subscribe]]></Event></xml>`
  } else if (type && type === 'unsubscribe') {
    myXMLText = `<xml><ToUserName><![CDATA[toUser]]></ToUserName><FromUserName><![CDATA[${openId}]]></FromUserName><CreateTime>${createdTime}</CreateTime><MsgType><![CDATA[event]]></MsgType><Event><![CDATA[unsubscribe]]></Event></xml>`
  }
  return myXMLText
}

function requestPostEvent(xmlText) {
  request({
    url: url,
    method: "POST",
    headers: {
      "content-type": "application/xml",
    },
    body: xmlText,
  }, function (err, response, body) {
    if (err) logger.error(err)
  })
}

function sendWifiEvent(openId, createdTime) {
  const xmlText = getXmlText(openId, createdTime, 'wifi')
  requestPostEvent(xmlText)
}

function sendSubscribeEvent(openId, createdTime, type) {
  const xmlText = getXmlText(openId, createdTime, type)
  requestPostEvent(xmlText)
}

function initMongo() {
  return (cb) => {
    const ds = {
      "host": "127.0.0.1",
      "port": 27017,
      "database": "wechatnow"
    }
    const url = `mongodb://${ds.host}:${ds.port}/${ds.database}`
    MongoClient.connect(url, function (err, db) {
      center.mongo = db
      if (err) return logger.error('mongo init: %s', err)
      console.log('mongo')
      cb(null, center)
    })
  }
}


function saveWifiDailyDatcube(center, subtractDays) {
  const WifiDailyDatcube = center.mongo.collection('WifiDailyDatacube')
  const WifiUserDailyEvent = center.mongo.collection('WifiUserDailyEvent')
  const Follower = center.mongo.collection('follower')
  const today = new moment().tz(TIMEZONE_SHANGHAI)
  const yesterdayStr = today.clone().subtract(subtractDays, 'days').startOf('day').format('YYYY-MM-DD')

  WifiUserDailyEvent.aggregate([{
    $match: {
      dateStr: yesterdayStr,
    },
  }, {
    $group: {
      _id: '$state',
      count: {
        $sum: 1,
      },
    },
  }], (err, result) => {
    if (err) return logger.error('WifiUserDailyEvent.aggregate error: %s', err)
    const wifiDailyDatacube = {
      dateStr: yesterdayStr,
      newFollowers: 0,
      previousFollowers: 0,
      nonFollowers: 0,
      connectedUsers: 0,
    }
    result.forEach((element) => {
      wifiDailyDatacube[element._id + 's'] = element.count
    })

    wifiDailyDatacube.connectedUsers = wifiDailyDatacube.newFollowers + wifiDailyDatacube.previousFollowers + wifiDailyDatacube.nonFollowers
    if(wifiDailyDatacube.connectedUsers !== 0) {console.log(yesterdayStr)}

    WifiDailyDatcube.insert(wifiDailyDatacube, (err) => {
      if (err) return logger.error('WifiUserDailyEvent.aggregate create WifiDailyDatcub error: %s', err)
    })

    //get daily new followers then make isWifiUsers true in follower collection
    WifiUserDailyEvent.find({
      state: 'newFollower',
    }).toArray((err, wifiUserDailyEvents) => {
      if (err) return logger.error('WifiUserDailyEvent find newFollowers error: %s', err)
      if (wifiUserDailyEvents.length === 0) return
      const openIds = []
      _.forEach(wifiUserDailyEvents, (value) => {
        openIds.push(value.openId)
      })

      Follower.updateMany({
        openId: {
          $in: openIds,
        },
      }, {
        $set: {
          isWifiUser: true,
        },
      }, (err) => {
        if (err) return logger.error('Follower updateMany error: %s', err)
      })

    })

  })

}

/*
 *
 * wechat api will reach max api daily quota limit, follower to pull data from wechat server
 *
 */
function simulateWifiAndSubEvents() {
  for (let i = 0; i < 1000; i++) {
    const openId = openIds[_.random(0, openIds.length - 1)]
    const createdTime = getFormatDate(_.random(0, 3), _.random(0, 30)).getTime() / 1000
    if (i % 2 === 0) sendWifiEvent(openId, createdTime)
    if (i % 5 === 0) sendSubscribeEvent(openId, createdTime, 'subscribe')
    if (i % 7 === 0) sendSubscribeEvent(openId, createdTime, 'unsubscribe')
  }
}

function generateWifiDailyDatacube() {
    const WifiDailyDatacube  = center.mongo.collection('WifiDailyDatacube')
    const createdTime = getFormatDate(_.random(0, 3), _.random(0, 30)).getTime()
    const dateStr = moment(createdTime).tz('Asia/Shanghai').format('YYYY-MM-DD')
    const wifiDailyDatacube = {
      dateStr: dateStr,
      newFollowers: _.random(0,100),
      previousFollowers: _.random(0,200),
      nonFollowers:  _.random(0,300),
      connectedUsers: 0,
    }
    wifiDailyDatacube.connectedUsers = wifiDailyDatacube.newFollowers + wifiDailyDatacube.previousFollowers + wifiDailyDatacube.nonFollowers

    WifiDailyDatacube.insert(wifiDailyDatacube, (err) => {
      if (err) return logger.error('WifiUserDailyEvent.aggregate create WifiDailyDatcub error: %s', err)
    })
}

function generateWifiFollowersData() {
  const Follower = center.mongo.collection('follower')
  const cursor = Follower.find({})
  let i = 0
  cursor.forEach((follower) => {
    i ++
    if(i % 7 === 0) {
      Follower.updateOne({_id: follower._id}, {$set: {isWifiUser: true}})
    }
  })
}

async.series([
  initMongo(),
], (err) => {
  //for (let k = 0; k < 90; k++) {
  //  saveWifiDailyDatcube(center, k)
  //}
  for (let k = 0; k < 90; k++) {
    generateWifiDailyDatacube()
  }
  generateWifiFollowersData()
  console.log('ok, done')
})
