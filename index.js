var mongoose = require('mongoose');
var fs = require('fs');
var csv = require('csv-stream');
var murmurHash = require('murmurhash-native').murmurHash32;
var Throttle = require('throttle');
var CLI = require('clui');
var clc = require('cli-color');
var b32decode = require('./base32.js');

mongoose.connect('mongodb://localhost/cjdns');

var KeySechma = new mongoose.Schema({
    pub: Buffer,
    priv: Buffer,
    ip: String,
    bloom: { type: Number, min: -2147483647, max: 2147483647 }
});

var Key = mongoose.model('Key', KeySechma);

var readStream = fs.createReadStream('keys');
var rowStream = csv.createStream(options = {
	delimiter : ' ',
	endLine : '\n',  
	columns : ['priv', 'ip', 'pub']
});

var sent = 0;
var open = 0;

rowStream.on('data', (data) => {
	var ip = data.ip.replace(/:/g, '')
	var priv = new Buffer(data.priv, 'hex')
	if (data.pub.substring(data.pub.length-2) !== ".k") { throw new Error("key does not end with .k"); }
    var pub = b32decode(data.pub.substring(0, data.pub.length-2));

    var bloom = murmurHash(ip)
    var key = new Key({pub: pub, priv: priv, ip: ip, bloom: bloom});

    key.save((err) => {
        if (err) {
            console.log(bloom)
            throw err;
        }
        open--;
        sent++;
    })
    open++;

});
rowStream.on('finish', () => {
    process.exit();
})
readStream.pipe(rowStream);

var last = 0
var avg = 0
console.log('\033[2J');

setInterval(() => {
    var Line = CLI.Line;
    var LineBuffer = CLI.LineBuffer;

    var outputBuffer = new LineBuffer({
        x: 1,
        y: 1,
        width: 'console',
        height: 'console'
    });

    var title = new Line(outputBuffer)
    new Line(outputBuffer)
    .column('Queue:', 20, [clc.cyan])
    .column(CLI.Gauge(open, 1000, 20, 300, open))
    .store();
    var rate = sent - last;
    last = sent;

    avg = avg * 9/10 + rate /10
    new Line(outputBuffer)
    .column('Rate:', 20, [clc.cyan])
    .column(avg.toPrecision(4), 6, [clc.white.bold])
    .column("[" + rate.toString() +"]", 6, [clc.white.bold])
    .column("per second", 10, [clc.white])
    .store();
    var time = (12e6-sent)/avg/60;
    var eta = new Date();
    eta.setMinutes(eta.getMinutes() + time);
}, 1000);

