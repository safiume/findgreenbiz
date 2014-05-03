var mytable = 'grnbiz';
var rowheader = ('COMPANY TEXT, ADDRESS TEXT, SECTOR TEXT, STATUS TEXT, LAT INT, LONG INT, CKADDR TEXT');

var sqlite3 = require('sqlite3');
var mod_vasync = require('vasync');
var geocoder = require ('geocoder');
var Parser = require('node-dbf');

var PARSER = new Parser('SanFranciscoGreenBusinesses.dbf');
var db = new sqlite3.Database('sfgrnbizsql.db');

function
worker_func(task, next)
{
	var cb1 = function (err, res) {
		
		console.log(res);
		if (res.results[0].geometry == undefined) {
			return cb1 ( new Error ("No Lat/Long location found.") );
		}
		var data = res.results[0];
		var geodata = data.geometry;

		var query = 'INSERT INTO ' +mytable +' VALUES ($company, $address, ' +
		    '$sector, $status, $lat, $long, $chkAddress)';
		var params = {
			$company: task.company,
			$address: task.address,
			$sector: task.sector,
			$status: task.status,
			$lat: geodata.location.lat,
			$long: geodata.location.lng,
			$chkAddress: data.formatted_address
		};
	/*
	 * Note: we pass in "next", the function we need to call on
	 * completion of this queue task, as the callback for
	 * db.run -- db.run() will then call next for us when _it_
	 * is finished inserting into the database.
	 */
		db.run(query, params, next);
	};

	var opts = ("google",{"key":"AIzaSyAlL45KShTCX5qg6z0LVJ4ZR2RW5W0VVz4"});
	geocoder.geocode(task.address, cb1, opts);
}

var QUEUE = mod_vasync.queuev({
	concurrency: 1,
	worker: worker_func
});

QUEUE.on('end', function () {
	console.error('Queue work complete!');
});

PARSER.on('record', function (rec) {
	QUEUE.push({
		address: rec.ADDRESS, 
		company: rec.COMPANY,
		sector: rec.SECTOR,
		status: rec.STATUS
	});
});

PARSER.on('end', function () {
	QUEUE.close();
});


db.run("CREATE TABLE IF NOT EXISTS " + mytable + " (" +rowheader +")", function (err) {
	if (err) { 
		console.log(err.stack);
		process.exit(1); 
	} else { 
		PARSER.parse(); 
	}
});

