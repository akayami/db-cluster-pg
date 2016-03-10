var adapter = require('../index.js');

var conf = {
	user: 'tomasz',
	database: 'test'
}

var c;

describe('Query Parser', function() {

	before(function(done) {
		try {
			var pool = adapter.getPool(require('pg'), conf);
			pool.getConnection(function(err, conn) {
				if(err) {
					done(err)
				} else {
					c = conn;
					done();
				}
			})
		} catch(e) {
			done(e);
		}
	})

	after(function(done) {
		c.release();
		done();
	})

	it('Needs to parse the query with an identifer', function(done) {
		var out = c.parse('SELECT * FROM ??', ['table']);
		if(out == 'SELECT * FROM "table"') {
			done();
		} else {
			done(new Error('Invalid Response: ' + out));
		}
	})

	it('Needs to parse the query with two identifers', function(done) {
		var out = c.parse('SELECT * FROM ??.??', ['schema', 'table']);
		if(out == 'SELECT * FROM "schema"."table"') {
			done();
		} else {
			done(new Error('Invalid Response: ' + out));
		}
	})

	it('Needs to parse the query with a value', function(done) {
		var out = c.parse('SELECT * FROM table where value = ?', ['value']);
		if(out == `SELECT * FROM table where value = 'value'`) {
			done();
		} else {
			done(new Error('Invalid Response: ' + out));
		}
	})

	it('Needs to parse the query with two identfiers and two values', function(done) {
		var out = c.parse('SELECT * FROM ??.?? WHERE value1 = ? AND value2 = ?', ['schema', 'table', 'value1', 'value2']);
		if(out == `SELECT * FROM "schema"."table" WHERE value1 = 'value1' AND value2 = 'value2'`) {
			done();
		} else {
			done(new Error('Invalid Response: ' + out));
		}
	})
})
