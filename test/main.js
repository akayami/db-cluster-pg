var adapter = require('../index.js');

var conf = {
	user: 'tomasz',
	database: 'test'
}

describe('Postgres db-cluster adapter', function() {
	it('Needs to prepare cluster adapter', function(done) {
		try {
			adapter.getPool(require('pg'), conf);
			done();
		} catch(e) {
			done(e);
		}
	})
	it('Needs to aquire connection', function(done) {
		try {
			var pool = adapter.getPool(require('pg'), conf);
			pool.getConnection(function(err, conn) {
				if(err) {
					conn.release();
					done(err)
				} else {
					conn.release();
					done();
				}
			})
		} catch(e) {
			done(e);
		}
	})
});
