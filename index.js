"use strict";

var engine = 'pg';

class Result {
	constructor(raw, pkReferenceName) {
		this.raw = raw;
		this.referenceName = pkReferenceName;
		this.insertId = (this.referenceName ? this.rows()[0][this.referenceName] : null);
		this.length = (this.raw && this.raw.rows ? this.raw.rows.length : null);
	}

	rows() {
		return this.raw.rows;
	}

	raw() {
		return this.raw;
	}

	count() {
		return this.raw.rowCount;
	}

	fields() {
		return this.raw.fields;
	}
}

class Connection {

	constructor(raw, done) {
		this.connection = raw;
		this.done = done;
		this.map = {};
		this.engine = engine
	}

	init(cb) {
		this.connection.query("SELECT pgn.nspname schema_name, pgc.relname table_name, pga.attname column_name, format_type(pga.atttypid, pga.atttypmod) AS data_type FROM pg_index pgi JOIN pg_attribute pga ON (pga.attrelid = pgi.indrelid AND pga.attnum = ANY(pgi.indkey)) JOIN pg_class pgc ON (pgc.oid= pgi.indrelid AND pgc.relkind='r') JOIN pg_namespace pgn ON (pgc.relnamespace=pgn.oid) WHERE  pgi.indisprimary", function(err, result) {
			if(err) {
				return cb(err);
			}
			for(var x = 0; x < result.rows.length; x++) {
				this.map[result.rows[x]['table_name']] = result.rows[x]['column_name'];
			}
			cb(null);
		}.bind(this));
	}

	escape(phs, value) {
		switch (phs) {
			case '??':
				return this.escapeIdentifier(value);
			case '?':
				return this.escapeValue(value);
		}
	}

	escapeIdentifier(string) {
		return '"' + string + '"';
	}

	escapeValue(string) {
		return (string != null ? "'" + string + "'" : null);
	}

	parse(sql, options) {
		if (options) {
			var match = /\?+/g;
			var myArray;
			var matches = [];
			while ((myArray = match.exec(sql)) !== null) {
				matches.push([match.lastIndex, myArray[0]]);
			}
			if (matches.length > options.length) {
				throw new Error('Missing placeholders');
			}
			for (var x = 0; x < options.length; x++) {
				if(matches[x] && matches[x][1]) {
					sql = sql.replace(matches[x][1], this.escape(matches[x][1], options[x]));
				}
			}
		}
		return sql;
	}

	insert(table, data, options, cb) {
		if(!cb) {
			cb = options;
			options = {};
		}
		var fields = Object.keys(data);
		var values = [];
		var dataArray = [table];
		for (var f = 0; f < fields.length; f++) {
			if(data[fields[f]] !== undefined) {
				dataArray.push(fields[f]);
			}
		}
		var fieldPh = [];
		var valuePh = [];
		fields.forEach(function(field) {
			if(data[field] !== undefined) {
				fieldPh.push('??');
				valuePh.push('?')
				dataArray.push(data[field]);
			}
		});

		if(this.map && this.map[table]) {
			dataArray.push(this.map[table]);
			this.query('INSERT INTO ?? (' + fieldPh.join(', ') + ') values (' + valuePh.join(', ') + ') RETURNING ??', dataArray, {pkColumnName: this.map[table]}, cb);
		} else {
			this.query('INSERT INTO ?? (' + fieldPh.join(', ') + ') values (' + valuePh.join(', ') + ')', dataArray, cb);
		}
	};

	update(table, data, condition, cond_params, cb) {
		var fields = Object.keys(data);
		var values = [];
		var dataArray = [table];
		var fieldPh = [];
		fields.forEach(function(field, f) {
			if(data[field] !== undefined) {
				fieldPh.push('??=?');
				dataArray.push(fields[f]);
				dataArray.push(data[field]);
			}
		});
		cond_params.forEach(function(param) {
			dataArray.push(param);
		})
		this.query('UPDATE ?? SET ' + fieldPh.join(', ') + ' WHERE ' + condition, dataArray, cb);
	};

	query(sql, dataArray, options, cb) {
		if (!cb) {
			if(!options) {
				cb = dataArray;
				dataArray = null;
			} else {
				cb = options;
				options = {};
			}
		}
		// if(typeof(sql) == 'object') {
		// 	options['nestTables'] = sql.nestTables;
		// 	sql = sql.sql
		// }
		var sql = this.parse(sql, dataArray);
		this.connection.query(sql, function(err, result) {
			if(err) {
				err.sql = this.sql;
			}
			//console.log(options);
			cb(err, new Result(result, ((options && options['pkColumnName']) ? options['pkColumnName'] : null)));
		}.bind({sql: sql}));
	}

	beginTransaction(cb, options) {
		this.query('START TRANSACTION', cb)
	}

	rollback(cb) {
		this.query('ROLLBACK', cb);
	}

	commit(cb) {
		this.query('COMMIT', cb);
	}

	release(cb) {
		this.done();
		if (cb) {
			cb();
		}
	}
};

class Pool {
	constructor(driver, config) {
		this.driver = driver;
		this.config = config;
		this.map = {};
	}

	getConnection(cb) {
		this.driver.connect(this.config, function(err, client, done) {
			if (err) {
				return cb(err);
			}
			var conn = new Connection(client, done);
			conn.init(function(err) {
				if(err) {
					return cb(err);
				}
				cb(null, conn);
			})
		}.bind(this))
	}

	end(cb) {
		this.driver.end();
		cb();
	}
};

module.exports = {
	engine: engine,
	getPool: function(driver, object) {
		return new Pool(driver, object);
	},
	getDriver: function() {
		return require('pg')
	}
};
