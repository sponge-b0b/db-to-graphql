var oracledb = require('oracledb');
var _ = require("lodash");
var vm = require('vm');

oracledb.queueTimeout = 0;
oracledb.poolTimeout = 0;

var maxRows = 200;

var setMaxRows = function (val) {
	maxRows = val;
};

/*
IN:
__dbConnection = { user: "", password: "", connectString: "host:port/sid" }
dbType = "" // "Oracle"
selectedSchemas = ['', '', '', ...]
OUT:
{
  root: {},
  schema: graphqlSchema
}
*/
var generateGraphQL = function (__dbConnection, dbType, selectedSchemas = null) {
	return new Promise(function (resolve) {
		if (dbType === 'Oracle') {
			getOracleORM(__dbConnection, selectedSchemas)
				.then(function (dbSchema) {
					var root = {};
					const sandbox = { root: root, dbSchema: dbSchema };
					var code;
					vm.createContext(sandbox);

					var graphqlSchema = '';
					var schemaQuery = 'type Query {';
					var selectedTables;

					if (selectedSchemas != null) {
						selectedTables = _.filter(dbSchema, function (obj) {
							return _.includes(selectedSchemas, obj.name);
						});
					} else {
						selectedTables = dbSchema;
					}

					console.log();
					console.log('Database Schema');
					console.log('-----------------------------------------------------------------------------------------------------------------------');
					console.log(dbSchema);
					console.log();
					console.log('Database Tables');
					console.log('-----------------------------------------------------------------------------------------------------------------------');
					console.log(selectedTables);

					// graphqlSchema
					_.forEach(selectedTables, function (tables) {
						_.forEach(tables.Tables, function (table) {
							schemaQuery += `${table.owner}_${table.name}(`;
							graphqlSchema += `type ${table.owner}_${table.name} {`;
							var isFirst = true;
							Object.keys(table).forEach(function (key) {
								if (key != 'DB_CONNECTION' && key != 'DB_TYPE' && key != 'selectColumns' && key != 'selectColumnsFormatted' && typeof table[key] == 'object') {
									graphqlSchema += `${table[key].name}: ${translateJStoGraphQLType(table[key].dataType)}`;
									if (!isFirst) {
										schemaQuery += ', ';
									}
									schemaQuery += table[key].name + ': ' + translateJStoGraphQLType(table[key].dataType) + ' = null';
									isFirst = false;
								}
							});
							graphqlSchema += `}`;
							schemaQuery += '): [' + table.owner + '_' + table.name + ']';
						});
					});
					schemaQuery += `}`;
					graphqlSchema += `${schemaQuery}`;

					// root
					var rootFunction = '';
					_.forEach(selectedTables, function (tables) {
						_.forEach(tables.Tables, function (table) {
							rootFunction = 'root.' + table.owner + '_' + table.name + ' = function({' + table.selectColumnsFormatted.join() + '}) { let whereColumns = [];';
							_.forEach(table.selectColumnsFormatted, function (column) {
								rootFunction += ' if (' + column + ' != null) { whereColumns.push({';
								rootFunction += ' column: dbSchema.' + table.owner + '.Tables.' + table.name + '.' + column + ',';
								rootFunction += ' value: ' + column + ',';
								rootFunction += ' operation: \'AND\' }); }';
							});
							rootFunction += ' if (whereColumns.length > 0) {';
							rootFunction += ' return dbSchema.' + table.owner + '.Tables.' + table.name + '.findAll(whereColumns).then(function(res) { return res; }).catch((err) => { console.log(err); }); }';
							rootFunction += ' else {';
							rootFunction += ' return dbSchema.' + table.owner + '.Tables.' + table.name + '.findAll().then(function(res) { return res; }).catch((err) => { console.log(err); }); } };';

							vm.runInContext(rootFunction, sandbox);
						});
					});

					resolve({ root: root, graphqlSchema: graphqlSchema });
				})
				.catch((err) => {
					console.log(err);
				});
		}
	});
};

var findAll = function (whereColumns = null) {
	var data = getData(this.DB_CONNECTION, { name: this.name, owner: this.owner }, this.selectColumns, whereColumns)
		.then(function (queryData) {
			return queryData;
		})
		.catch((err) => {
			console.log(err);
		});

	return new Promise(function (resolve) {
		data
			.then(function (val) {
				resolve(val);
			});
	});
};

var verifyDatabaseConnection = function (__dbConnection) {
	if (__dbConnection == null || __dbConnection.user == null || __dbConnection.password == null || __dbConnection.connectString == null) {
		return false;
	}
	return true;
};

var translateJStoGraphQLType = function (type) {
	switch (type) {
		case 'FLOAT':
			return 'Float';
		case 'NUMBER':
			return 'Float';
		default:
			return 'String';
	}
};

var getData = function (__dbConnection, fromTable, selectColumns, whereColumns = null) {
	return new Promise(function (resolve, reject) {
		if (!verifyDatabaseConnection(__dbConnection)) {
			reject();
		}
		if (fromTable == null || fromTable.name == null || fromTable.owner == null || selectColumns == null || selectColumns.length === 0) {
			reject();
		}
		if (whereColumns != null && whereColumns.length === 0) {
			reject();
		}

		oracledb.getConnection({
			user: __dbConnection.user,
			password: __dbConnection.password,
			connectString: __dbConnection.connectString
		},
			function (err, connection) {
				if (err) {
					console.log(err);
					connection.close();
					reject();
				}

				var bindvars = {
					cursor: { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
				};

				var query = 'BEGIN OPEN :cursor FOR SELECT ';
				for (let i = 0; i < selectColumns.length; i++) {
					query += '' + selectColumns[i];
					if (selectColumns[i + 1] != null) {
						query += ',';
					}
				}

				query += ' FROM ' + fromTable.owner + '.' + fromTable.name;

				const sandbox = { bindvars: bindvars, whereColumns: whereColumns };
				var code = '';
				vm.createContext(sandbox);
				if (whereColumns != null) {
					for (let i = 0; i < whereColumns.length; i++) {
						if (i === 0) {
							query += ' WHERE (ROWNUM <= ' + maxRows + ') AND (';
						} else {
							query += ' ' + whereColumns[i].operation;
						}
						if (whereColumns[i].column.dataType === 'STRING') {
							query += ' ' + whereColumns[i].column.name + ' LIKE :' + whereColumns[i].column.name + i;
						} else {
							query += ' ' + whereColumns[i].column.name + ' = :' + whereColumns[i].column.name + i;
						}
						code = 'bindvars.' + whereColumns[i].column.name + i + ' = whereColumns[' + i + '].value;';
						vm.runInContext(code, sandbox);
					}
					query += ')';
				} else {
					query += ' WHERE (ROWNUM <= ' + maxRows + ')';
				}
				bindvars = sandbox.bindvars;
				query += "; END;";

				connection.execute(query, bindvars, { prefetchRows: 400 }, function (err, result) {
					var cursor;
					var stream;
					var sandbox2 = { resRow: {}, dbData: [] };
					vm.createContext(sandbox2);
					var code2 = '';

					if (err) {
						console.log(err);
						connection.close();
						reject();
					}

					cursor = result.outBinds.cursor;
					stream = cursor.toQueryStream();

					stream.on('data', function (row) {
						sandbox2.row = row;
						code2 = 'resRow = {};';
						vm.runInContext(code2, sandbox2);
						code2 = '';
						for (let i = 0; i < selectColumns.length; i++) {
							let splitColumnName = selectColumns[i].split(' ');
							if (row[i] == null) {
								code2 += ' resRow.' + splitColumnName[splitColumnName.length - 1] + ' = null;';
							} else if (typeof row[i] == "number") {
								code2 += ' resRow.' + splitColumnName[splitColumnName.length - 1] + ' = row[' + i + '];';
							} else {
								code2 += ' resRow.' + splitColumnName[splitColumnName.length - 1] + ' = row[' + i + '];';
							}
						}
						code2 += 'dbData.push(resRow);';
						vm.runInContext(code2, sandbox2);
					});

					stream.on('end', function () {
						connection.close();
						resolve(sandbox2.dbData);
					});
				});
			});
	});
};

/*
Schemas.DB_CONNECTION;
Schemas.DB_TYPE;
Schemas.SCHEMA_NAME.name;
Schemas.SCHEMA_NAME.Tables.TABLE_NAME.name;
Schemas.SCHEMA_NAME.Tables.TABLE_NAME.owner;
Schemas.SCHEMA_NAME.Tables.TABLE_NAME.DB_CONNECTION;
Schemas.SCHEMA_NAME.Tables.TABLE_NAME.DB_TYPE;
Schemas.SCHEMA_NAME.Tables.TABLE_NAME.selectColumns;
Schemas.SCHEMA_NAME.Tables.TABLE_NAME.findAll({ column: "COLUMN_NAME", value: "VALUE", operation: "AND" });
Schemas.SCHEMA_NAME.Tables.TABLE_NAME.COLUMN_NAME.name;
Schemas.SCHEMA_NAME.Tables.TABLE_NAME.COLUMN_NAME.dataType;
*/
var getOracleORM = function (__dbConnection, selectedSchemas) {
	return new Promise(function (resolve, reject) {
		if (!verifyDatabaseConnection(__dbConnection)) {
			reject();
		}

		oracledb.getConnection({
			user: __dbConnection.user,
			password: __dbConnection.password,
			connectString: __dbConnection.connectString
		},
			function (err, connection) {
				if (err) {
					console.log(err);
					connection.close();
					reject();
				}

				var bindvars = {
					cursor: { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
				};

				var schemas = '';
				var isFirst = true;
				_.forEach(selectedSchemas, function (schema) {
					if (isFirst) {
						schemas += `'${schema}'`;
						isFirst = false;
					} else {
						schemas += `, '${schema}'`;
					}
				});

				var query = `BEGIN OPEN :cursor FOR
					SELECT '{'
						|| '"owner": "' || TC.OWNER || '",'
						|| '"tableName": "' || TC.TABLE_NAME || '",'
						|| '"columnName": "' || TC.COLUMN_NAME || '",'
						|| '"dataType": "' ||  TC.DATA_TYPE || '",'
						|| '"unique": "' ||  CASE WHEN I.UNIQUENESS = 'UNIQUE' THEN 'YES' ELSE 'NO' END || '"'
						|| '}' AS JSON
					FROM ALL_IND_COLUMNS IC
					JOIN ALL_INDEXES I
					ON IC.INDEX_NAME = I.INDEX_NAME
					AND IC.INDEX_OWNER IN (${schemas})
					AND I.INDEX_TYPE = 'NORMAL'
					RIGHT OUTER JOIN ALL_TAB_COLUMNS TC
					ON IC.COLUMN_NAME = TC.COLUMN_NAME
					WHERE TC.OWNER IN (${schemas})
				END;`;

				console.log();
				console.log('SQL Query');
				console.log('-----------------------------------------------------------------------------------------------------------------------');
				console.log(query);

				connection.execute(query, bindvars, { prefetchRows: 400 }, function (err, result) {
					var cursor;
					var stream;
					var __orm = [];

					if (err) {
						console.log(err);
						connection.close();
						reject();
					}

					cursor = result.outBinds.cursor;
					stream = cursor.toQueryStream();

					stream.on('data', function (row) {
						__orm.push(JSON.parse(row[0]));
					});

					stream.on('end', function () {
						connection.close();
						var orm = [];

						_.forEach(__orm, function (value) {
							var ind = _.findIndex(orm, { ownerValue: value.owner, tableNameValue: value.tableName });

							if (ind === -1) {
								var code1 = {
									owner: value.owner.replace(/\W/g, ''),
									ownerValue: value.owner,
									tableName: value.tableName.replace(/\W/g, ''),
									tableNameValue: value.tableName,
									columns: [{
										columnName: value.columnName.replace(/\W/g, ''),
										columnNameValue: value.columnName,
										dataType: value.dataType,
										unique: value.unique
									}]
								};
								orm.push(code1);
							} else {
								var code2 = orm[ind];
								code2.columns.push({
									columnName: value.columnName.replace(/\W/g, ''),
									columnNameValue: value.columnName,
									dataType: value.dataType,
									unique: value.unique
								});
								orm[ind] = code2;
							}
						});

						var Schemas = {};
						const sandbox = { orm: orm, Schemas: Schemas, findAll: findAll, __dbConnection: __dbConnection };
						var code = '';
						vm.createContext(sandbox);

						code = 'Schemas.DB_CONNECTION = __dbConnection;';
						code += 'Schemas.DB_TYPE = \'ORACLE\';';
						vm.runInContext(code, sandbox);

						for (let i = 0; i < orm.length; i++) {
							code = 'if (Schemas.' + orm[i].owner + ' == null) { Schemas.' + orm[i].owner + ' = {}; Schemas.' + orm[i].owner + '.Tables = {}; Schemas.' + orm[i].owner + '.name = \"' + orm[i].owner + '\";}';

							code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + ' = {};';
							code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.name = "' + orm[i].tableNameValue + '";';
							code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.owner = "' + orm[i].ownerValue + '";';
							code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.findAll = findAll;';
							code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.selectColumns = [];';
							code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.selectColumnsFormatted = [];';
							code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.DB_CONNECTION = __dbConnection;';
							code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.DB_TYPE = \'ORACLE\';';

							for (let j = 0; j < orm[i].columns.length; j++) {
								code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.' + orm[i].columns[j].columnName + ' = {};';
								code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.' + orm[i].columns[j].columnName + '.name = "' + orm[i].columns[j].columnNameValue + '";';
								code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.' + orm[i].columns[j].columnName + '.dataType = "' + orm[i].columns[j].dataType + '";';
								code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.' + orm[i].columns[j].columnName + '.unique = "' + orm[i].columns[j].unique + '";';
								code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.selectColumns.push("' + orm[i].columns[j].columnNameValue + '");';
								code += 'Schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.selectColumnsFormatted.push("' + orm[i].columns[j].columnName + '");';
							}
							vm.runInContext(code, sandbox);
						}

						resolve(sandbox.Schemas);
					});
				});
			});
	});
};

module.exports = {
	generateGraphQL: generateGraphQL,
	setMaxRows: setMaxRows
};