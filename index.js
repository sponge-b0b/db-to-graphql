var oracledb = require('oracledb');
var _ = require("lodash");
const vm = require('vm');
var { buildSchema } = require('graphql');

oracledb.queueTimeout = 0;
oracledb.poolTimeout = 0;

var maxRows = 200;

async function setMaxRows(val) {
	maxRows = val;
};

/*
IN:
dbConnection = { user: "", password: "", connectionString: "host:port/sid" }
dbType = "" // "Oracle"
selectedSchemas = ['', '', '', ...]
OUT:
{
  root: {},
  schema: buildSchema('')
}
*/
async function generateGraphQL(dbConfig, dbType, selectedSchemas = null) {
	var result = { root: {}, schema: {} };
	try {
		validateDatabaseConfig(dbConfig);

		if (dbType != 'Oracle') {
			throw "Database type not supported: " + dbType;
		}

		//await oracledb.createPool(dbConfig);

		var dbSchema = await getOracleORM(dbConfig);

		const sandbox = { root: root, dbSchema: dbSchema };
		vm.createContext(sandbox);

		var graphqlSchema = '';
		var schemaQuery = 'type Query {';
		var selectedTables;

		if (selectedSchemas != null) {
			selectedTables = _.filter(dbSchema, function (o) {
				return _.includes(selectedSchemas, o.name);
			});
		}
		else {
			selectedTables = dbSchema;
		}

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

		result.schema = buildSchema(graphqlSchema);
	} catch (error) {
		throw error;
	}
	return result;
};

async function findAll(whereColumns = null) {
	return await getData(this.DB_CONNECTION, { name: this.name, owner: this.owner }, this.selectColumns, whereColumns);
};

function validateDatabaseConfig(dbConfig) {
	if (dbConfig == null || dbConfig.user == null || dbConfig.password == null || dbConfig.connectString == null) {
		throw 'The database configuration is invalid.';
	};
};

function translateJStoGraphQLType(type) {
	switch (type) {
		case 'STRING':
			return 'String';
		case 'NUMBER':
			return 'Float';
		default:
			return 'String';
	}
};

async function getData(dbConfig, fromTable, selectColumns, whereColumns = null) {
	if (fromTable == null || fromTable.name == null || fromTable.owner == null || selectColumns == null || selectColumns.length === 0) {
		throw 'fromTable is invalid: ' + fromTable;
	}

	if (whereColumns != null && whereColumns.length === 0) {
		throw 'whereColumns are invalid: ' + whereColumns;
	}

	var bindVars = {
		cursor: { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
	};

	const sandbox = { bindVars: bindVars, whereColumns: whereColumns };
	vm.createContext(sandbox);

	var query = 'BEGIN OPEN :cursor FOR SELECT ';
	for (let i = 0; i < selectColumns.length; i++) {
		query += '' + selectColumns[i];
		if (selectColumns[i + 1] != null) {
			query += ',';
		}
	}
	query += ' FROM ' + fromTable.owner + '.' + fromTable.name;

	var code = '';
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
			code = 'bindVars.' + whereColumns[i].column.name + i + ' = whereColumns[' + i + '].value;';
			vm.runInContext(code, sandbox);
		}
		query += ')';
	}
	else {
		query += ' WHERE (ROWNUM <= ' + maxRows + ')';
	}

	bindVars = sandbox.bindVars;
	query += "; END;";

	var connection = null;
	var sandbox2 = { resRow: {}, dbData: [] };
	vm.createContext(sandbox2);
	try {
		var connection = await oracledb.getConnection({
			user: dbConfig.user,
			password: dbConfig.password,
			connectString: dbConfig.connectString
		});
		var result = await connection.execute(query, bindVars, { prefetchRows: 400 });

		var cursor = result.outBinds.cursor;
		var stream = await cursor.toQueryStream();
		var row = await stream.on('data');
		var code2 = 'resRow = {};';

		sandbox2.row = row;
		vm.runInContext(code2, sandbox2);

		code2 = '';
		for (let i = 0; i < selectColumns.length; i++) {
			var splitColumnName = selectColumns[i].split(' ');
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
		await stream.on('end');
		await connection.close();
	} catch (error) {
		if (connection != null) {
			await connection.close();
		}
		throw error;
	}
	return sandbox2.dbData;
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
async function getOracleORM(dbConfig) {
	var query = `BEGIN OPEN :cursor FOR
		SELECT '{'
		|| '"tableName": "' || tab.TABLE_NAME || '",'
		|| '"owner": "' || tab.OWNER || '",'
		|| '"columnName": "' || tab.COLUMN_NAME || '",'
		|| '"dataType": "' ||  CASE WHEN tab.DATA_TYPE LIKE 'VARCHAR%' THEN 'STRING'
			WHEN tab.DATA_TYPE = 'CHAR' THEN 'STRING'
			WHEN tab.DATA_TYPE = 'DATE' THEN 'STRING'
			WHEN tab.DATA_TYPE LIKE 'TIMESTAMP%' THEN 'STRING'
			WHEN tab.DATA_TYPE LIKE 'LONG%' THEN 'NUMBER'
			WHEN tab.DATA_TYPE = 'NUMBER' THEN 'NUMBER'
			WHEN tab.DATA_TYPE = 'RAW' THEN 'STRING'
			WHEN tab.DATA_TYPE LIKE '_LOB' THEN 'STRING'
			WHEN tab.DATA_TYPE = 'RAW' THEN 'STRING'
			ELSE 'STRING' END || '"'
		|| '}' AS JSON
		FROM ALL_TAB_COLUMNS tab
		WHERE tab.OWNER NOT IN ('SYS', 'SYSTEM')
		AND tab.TABLE_NAME NOT IN ('PLSQL_PROFILER_DATA', 'TOAD_PLAN_TABLE')
		AND REGEXP_INSTR(tab.COLUMN_NAME ,'[^[:alnum:]_*]') = 0
		AND REGEXP_INSTR(tab.TABLE_NAME ,'[^[:alnum:]_*]') = 0
		UNION ALL
		SELECT '{'
		|| '"tableName": "' || syn.SYNONYM_NAME || '",'
		|| '"owner": "' || tab.OWNER || '",'
		|| '"columnName": "' || tab.COLUMN_NAME || '",'
		|| '"dataType": "' ||  CASE WHEN tab.DATA_TYPE LIKE 'VARCHAR%' THEN 'STRING'
			WHEN tab.DATA_TYPE = 'CHAR' THEN 'STRING'
			WHEN tab.DATA_TYPE = 'DATE' THEN 'STRING'
			WHEN tab.DATA_TYPE LIKE 'TIMESTAMP%' THEN 'STRING'
			WHEN tab.DATA_TYPE LIKE 'LONG%' THEN 'NUMBER'
			WHEN tab.DATA_TYPE = 'NUMBER' THEN 'NUMBER'
			WHEN tab.DATA_TYPE = 'RAW' THEN 'STRING'
			WHEN tab.DATA_TYPE LIKE '_LOB' THEN 'STRING'
			WHEN tab.DATA_TYPE = 'RAW' THEN 'STRING'
			ELSE 'STRING' END || '"'
		|| '}' AS JSON
		FROM ALL_SYNONYMS syn
		JOIN ALL_TAB_COLUMNS tab ON syn.TABLE_NAME = tab.TABLE_NAME
		WHERE syn.SYNONYM_NAME != syn.TABLE_NAME
		AND syn.OWNER NOT IN ('SYS', 'SYSTEM', 'PUBLIC')
		AND syn.TABLE_OWNER NOT IN ('SYS', 'SYSTEM', 'PUBLIC')
		AND NOT EXISTS (
			SELECT 1 FROM ALL_TAB_COLUMNS x
			WHERE x.TABLE_NAME = syn.SYNONYM_NAME
			AND x.OWNER NOT IN ('SYS', 'SYSTEM')
			AND x.TABLE_NAME NOT IN ('PLSQL_PROFILER_DATA', 'TOAD_PLAN_TABLE')
			AND REGEXP_INSTR(x.COLUMN_NAME ,'[^[:alnum:]_*]') = 0
			AND REGEXP_INSTR(x.TABLE_NAME ,'[^[:alnum:]_*]') = 0
		);

		END;`;

	var bindVars = {
		cursor: { type: oracledb.CURSOR, dir: oracledb.BIND_OUT }
	};

	var schemas = {};
	var connection = null;
	try {
		var connection = await oracledb.getConnection({
			user: dbConfig.user,
			password: dbConfig.password,
			connectString: dbConfig.connectString
		});
		var result = await connection.execute(query, bindVars, { prefetchRows: 400 });

		var cursor = result.outBinds.cursor;
		var stream = await cursor.toQueryStream();

		var _orm = [];
		stream.on('data', function (row) {
			_orm.push(JSON.parse(row[0]));
		});

		stream.on('end', function (row) {
			await connection.close();
		});

		var orm = [];
		_.forEach(_orm, function (value, key) {
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
						dataType: value.dataType
					}]
				};
				orm.push(code1);
			}
			else {
				var code2 = orm[ind];
				code2.columns.push({
					columnName: value.columnName.replace(/\W/g, ''),
					columnNameValue: value.columnName,
					dataType: value.dataType
				});
				orm[ind] = code2;
			}
		});

		const sandbox = { orm: orm, schemas: schemas, findAll: findAll };
		vm.createContext(sandbox);

		var code = 'schemas.DB_CONNECTION = dbConfig;';
		code += 'schemas.DB_TYPE = \'ORACLE\';';
		vm.runInContext(code, sandbox);

		//var code = '';
		for (let i = 0; i < orm.length; i++) {
			code = 'if (schemas.' + orm[i].owner + ' == null) { schemas.' + orm[i].owner + ' = {}; schemas.' + orm[i].owner + '.Tables = {}; schemas.' + orm[i].owner + '.name = \"' + orm[i].owner + '\";}';

			code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + ' = {};';
			code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.name = "' + orm[i].tableNameValue + '";';
			code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.owner = "' + orm[i].ownerValue + '";';
			code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.findAll = findAll;';
			code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.selectColumns = [];';
			code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.selectColumnsFormatted = [];';
			code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.DB_CONNECTION = dbConfig;';
			code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.DB_TYPE = \'ORACLE\';';

			for (let j = 0; j < orm[i].columns.length; j++) {
				code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.' + orm[i].columns[j].columnName + ' = {};';
				code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.' + orm[i].columns[j].columnName + '.name = "' + orm[i].columns[j].columnNameValue + '";';
				code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.' + orm[i].columns[j].columnName + '.dataType = "' + orm[i].columns[j].dataType + '";';
				code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.selectColumns.push("' + orm[i].columns[j].columnNameValue + '");';
				code += 'schemas.' + orm[i].owner + '.Tables.' + orm[i].tableName + '.selectColumnsFormatted.push("' + orm[i].columns[j].columnName + '");';
			}
			vm.runInContext(code, sandbox);
		}
		schemas = sandbox.schemas;
	} catch (error) {
		if (connection != null) {
			await connection.close();
		}
		throw error;
	}
	return schemas;
};

module.exports = {
	generateGraphQL: generateGraphQL,
	setMaxRows: setMaxRows
};