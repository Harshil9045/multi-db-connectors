var debug = require("debug")("node-database-connectors:bigquery");
const { BigQuery } = require("@google-cloud/bigquery");

const utils = require("./utils.js");
//connect
var fieldIdentifier_left = "`",
  fieldIdentifier_right = "`";

exports.connectPool = function (json, cb) {
  return connectPool(json, cb);
};

exports.connect = function (json, cb) {
  return connect(json, cb);
};

function __createClient(json) {
  __validateConnJSON(json);
  const options = { projectId: json.projectId };
  if (json.location) {
    options.location = json.location;
  }
  if (json.keyType == "filepath") {
    options.keyFilename = json.keyValue;
  } else if (json.keyType == "env_variable") {
    if (!process.env[json.keyValue]) {
      console.log("No Env variable found with value " + json.keyValue);
      throw new Error("No Env variable found with value " + json.keyValue);
    }
    options.keyFilename = process.env[json.keyValue];
  }
  const bigqueryClient = new BigQuery(options);
  bigqueryClient.__axiomQueryOptions = __queryOptions(json);
  bigqueryClient.__axiomLimiter = __createLimiter(
    __positiveNumber(json.connectionLimit, DEFAULT_CONNECTION_LIMIT),
    __positiveNumber(json.acquireTimeout, DEFAULT_ACQUIRE_TIMEOUT)
  );
  return bigqueryClient;
}

//max connectionLimit queries at a time, like a connection pool
const DEFAULT_CONNECTION_LIMIT = 25;
const DEFAULT_ACQUIRE_TIMEOUT = 60 * 1000;

function __positiveNumber(value, defaultValue) {
  const n = Number(value);
  return n > 0 ? n : defaultValue;
}

function __createLimiter(maxConcurrent, acquireTimeout) {
  let active = 0;
  const queue = [];

  function newRelease() {
    let released = false;
    return function release() {
      if (released) return;
      released = true;
      active--;
      while (queue.length > 0 && active < maxConcurrent) {
        const next = queue.shift();
        clearTimeout(next.timer);
        active++;
        next.cb(null, newRelease());
      }
    };
  }

  function acquire(cb) {
    if (active < maxConcurrent) {
      active++;
      cb(null, newRelease());
      return;
    }
    const entry = { cb: cb, timer: null };
    entry.timer = setTimeout(function () {
      const index = queue.indexOf(entry);
      if (index > -1) queue.splice(index, 1);
      const err = new Error(
        "Timeout acquiring a BigQuery query slot after " + acquireTimeout + "ms (" +
          active + " running, " + queue.length + " waiting, limit " + maxConcurrent + ")"
      );
      err.code = "POOL_ACQUIRE_TIMEOUT";
      cb(err);
    }, acquireTimeout);
    queue.push(entry);
  }

  return {
    acquire: acquire,
    stats: function () {
      return { active: active, waiting: queue.length, limit: maxConcurrent, acquireTimeout: acquireTimeout };
    },
  };
}

exports.createLimiter = function (maxConcurrent, acquireTimeout) {
  return __createLimiter(maxConcurrent, acquireTimeout);
};

//one client per config
const __clients = new Map();

function __clientKey(json) {
  return JSON.stringify([
    json.projectId, json.keyType, json.keyValue, json.dataset || json.schema, json.location,
    json.maximumBytesBilled, json.jobTimeoutMs, json.timezone, json.connectionLimit, json.acquireTimeout,
    json.labels,
  ]);
}

function __getClient(json) {
  __validateConnJSON(json);
  const key = __clientKey(json);
  if (!__clients.has(key)) {
    __clients.set(key, __createClient(json));
  }
  return __clients.get(key);
}

function __queryOptions(json) {
  const queryOptions = {};
  if (json.location) {
    queryOptions.location = json.location;
  }
  const dataset = json.dataset || json.schema;
  if (dataset) {
    queryOptions.defaultDataset = { projectId: json.projectId, datasetId: dataset };
  }
  if (json.maximumBytesBilled !== undefined && json.maximumBytesBilled !== null && json.maximumBytesBilled !== "") {
    queryOptions.maximumBytesBilled = json.maximumBytesBilled.toString();
  }
  if (json.labels && typeof json.labels == "object") {
    queryOptions.labels = json.labels;
  }
  if (Number(json.jobTimeoutMs) > 0) {
    queryOptions.jobTimeoutMs = Number(json.jobTimeoutMs).toString();
  }
  return {
    queryOptions: queryOptions,
    timezone: json.timezone || "local",
  };
}

exports.getQueryOptions = function (json) {
  return __queryOptions(json);
};

function connectPool(json, cb) {
  const bigqueryClient = __getClient(json);
  if (cb) cb(null, bigqueryClient);
  return bigqueryClient;
  // var numConnections = json.connectionLimit || 0;
  // var pool = db.createPool({
  //   acquireTimeout: json.acquireTimeout || 2 * 1000,
  //   connectionLimit: numConnections,
  //   host: json.host,
  //   port: json.port,
  //   user: json.user,
  //   database: json.database,
  //   password: json.password,
  //   multipleStatements: json.connectionLimit === false ? false : true,
  // });
  // if (cb) cb(null, bigqueryClient);
  // return bigqueryClient;

  // acquireConnection(0);

  // function acquireConnection(index) {
  //   if (index >= numConnections) {
  //     return;
  //   }
  //   debug("acquiring connection ", index, json);
  //   pool.getConnection(function (err, connection) {
  //     if (err) {
  //       debug("error in acquiring connection", index, err, json);
  //     } else {
  //       debug("releasing connection ", index, json);
  //       connection.release();
  //     }
  //     acquireConnection(index + 1);
  //   });
  // }
  // return pool;
}

function connect(json, cb) {
  const bigqueryClient = __getClient(json);
  if (cb) cb(null, bigqueryClient);
  return bigqueryClient;

  // var connection = db.createConnection({
  //   host: json.host,
  //   port: json.port,
  //   user: json.user,
  //   database: json.database,
  //   password: json.password,
  //   multipleStatements: json.connectionLimit === false ? false : true,
  // });
  // console.log("CONNECTION CREATED...", connection.state, connection.threadId);
  // connection.connect(function (err) {
  //   if (err) {
  //     debug("error-A");
  //     debug(["c.connect", err]);
  //   } else {
  //     connection.on("error", function (e) {
  //       debug("error-B");
  //       debug(["error", e]);
  //     });
  //   }
  //   if (cb) cb(err, connection);
  // });
  // return connection;
}

//disconnect
exports.disconnect = function () {
  return disconnect(arguments[0]);
};

function disconnect(connection) {
  //nothing to close, client is shared
}

function __validateConnJSON(json) {
  if (json == null || json == undefined) {
    console.log("Connection options to bigquery can not be empty or null");
    throw new Error("Connection options to bigquery can not be empty or null");
  }

  if (typeof json != "object") {
    console.log("Connecting json to bigquery should be an object.");
    throw new Error("Connecting json to bigquery should be an object.");
  }

  if (!json.hasOwnProperty("keyType")) {
    console.log("keyType property required while connecting to BigQuery");
    throw new Error("Expecting keyType while connecting to Bigquey.");
  }

  if (json.keyType != "filepath" && json.keyType != "env_variable" && json.keyType != "adc") {
    console.log(
      "Unexpected value for keyType found. It should be filepath, env_variable or adc."
    );
    throw new Error(
      "keyType values can be filepath, env_variable or adc for connecting to BigQuery."
    );
  }

  if (json.keyType != "adc" && !json.keyValue) {
    console.log("keyValue property required while connecting to BigQuery");
    throw new Error("Expecting keyValue while connecting to Bigquey.");
  }

  if (!json.projectId) {
    console.log("projectId property required while connecting to BigQuery");
    throw new Error("Expecting projectId while connecting to Bigquey.");
  }
}

//prepare query
exports.prepareQuery = function () {
  return prepareQuery(arguments[0], arguments[1]);
};

function __schema(json, dbConfig) {
  if (json && json.schema) return json.schema;
  if (dbConfig && dbConfig.dataset) return dbConfig.dataset;
  if (dbConfig && dbConfig.schema) return dbConfig.schema;
  return null;
}

function __tableRef(table, schema) {
  if (table == null || table === "") return encloseField(table);
  if (!schema || table.toString().indexOf(".") > -1) return encloseField(table);
  return encloseField(schema) + "." + encloseField(table);
}

function __colRef(table, field) {
  return table != null ? table + "." + field : field;
}

function __limitClause(limit) {
  var parts = limit.toString().split(",");
  if (parts.length == 2) {
    return " LIMIT " + parseInt(parts[1].trim()) + " OFFSET " + parseInt(parts[0].trim());
  }
  return " LIMIT " + limit;
}

var __formatTokens = {
  "%i": "%M",
  "%s": "%S",
  "%M": "%B",
  "%W": "%A",
  "%h": "%I",
  "%c": "%m",
  "%f": "%E6S",
};

function __toBigQueryFormat(format) {
  return format.toString().replace(/%./g, function (token) {
    return __formatTokens.hasOwnProperty(token) ? __formatTokens[token] : token;
  });
}

function createInsertQuery(json, dbConfig) {
  var table = json.table ? __tableRef(json.table, __schema(json, dbConfig)) : null;
  var vInsert = json.insert ? json.insert : null;
  var arrInsert = [];
  arrInsert = createInsert(vInsert);
  var query = "";
  if (!Array.isArray(vInsert)) {
    if (!Array.isArray(vInsert.fValue[0])) {
      query =
        "INSERT INTO " +
        table +
        "(" +
        arrInsert.fieldArr.join() +
        ") VALUES(" +
        arrInsert.valueArr.join() +
        ")";
    } else {
      query =
        "INSERT INTO " +
        table +
        "(" +
        arrInsert.fieldArr.join() +
        ") VALUES " +
        arrInsert.valueArr.join() +
        "";
    }
  } else {
    query =
      "INSERT INTO " +
      table +
      "(" +
      arrInsert.fieldArr.join() +
      ") VALUES(" +
      arrInsert.valueArr.join() +
      ")";
  }
  return query + ";";
}

function createUpdateQuery(json, dbConfig) {
  var arrUpdate = [],
    arrFilter = [],
    strJOIN = "";
  var schema = __schema(json, dbConfig);
  var table = json.table ? json.table : null;
  var fromTblAlias = json.alias ? json.alias : null;
  var vUpdate = json.update ? json.update : null,
    vFilter = json.filter ? json.filter : null,
    join = json.join ? json.join : null;

  if (vFilter != null) arrFilter = createFilter(vFilter);
  if (join != null) {
    strJOIN = createJOIN(join, schema);
    if (strJOIN.length > 0) {
      table = strJOIN;
    } else {
      if (table != null) {
        table =
          __tableRef(table, schema) + (fromTblAlias ? " as " + fromTblAlias : "");
      } else {
        table = fromTblAlias ? " as " + fromTblAlias : "";
      }
    }
  } else if (table != null) {
    table = __tableRef(table, schema);
  }

  arrUpdate = createUpdate(vUpdate);
  var query = "UPDATE " + table + " SET " + arrUpdate.join() + "";
  if (arrFilter.length > 0) {
    query += " WHERE " + arrFilter.join("");
  } else {
    query += " WHERE true";
  }
  return query + ";";
}

function createSelectQuery(json, dbConfig) {
  var arrSelect = [],
    arrSortBy = [],
    arrFilter = [],
    arrGroupBy = [],
    arrHaving = [],
    strJOIN = "";
  var table = json.table ? json.table : null;
  var fromTblAlias = json.alias ? json.alias : json.table;
  var schema = __schema(json, dbConfig);
  var sortby = json.sortby ? json.sortby : null,
    limit = json.limit ? json.limit : null,
    join = json.join ? json.join : null;
  var vSelect = json.select ? json.select : null,
    vFilter = json.filter ? json.filter : null,
    vGroupby = json.groupby ? json.groupby : null,
    vHaving = json.having ? json.having : null;

  if (vHaving != null) arrHaving = createAggregationFilter(vHaving);
  if (vGroupby != null) arrGroupBy = createSelect(vGroupby, false);
  if (vFilter != null) arrFilter = createFilter(vFilter);

  arrSelect = createSelect(vSelect, true);

  //from/join
  strJOIN = createJOIN(join, schema);
  if (strJOIN.length > 0) {
    table = strJOIN;
  } else {
    table = __tableRef(table, schema);
  }

  //order by
  if (sortby != null) {
    for (var s = 0; s < sortby.length; s++) {
      var encloseFieldFlag =
        sortby[s].encloseField != undefined ? sortby[s].encloseField : true;
      var sortField = encloseField(sortby[s].field, encloseFieldFlag);
      var sortTable =
        sortby[s].table != undefined ? encloseField(sortby[s].table) : null;
      var sortOrder = sortby[s].order ? sortby[s].order : "ASC";
      if (sortTable == null) arrSortBy.push(sortField + " " + sortOrder);
      else arrSortBy.push(sortTable + "." + sortField + " " + sortOrder);
    }
  }

  var query = "SELECT " + arrSelect.join();
  if (table != "") {
    query += " FROM " + table + "";
  }
  if (arrFilter.length > 0) {
    query += " WHERE " + arrFilter.join("");
  }
  if (arrGroupBy.length > 0) {
    query += " GROUP BY " + arrGroupBy.join();
  }
  if (arrHaving.length > 0) {
    query += " HAVING " + arrHaving.join("");
  }
  if (arrSortBy.length > 0) {
    query += " ORDER BY " + arrSortBy.join();
  }
  if (limit != null) {
    query += __limitClause(limit);
  }
  return query + ";";
}

function createDeleteQuery(json, dbConfig) {
  var table = json.table ? __tableRef(json.table, __schema(json, dbConfig)) : null;
  var arrFilter = [];
  var vFilter = json.filter ? json.filter : null;
  if (vFilter != null) {
    arrFilter = createFilter(vFilter);
  }
  var query = "";
  if (arrFilter.length > 0) {
    query = "DELETE FROM " + table + " WHERE " + arrFilter.join("");
  } else {
    query = "DELETE FROM " + table + " WHERE 1=1";
  }
  return query + ";";
}

function validateJson(json) {
  return utils.validateJson(json);
}

function prepareQuery(json, dbConfig) {
  var validate = validateJson(json);

  if (validate !== "") {
    return validate;
  } else {
    var vInsert = json.insert ? json.insert : null,
      vSelect = json.select ? json.select : null,
      vUpdate = json.update ? json.update : null,
      vDelete = json.delete ? json.delete : null;

    //INSERT
    if (vInsert != null) {
      return createInsertQuery(json, dbConfig);
    }
    //UPDATE
    else if (vUpdate != null) {
      return createUpdateQuery(json, dbConfig);
    }
    //DELETE
    else if (vDelete != null) {
      return createDeleteQuery(json, dbConfig);
    }
    //SELECT
    else if (vSelect != null) {
      return createSelectQuery(json, dbConfig);
    }
  }
}

//Create select expression
function createSelect(arr, selectAll) {
  var tempArr = [];
  if (arr != null) {
    if (arr.length == 0 && selectAll == true) {
      tempArr.push("*");
    } else {
      for (var s = 0; s < arr.length; s++) {
        var obj = arr[s];
        if (typeof obj === "string") {
          obj = { field: obj };
        }
        if (
          obj.encloseField != undefined &&
          typeof obj.encloseField != "boolean"
        ) {
          obj.encloseField = obj.encloseField == "false" ? false : true;
        }
        var encloseFieldFlag =
          obj.encloseField != undefined ? obj.encloseField : true;
        var field = encloseField(obj.field, encloseFieldFlag);
        var table = obj.table ? encloseField(obj.table) : null;
        var hasAlias = obj.alias ? true : false;
        var alias = encloseField(obj.alias ? obj.alias : obj.field);
        var expression = obj.expression ? obj.expression : null;
        var aggregation = obj.aggregation ? obj.aggregation : null;
        var dataType = obj.dataType ? obj.dataType : null;
        var format = obj.format ? obj.format : null;
        var selectText = "";
        if (expression != null) {
          selectText = "(CASE ";
          var cases = expression.cases;
          var defaultCase = expression["default"];
          var defaultValue = "";
          for (var e = 0; e < cases.length; e++) {
            var operator = cases[e].operator;
            var value = cases[e].value;
            var out = cases[e].out;
            var outVal = "";
            if (out.hasOwnProperty("value")) {
              outVal = out.value;
            } else {
              outVal = encloseField(out.table) + "." + encloseField(out.field);
            }
            var strOperatorSign = "";
            strOperatorSign = operatorSign(operator, value);
            if (strOperatorSign.indexOf("IN") > -1) {
              //IN condition has different format
              selectText +=
                " WHEN " +
                __colRef(table, field) +
                " " +
                strOperatorSign +
                " (" +
                value.map(__quoteValue).join(",") +
                ") THEN " +
                outVal;
            } else {
              selectText +=
                " WHEN " +
                __colRef(table, field) +
                " " +
                strOperatorSign +
                " " +
                __quoteValue(value) +
                " THEN " +
                outVal;
            }
          }
          if (defaultCase.hasOwnProperty("value")) {
            defaultValue = defaultCase.value;
          } else {
            defaultValue =
              encloseField(defaultCase.table) +
              "." +
              encloseField(defaultCase.field);
          }
          selectText += " ELSE " + defaultValue + " END)";
        } else {
          if (dataType != null) {
            if (dataType.toString().toLowerCase() == "datetime") {
              selectText =
                " FORMAT_DATETIME('" +
                __toBigQueryFormat(format) +
                "', CAST(" +
                __colRef(table, field) +
                " AS DATETIME)) ";
            } else {
              if (encloseFieldFlag == false || encloseFieldFlag == "false")
                selectText = field;
              else selectText = table + "." + field;
            }
          } else {
            if (encloseFieldFlag == false || encloseFieldFlag == "false") {
              selectText = field;
            } else {
              if (table != null) {
                selectText = table + "." + field;
              } else {
                selectText = field;
              }
            }
          }
        }
        if (aggregation != null) {
          //CBT:this is for nested aggregation if aggregation key contains Array
          if (
            Object.prototype.toString.call(aggregation).toLowerCase() ===
            "[object array]"
          ) {
            var aggregationText = "";
            aggregation.forEach(function (d) {
              aggregationText = aggregationText + d + "(";
            });
            selectText = aggregationText + selectText;
            aggregationText = "";
            aggregation.forEach(function (d) {
              aggregationText = aggregationText + ")";
            });
            selectText = selectText + aggregationText;
          } else {
            selectText = aggregation + "(" + selectText + ")";
          }
        }
        if (hasAlias) selectText += " as " + alias;
        tempArr.push(selectText);
        selectText = null;
      }
    }
  }

  // else {
  //   tempArr.push('*');
  // }
  return tempArr;
}

function createInsert(arr) {
  var tempJson = {
    fieldArr: [],
    valueArr: [],
  };
  if (arr == null) {
    console.error("createInsert -> blank arr found");
  } else {
    if (!Array.isArray(arr)) {
      for (var y = 0; y < arr.field.length; y++) {
        var obj = arr.field[y];
        var field = encloseField(obj);
        tempJson.fieldArr.push(field);
      }
      for (var z = 0; z < arr.fValue.length; z++) {
        var obj = arr.fValue[z];
        if (Array.isArray(obj)) {
          var subValueArr = obj.map(__quoteValue);
          tempJson.valueArr.push("(" + subValueArr.join() + ")");
        } else {
          tempJson.valueArr.push(__quoteValue(obj));
        }
      }
    } else {
      for (var s = 0; s < arr.length; s++) {
        var obj = arr[s];
        if (
          obj.encloseField != undefined &&
          typeof obj.encloseField != "boolean"
        ) {
          obj.encloseField = obj.encloseField == "false" ? false : true;
        }
        var encloseFieldFlag =
          obj.encloseField != undefined ? obj.encloseField : true;
        var field = encloseField(obj.field, encloseFieldFlag);
        tempJson.fieldArr.push(field);
        tempJson.valueArr.push(__quoteValue(obj.fValue));
      }
    }
    return tempJson;
  }
}

function replaceSingleQuote(aValue) {
  if (aValue != undefined && typeof aValue === "string") {
    return aValue.replace(/\\/g, "\\\\").replace(/\'/g, "\\'");
  } else {
    return aValue;
  }
}

function __quoteValue(aValue) {
  if (aValue === null || aValue === undefined) return "null";
  if (typeof aValue === "number" || typeof aValue === "boolean") return String(aValue);
  if (aValue instanceof Date) return "'" + aValue.toISOString() + "'";
  return "'" + replaceSingleQuote(aValue.toString()) + "'";
}

function createUpdate(arr) {
  var tempArr = [];
  if (arr != null) {
    for (var s = 0; s < arr.length; s++) {
      var obj = arr[s];
      if (
        obj.encloseField != undefined &&
        typeof obj.encloseField != "boolean"
      ) {
        obj.encloseField = obj.encloseField == "false" ? false : true;
      }
      var encloseFieldFlag =
        obj.encloseField != undefined ? obj.encloseField : true;
      var field = encloseField(obj.field, encloseFieldFlag);
      var table = obj.table ? encloseField(obj.table) : null;
      var fValue = obj.fValue; // ? obj.fValue : '';
      var selectText = "";
      if (fValue != null) {
        selectText = __colRef(table, field) + "=" + __quoteValue(fValue);
      } else {
        if (encloseFieldFlag == true) {
          if (table != null) {
            selectText = table + "." + field + "=null";
          } else {
            selectText = field + "=null";
          }
        } else {
          selectText = field;
        }
      }
      tempArr.push(selectText);
    }
    return tempArr;
  }
}
//Create select expression
function createAggregationFilter(obj) {
  var tempHaving = [];
  if (obj != null) {
    tempHaving = createFilter(obj);
  }
  return tempHaving;
}

exports.createFilter = function (arr) {
  return createFilter(arr);
};

//Create filter conditions set
function createFilter(arr) {
  var tempArrFilter = [];
  if (arr != null) {
    if (
      arr.hasOwnProperty("and") ||
      arr.hasOwnProperty("AND") ||
      arr.hasOwnProperty("or") ||
      arr.hasOwnProperty("OR")
    ) {
      //multiple conditions
      tempArrFilter = createMultipleConditions(arr);
    } else {
      //single condition
      var conditiontext = createSingleCondition(arr);
      tempArrFilter.push(conditiontext);
    }
  }
  return tempArrFilter;
}

function createMultipleConditions(obj) {
  var tempArrFilters = [];
  var conditionType = Object.keys(obj)[0]; //AND/OR/NONE
  if (conditionType.toString().toLowerCase() != "none") {
    var listOfConditions = obj[conditionType]; //all conditions
    for (var c = 0; c < listOfConditions.length; c++) {
      var tempConditionType = Object.keys(listOfConditions[c])[0];
      //console.log('*************' + tempConditionType + '*******************');
      if (
        tempConditionType.toString().toLowerCase() == "and" ||
        tempConditionType.toString().toLowerCase() == "or"
      ) {
        tempArrFilters.push(createMultipleConditions(listOfConditions[c]));
      } else if (tempConditionType.toString().toLowerCase() == "none") {
        var conditiontext = createSingleCondition(listOfConditions[c].none);
        tempArrFilters.push(conditiontext);
      } else {
        var conditiontext = createSingleCondition(listOfConditions[c]);
        tempArrFilters.push(conditiontext);
      }
    }
  } else {
    //single condition
    if (listOfConditions.length > 0) {
      var conditiontext = createSingleCondition(listOfConditions.none);
      tempArrFilters.push(conditiontext);
    }
  }
  var tempConditionSet =
    "(" + tempArrFilters.join(" " + conditionType + " ") + ")";
  tempArrFilters = [];
  tempArrFilters.push(tempConditionSet);
  return tempArrFilters;
}

function encloseField(a, flag) {
  if (flag == undefined || (flag != undefined && flag == true))
    return fieldIdentifier_left + a + fieldIdentifier_right;
  else return a;
}

function operatorSign(operator, value) {
  var sign = "";
  if (operator.toString().toLowerCase() == "eq") {
    if (Object.prototype.toString.call(value) === "[object Array]") {
      sign = "IN";
    } else if (typeof value === "undefined" || value == null) {
      sign = "IS";
    } else if (typeof value == "string") {
      sign = "=";
    } else {
      sign = "=";
    }
  } else if (operator.toString().toLowerCase() == "noteq") {
    if (Object.prototype.toString.call(value) === "[object Array]") {
      sign = "NOT IN";
    } else if (typeof value === "undefined" || value == null) {
      sign = "IS NOT";
    } else if (typeof value == "string") {
      sign = "<>";
    } else {
      sign = "<>";
    }
  } else if (operator.toString().toLowerCase() == "match") {
    sign = "LIKE";
  } else if (operator.toString().toLowerCase() == "notmatch") {
    sign = "NOT LIKE";
  } else if (operator.toString().toLowerCase() == "gt") {
    sign = ">";
  } else if (operator.toString().toLowerCase() == "lt") {
    sign = "<";
  } else if (operator.toString().toLowerCase() == "gteq") {
    sign = ">=";
  } else if (operator.toString().toLowerCase() == "lteq") {
    sign = "<=";
  } else {
    throw Error("Unknow operator '%s'", operator);
  }
  return sign;
}

function createSingleCondition(obj) {
  var field = obj.field,
    table = obj.table ? encloseField(obj.table) : null,
    aggregation = obj.aggregation ? obj.aggregation : null,
    operator = obj.operator,
    value = obj.value,
    encloseFieldFlag = obj.encloseField;
  if (encloseFieldFlag != undefined && typeof encloseFieldFlag != "boolean") {
    encloseFieldFlag = encloseFieldFlag == "false" ? false : true;
  }
  var conditiontext = "";
  if (aggregation != null) {
    if (encloseFieldFlag == false) {
      //CBT:this is for nested aggregation if aggregation key contains Array
      if (
        Object.prototype.toString.call(aggregation).toLowerCase() ===
        "[object array]"
      ) {
        var aggregationText = "";
        aggregation.forEach(function (d) {
          aggregationText = aggregationText + d + "(";
        });
        conditiontext = aggregationText + field;
        aggregationText = "";
        aggregation.forEach(function (d) {
          aggregationText = aggregationText + ")";
        });
        conditiontext = conditiontext + aggregationText;
      } else {
        conditiontext = aggregation + "(" + field + ")";
      }
    } else {
      if (
        Object.prototype.toString.call(aggregation).toLowerCase() ===
        "[object array]"
      ) {
        var aggregationText = "";
        aggregation.forEach(function (d) {
          aggregationText = aggregationText + d + "(";
        });
        if (table != null) {
          conditiontext = aggregationText + table + "." + encloseField(field);
        } else {
          conditiontext = aggregationText + encloseField(field);
        }
        aggregationText = "";
        aggregation.forEach(function (d) {
          aggregationText = aggregationText + ")";
        });
        conditiontext = conditiontext + aggregationText;
      } else {
        if (table != null) {
          conditiontext =
            aggregation + "(" + table + "." + encloseField(field) + ")";
        } else {
          conditiontext = aggregation + "(" + encloseField(field) + ")";
        }
      }
    }
  } else {
    if (encloseFieldFlag == false) {
      conditiontext = field;
    } else {
      if (table != null) {
        conditiontext = "" + table + "." + encloseField(field) + "";
      } else {
        conditiontext = "" + encloseField(field) + "";
      }
    }
  }

  if (operator != undefined) {
    if(Array.isArray(value) && value.length ==1)
     {
       const updatedValue = value[0];
       value = updatedValue;
     }
    var sign = operatorSign(operator, value);
    if (sign.indexOf("IN") > -1) {
      //IN condition has different format
      var tempValue = value
        .map((d) => (d != null ? replaceSingleQuote(d.toString()) : d))
        .join("','");
      conditiontext += " " + sign + " ('" + tempValue + "')";
    } else {
      var tempValue = "";
      if (typeof value === "undefined" || value == null) {
        tempValue = "null";
      } else if (typeof value === "object") {
        sign = operatorSign(operator, "");
        if (value.hasOwnProperty("field")) {
          var rTable = value.table ? value.table : "";
          tempValue = encloseField(rTable) + "." + encloseField(value.field);
        }
      } else {
        if (typeof value == "string") {
          tempValue = "'" + replaceSingleQuote(value) + "'";
        } else {
          tempValue = value;
        }
      }
      conditiontext += " " + sign + " " + tempValue;
    }
  }
  return conditiontext;
}

//create join conditions
function createJOIN(join, schema) {
  var joinText = "";
  if (join != null) {
    var fromTbl = join.table;
    var fromTblAlias = join.alias;
    var joinwith = join.joinwith;
    // var strJoinConditions = '';
    joinText +=
      __tableRef(fromTbl, join.schema || schema) + (fromTblAlias ? " as " + fromTblAlias : "");
    for (var j = 0; j < joinwith.length; j++) {
      var table = joinwith[j].table,
        tableAlias = joinwith[j].alias,
        type = joinwith[j].type ? joinwith[j].type : "INNER",
        joincondition = joinwith[j].joincondition;

      //            for (var jc = 0; jc < joincondition.length; jc++) {
      //                strJoinConditions += joincondition[jc].on + ' ' + operatorSign(joincondition[jc].operator, joincondition[jc].value) + ' ' + joincondition[jc].value + ' ';
      //            }
      joinText +=
        " " +
        type.toString().toUpperCase() +
        " JOIN " +
        __tableRef(table, joinwith[j].schema || join.schema || schema) +
        (tableAlias ? " as " + tableAlias : "") +
        " ON " +
        createFilter(joincondition).join("");
    }
  }
  return joinText;
}

//run query
exports.execQuery = function (query, connection, cb) {
  return execQuery(query, connection, cb);
};

function execQuery(query, connection, cb) {
  // var query = arguments[0][0];
  // var connection = null;
  // var format = null;
  // if (arguments[0].length > 1) {
  //   format = arguments[0][2];
  // }
  // if (arguments[0].length > 0) {
  // connection = arguments[0][1];
  //Commenting pipe and returning full JSON;
  //return connection.query(query).stream({ highWaterMark: 5 }).pipe(objectToCSV(format));
  connection.query(query, function (err, result, fields) {
    cb(err, result, fields);
  });
  // } else {
  //   return {
  //     status: false,
  //     content: {
  //       result: 'Connection not specified.'
  //     }
  //   };
  // }
}
/*
function objectToCSV(format) {
var stream = require('stream')
var liner = new stream.Transform({ objectMode: true })
var csv = [];
var isFirstChunk = true;
liner._transform = function (chunk, encoding, done) {
if (format == 'csv') {
var keys = Object.keys(chunk);
csv = [];
if (isFirstChunk == true) {
for (var i = 0; i < keys.length; i++) {
csv.push(keys[i]);
}
}
else {
for (var i = 0; i < keys.length; i++) {
csv.push(chunk[keys[i]]);
}
}
this.push(csv.join());
}
else if (format == 'jsonArray') {
var strChunk = '';
if (isFirstChunk == true) {
strChunk += '[' + JSON.stringify(chunk);
}
else {
strChunk += ',' + JSON.stringify(chunk);
}
this.push(strChunk);
}
else {
this.push(chunk);
}
isFirstChunk = false;
done()
}

liner._flush = function (done) {
if (format == 'jsonArray') {
this.push(']');
}
done()
}

return liner;
}
*/
