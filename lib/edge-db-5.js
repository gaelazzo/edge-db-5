exports.getCompiler = function () {
	return process.env.EDGE_SQL_NATIVE || (require('path').join(__dirname,'edge-db-5.dll'));
};