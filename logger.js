const log = (color, data) => console.log(color, data);

exports.log = data => log("\x1b[0m", data);
exports.error = data => log("\x1b[31m", data);
exports.success = data => log("\x1b[32m", data);
exports.warn = data => log("\x1b[33m", data);
