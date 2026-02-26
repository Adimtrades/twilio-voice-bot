const fs = require('fs');
const config = require('./claw.config');

function safeWrite(path, content) {
if (content.length > config.maxFileWriteSize) {
console.error("Blocked large file write attempt");
return;
}
fs.writeFileSync(path, content);
}

module.exports = safeWrite;
