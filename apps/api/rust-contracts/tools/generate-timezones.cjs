// Generate the bounded timezone set with Node 22's Intl and an IANA zoneinfo tree.
// Run with the reference Node image, a read-only zoneinfo mount, and --zoneinfo path.
const fs = require('fs');
const path = require('path');
const root = process.argv[2];
if (!root) throw new Error('zoneinfo directory required');
const names = new Set(Intl.supportedValuesOf('timeZone'));
function walk(dir, prefix = '') {
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const name = prefix + entry.name;
    if (['posix', 'right', 'SystemV'].includes(name)) continue;
    if (entry.isDirectory()) walk(path.join(dir, entry.name), name + '/');
    else if (!entry.name.startsWith('.') && entry.name !== '+VERSION') names.add(name);
  }
}
walk(root);
for (const name of ['UTC', 'UCT', 'Universal', 'Zulu', 'GMT', 'GMT0', 'GMT+0', 'GMT-0', 'Greenwich', 'EST', 'MST', 'HST', 'CST', 'PST', 'CST6CDT', 'EST5EDT', 'MST7MDT', 'PST8PDT', 'SystemV/AST4', 'SystemV/AST4ADT', 'SystemV/CST6', 'SystemV/CST6CDT', 'SystemV/EST5', 'SystemV/EST5EDT', 'SystemV/HST10', 'SystemV/MST7', 'SystemV/MST7MDT', 'SystemV/PST8', 'SystemV/PST8PDT', 'SystemV/YST9', 'SystemV/YST9YDT']) names.add(name);
const valid = [...names].filter(name => { try { new Intl.DateTimeFormat('en', { timeZone: name }); return true; } catch { return false; } }).map(name => name.toLowerCase());
process.stdout.write([...new Set(valid)].sort().join('\n') + '\n');
