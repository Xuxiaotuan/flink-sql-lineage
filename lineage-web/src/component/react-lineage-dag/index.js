const lineageDagBundle = require('./bundle');

const LineageDag = lineageDagBundle.default || lineageDagBundle;
const LineageTable = lineageDagBundle.LineageTable;

export {LineageTable};
export default LineageDag;
