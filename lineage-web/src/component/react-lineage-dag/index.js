const lineageDagBundle = require('./bundle.cjs');

const LineageDag = lineageDagBundle.default || lineageDagBundle;
const LineageTable = lineageDagBundle.LineageTable;

export {LineageTable};
export default LineageDag;
