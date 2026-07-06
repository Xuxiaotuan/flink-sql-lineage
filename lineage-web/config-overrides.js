const {
  override,
  disableEsLint,
  overrideDevServer,
  watchAll,
  useBabelRc,
  addWebpackAlias,
} = require("customize-cra")
const path = require('path')
const proxyTarget = process.env.REACT_APP_PROXY_TARGET || process.env.PROXY_TARGET || 'http://127.0.0.1:8194'

const stylus = () => config => {
  const stylusLoader = {
    test: /\.styl$/,
    include: [path.resolve(__dirname, 'src')],
    exclude: /node_modules/,
    sideEffects: true,
    use: [
      {
        loader: 'style-loader',
      },
      {
        loader: 'css-loader',
      },
      {
        loader: 'stylus-loader',
        options: {
          sourceMap: true,
        },
      },
    ],
  }
  const oneOf = config.module.rules.find(rule => rule.oneOf).oneOf
  oneOf.unshift(stylusLoader)
  return config
}

module.exports = {
  webpack: override(
    disableEsLint(),
    stylus(),
    useBabelRc(),
    addWebpackAlias({
      '@common': path.resolve(__dirname, './src/common')
    })
  ),
  devServer: overrideDevServer(
    watchAll(),
    () => {
      return  {
          proxy: {
            '*': {
              target: proxyTarget,
              logLevel:'debug',
              changeOrigin: true,
            },
          }
        }
    }
  )
};
