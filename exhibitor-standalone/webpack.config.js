const webpack = require('webpack')
const path = require('path')

module.exports = function(env) {
  return {
    devtool: 'source-map',
    entry: {
      'app': [
        'babel-polyfill',
        'react-hot-loader/patch',
        './src/main/resources/webapp/assets/index_dev'
      ]
    },
    output: {
      path: path.resolve(__dirname, 'target/' + 'classes' + '/assets'),
      filename: path.normalize('[name].js'),
      publicPath: 'assets/'
    },
    module: {
      rules: [
        {
          test: /\.css$/,
          use: [ 'style-loader', 'css-loader' ]
        },
        { 
          test: /\.js$/, 
          exclude: /node_modules/, 
          loader: 'babel-loader' 
        }   
      ]
    }
  }
}
