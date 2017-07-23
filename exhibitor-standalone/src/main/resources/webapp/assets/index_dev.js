import React from 'react'
import ReactDOM from 'react-dom'
import { Provider } from 'react-redux'
import { createStore, combineReducers } from 'react-redux'
import { AppContainer } from 'react-hot-loader'

import Root from './containers/Root'
import * as reducers from './reducers'

// const reducer = combineReducers({
//  ...reducers
// })

// let store = createStore(reducer)

ReactDOM.render(
    <AppContainer>
        <Root />
    </AppContainer>,
    document.getElementById('root')
)

if (module.hot) {
    module.hot.accept()
}
