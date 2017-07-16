import React from 'react'
import { PageHeader, Jumbotron, Grid, Col, Row } from 'react-bootstrap';
import styles from '../css/header.css'

class Header extends React.Component {
    render() {
        return(
        <Jumbotron className={styles.jumbotron}>
            <h3>Exhibitor for Zookeeper
            </h3>
        </Jumbotron>
    )
  }
}

export default Header
