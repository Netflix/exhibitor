import React from 'react'
import Tabs from '../components/Tabs';
import Header from '../components/Header';
import { PageHeader, Jumbotron, Grid, Col, Row } from 'react-bootstrap';

class Root extends React.Component {
  render() {
    return( 
         <div>
            <Grid>
                <Row>
                    <Col>
                        <Header />
                    </Col>
                    <Col xs={2}>
                        <Tabs />
                    </Col>
                    <Col xs={10}> 
                    </Col>
                </Row>
            </Grid>
         </div>
      )
  }
}

export default Root
