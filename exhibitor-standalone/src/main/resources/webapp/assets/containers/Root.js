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
  	  				<Col xs={3}>
  	  					<Header />
		 				<Tabs />
  	  				</Col>
  	    			<Col xs={9}> 
		 				
		 			</Col>
		 		</Row>
		 	</Grid>
		 </div>
	  )
  }
}

export default Root
