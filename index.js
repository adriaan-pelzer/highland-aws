const H = require ( 'highland' );
const R = require ( 'ramda' );
const aws = require ( 'aws-sdk' );

const caughtWrap = ( { serviceObject, serviceMethod } ) => {
    return parms => {
        return H.wrapCallback ( ( parms, callback ) => {
            try {
                serviceObject[serviceMethod] ( parms, callback );
            } catch ( error ) {
                callback ( error );
            }
        } )( parms )
            .errors ( ( error, push ) => ( error.code === 'ThrottlingException' || error.code === 'TooManyRequestsException' ) ?
                push ( null, 'ThrottlingException' ) :
                push ( error ) )
            .flatMap ( e => {
                if ( R.type ( e ) === 'String' && e === 'ThrottlingException' ) {
                    return H ( ( push, next ) => setTimeout ( () => next (
                        caughtWrap ( { serviceObject, serviceMethod } )( parms )
                    ), 1000 ) );
                }

                return H ( [ e ] );
            } );
    };
};

const awsCollectionStream = ( {
    serviceObject,
    serviceMethod,
    collectionName,
    parms,
    nextTokenKeyName,
    nextToken
} ) => caughtWrap ( { serviceMethod, serviceObject } )( {
    ...parms,
    [nextTokenKeyName === 'NextMarker' ? 'Marker' : nextTokenKeyName]: nextToken
} )
    .flatMap ( ( { [collectionName]: collection, [nextTokenKeyName]: nextToken } ) => nextToken ? H ( collection )
        .concat ( awsCollectionStream ( {
            serviceObject,
            serviceMethod,
            collectionName,
            parms,
            nextTokenKeyName,
            nextToken
        } ) ) : H ( collection )
    );

module.exports = ( {
    serviceObj,
    serviceName,
    serviceRegion = 'us-east-1',
    serviceMethod,
    parms = {}
} ) => {
    const streamError = error => H ( ( push ) => push ( error ) );

    if ( ! serviceObj && ! serviceName ) {
        return streamError ( 'Please specify an AWS service name (serviceName) or fully initialised AWS service object (serviceObj)' );
    }

    if ( ! serviceObj && ! serviceMethod ) {
        return streamError ( `Please specify a method to call on AWS.${serviceName} (serviceMethod) or fully initialised AWS service object (serviceObj)` );
    }

    const serviceObject = serviceObj || new aws[serviceName] ( { region: serviceRegion } );

    return caughtWrap ( { serviceMethod, serviceObject } )( parms )
        .flatMap ( result => {
            const collectionName = R.find ( key => R.type ( result[key] ) === 'Array', R.keys ( result ) );
            const nextTokenKeyName = R.find ( key => key.toLowerCase () === 'nexttoken' || key === 'NextMarker' || key === 'Marker', R.keys ( result ) );

            if ( nextTokenKeyName ) {
                return H ( result[collectionName] ).concat ( awsCollectionStream ( {
                    serviceObject,
                    serviceMethod,
                    collectionName,
                    parms,
                    nextTokenKeyName,
                    nextToken: result[nextTokenKeyName]
                } ) );
            }

            if ( collectionName ) {
                return H ( result[collectionName] );
            }

            return streamError ( 'This does not look like a method that returns a collection' );
        } )
};

if ( ! module.parent ) {
    return module.exports ( { serviceName: 'ECS', serviceRegion: 'eu-west-1', serviceMethod: 'listServices', parms: { cluster: 'default', maxResults: 2 } } )
        .each ( console.log );
}
