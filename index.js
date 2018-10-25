const H = require ( 'highland' );
const R = require ( 'ramda' );
const aws = require ( 'aws-sdk' );

const awsCollectionStream = ( {
    serviceObject,
    serviceMethod,
    collectionName,
    parms,
    nextTokenKeyName,
    nextToken
} ) => H.wrapCallback ( R.bind ( serviceObject[serviceMethod], serviceObject ) )( {
    ...parms,
    [nextTokenKeyName]: nextToken
} )
    .errors ( ( error, push ) => error.code === 'ThrottlingException' ? push ( null, 'ThrottlingException' ) : push ( error ) )
    .flatMap ( e => {
        if ( R.type ( e ) === 'String' && e === 'ThrottlingException' ) {
            return H ( ( push, next ) => setTimeout ( () => next (
                H.wrapCallback ( R.bind ( serviceObject[serviceMethod], serviceObject ) )( { ...parms, [nextTokenKeyName]: nextToken } )
            ), 1000 ) );
        }

        return H ( [ e ] );
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
    serviceName,
    serviceRegion = 'us-east-1',
    serviceMethod,
    parms = {}
} ) => {
    const streamError = error => H ( ( push ) => push ( error ) );

    if ( ! serviceName ) {
        return streamError ( 'Please specify an AWS service name (serviceName)' );
    }

    if ( ! serviceMethod ) {
        return streamError ( `Please specify a method to call on AWS.${serviceName} (serviceMethod)` );
    }

    const serviceObject = R.type ( serviceName ) === 'String' ? new aws[serviceName] ( { region: serviceRegion } ) : serviceName;

    return H.wrapCallback ( R.bind ( serviceObject[serviceMethod], serviceObject ) )( parms )
        .errors ( ( error, push ) => error.code === 'ThrottlingException' ? push ( null, 'ThrottlingException' ) : push ( error ) )
        .flatMap ( e => {
            if ( R.type ( e ) === 'String' && e === 'ThrottlingException' ) {
                return H ( ( push, next ) => setTimeout ( () => next (
                    H.wrapCallback ( R.bind ( serviceObject[serviceMethod], serviceObject ) )( parms )
                ), 1000 ) );
            }

            return H ( [ e ] );
        } )
        .flatMap ( result => {
            const collectionName = R.find ( key => R.type ( result[key] ) === 'Array', R.keys ( result ) );
            const nextTokenKeyName = R.find ( key => key.toLowerCase () === 'nexttoken' || key === 'Marker', R.keys ( result ) );

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
