const H = require ( 'highland' );
const R = require ( 'ramda' );
const aws = require ( 'aws-sdk' );

const serviceSpecificEdgeCaseTransform = serviceHostName => {
    return {
        'cloudfront.amazonaws.com': R.prop ( 'DistributionList' )
    }[serviceHostName] || R.identity;
};

const getServiceObject = ( { serviceObj, serviceName, serviceRegion = 'us-east-1' } ) => serviceObj || new aws[serviceName] ( { region: serviceRegion } );

const caughtWrap = ( { serviceObject, serviceMethod } ) => {
    const ecTransform = serviceSpecificEdgeCaseTransform ( serviceObject.endpoint.hostname );

    return parms => {
        return H.wrapCallback ( ( parms, callback ) => {
            try {
                serviceObject[serviceMethod] ( parms, callback );
            } catch ( error ) {
                callback ( error );
            }
        } )( parms )
            .errors ( ( error, push ) => (
                error.code === 'ThrottlingException' ||
                error.code === 'TooManyRequestsException' ||
                error.code === 'Throttling'
            ) ?
                push ( null, 'Throttling' ) :
                push ( error )
            )
            .flatMap ( e => {
                if ( e === 'Throttling' ) {
                    return H ( ( push, next ) => setTimeout ( () => next (
                        caughtWrap ( { serviceObject, serviceMethod } )( parms )
                    ), 1000 ) );
                }

                return H ( [ ecTransform ( e ) ] );
            } );
    };
};

const nextTokenKey = {
    isValid: key => key.toLowerCase () === 'nexttoken' || key === 'NextMarker' || key === 'Marker' || key === 'LastEvaluatedTableName',
    transformForNextRequest: key => {
        return {
            NextMarker: 'Marker',
            LastEvaluatedTableName: 'ExclusiveStartTableName'
        }[key] || key;
    }
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
    [nextTokenKey.transformForNextRequest ( nextTokenKeyName )]: nextToken
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

const validateInput = ( {
    serviceObj,
    serviceName,
    serviceRegion = 'us-east-1',
    serviceMethod,
    parms = {}
} ) => H ( push => {
    if ( ! serviceObj && ! serviceName ) {
        return push ( 'Please specify an AWS service name (serviceName) or fully initialised AWS service object (serviceObj)' );
    }

    if ( ! serviceObj && ! serviceMethod ) {
        return push ( `Please specify a method to call on AWS.${serviceName} (serviceMethod) or fully initialised AWS service object (serviceObj)` );
    }

    push ( null, { serviceObj, serviceName, serviceRegion, serviceMethod, parms } );
    return push ( null, H.nil );
} );


module.exports = {
    callPaginated: args => validateInput ( args )
        .map ( ( { serviceObj, serviceName, serviceRegion, serviceMethod, parms } ) => ( {
            serviceMethod,
            serviceObject: getServiceObject ( { serviceObj, serviceName, serviceRegion } ),
            parms
        } ) )
        .flatMap ( ( { serviceMethod, serviceObject, parms } ) => caughtWrap ( {
            serviceMethod,
            serviceObject
        } )( parms ).map ( result => ( { result, serviceMethod, serviceObject, parms } ) ) )
        .map ( ( { result, serviceMethod, serviceObject, parms } ) => ( {
            result,
            serviceMethod,
            serviceObject,
            parms,
            collectionName: R.find ( key => R.type ( result[key] ) === 'Array', R.keys ( result ) ),
            nextTokenKeyName: R.find ( nextTokenKey.isValid, R.keys ( result ) )
        } ) )
        .flatMap ( ( { result, serviceMethod, serviceObject, parms, collectionName, nextTokenKeyName } ) => nextTokenKeyName ?
            H ( result[collectionName] ).concat ( awsCollectionStream ( {
                serviceObject,
                serviceMethod,
                collectionName,
                parms,
                nextTokenKeyName,
                nextToken: result[nextTokenKeyName]
            } ) ) :
            ( collectionName ?
                H ( result[collectionName] ) :
                H ( push => push ( 'This does not look like a method that returns a collection' ) )
            )
        ),
    call: args => validateInput ( args )
        .flatMap ( ( { serviceObj, serviceName, serviceRegion, serviceMethod, parms } ) => caughtWrap ( {
            serviceMethod,
            serviceObject: getServiceObject ( { serviceObj, serviceName, serviceRegion } )
        } )( parms ) )
};

if ( ! module.parent ) {
    const parms = {
        serviceName: 'ECS',
        serviceRegion: 'eu-west-1',
        serviceMethod: 'listServices',
        parms: { cluster: 'iya-global-cluster', maxResults: 5 }
    };

    return H ( [
        module.exports.callPaginated ( parms ).collect (),
        module.exports.call ( parms )
    ] )
        .parallel ( 2 )
        .collect ()
        .map ( R.zip ( [ 'callPaginated', 'call' ] ) )
        .map ( R.fromPairs )
        .each ( console.log );
}
