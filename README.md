# aws-collection-stream
Takes care of any paginated or unpaginated AWS SDK call, and return the output as a (highland.js)[http://highlandjs.org] stream of objects, handling throttling.

## Installation
```
    npm install highland-aws
```

## callPaginated
```js
    const hAws = require ( 'hAws' );

    return hAws.callPaginated ( {
        serviceName: 'ECS',
        serviceRegion: 'eu-west-1',
        serviceMethod: 'listServices',
        parms: { maxResults: 5 }
    } )
        .errors ( error => console.error ( error ) )
        .each ( console.log );
```

## call
```js
    const hAws = require ( 'hAws' );

    return hAws.call ( {
        serviceName: 'ECS',
        serviceRegion: 'eu-west-1',
        serviceMethod: 'listServices',
        parms: { maxResults: 5 }
    } )
        .errors ( error => console.error ( error ) )
        .each ( console.log );
```
