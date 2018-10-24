# aws-collection-stream
Takes care of any paginated AWS SDK call, and return the output as a highland.js stream of objects.

```
    npm install aws-collection-stream
```

```js
    const awsCollectionStream = require ( 'aws-collection-stream' );

    return awsCollectionStream ( {
        serviceName: 'ECS',
        serviceRegion: 'eu-west-1',
        serviceMethod: 'listServices',
        parms: { maxResults: 2 }
    } )
        .errors ( error => console.error ( error ) )
        .each ( console.log );
```
