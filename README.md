Beebotte Python SDK
===================

| what          | where                                  |
|---------------|----------------------------------------|
| overview      | http://beebotte.com/overview           |
| tutorials     | http://beebotte.com/tutorials          |
| apidoc        | http://beebotte.com/docs/restapi       |
| source        | https://github.com/beebotte/bbt_python |

### Bugs / Feature Requests

Think you.ve found a bug? Want to see a new feature in beebotte? Please open an
issue in github. Please provide as much information as possible about the issue type and how to reproduce it.

    https://github.com/beebotte/bbt_python/issues

## Install

Using pip on Linux

    pip install beebotte

Using pip on MS Windows

    python -m pip install beebotte
  
Clone the source code from github
    git clone https://github.com/beebotte/bbt_python.git
  
## Usage
To use the library, you need to be a registered user. If this is not the case, create your account at <http://beebotte.com> and note your access credentials.

As a reminder, Beebotte resource description uses a two levels hierarchy:

* Channel: physical or virtual connected object (an application, an arduino, a coffee machine, etc) providing some resources
* Resource: most elementary part of Beebotte, this is the actual data source (e.g. temperature from a domotics sensor)
  
### Beebotte Constructor
Use your account API and secret keys to initialize Beebotte connector:

    from beebotte import *
    
    _accesskey  = 'YOUR_API_KEY'
    _secretkey  = 'YOUR_SECRET_KEY'
    _hostname   = 'api.beebotte.com'
    bbt = BBT( _accesskey, _secretkey, hostname = _hostname)
    
    ### Alternatively you can authenticate using the channel token
    _token      = 'YOUR_CHANNEL_TOKEN'
    bbt = BBT(token = _token, hostname = _hostname)

### Reading Data
You can read data from one of your channel resources using:

    records = bbt.read("channel1", "resource1", limit = 5 /* read last 5 records */)
    
You can read data from a public channel by specifying the channel owner:

    records = bbt.readPublic("owner", "channel1", "resource1", 5 /* read last 5 records */)
    
### Writing Data
You can write data to a resource of one of your channels using:

    bbt.write("channel1", "resource1", "Hello World")
    
If you have multiple records to write (to one or multiple resources of the same channel), you can use the `bulk write` method:

    bbt.writeBulk("channel1", [
        {"resource": "resource1", "data": "Hello"},
        {"resource": "resource2", "data": "World"}
    ])

### Publishing Data
You can publish data to a channel resource using:

    bbt.publish("any_channel", "any_resource", "Hello Horld")

Published data is transient. It will not be saved to any database; rather, it will be delivered to active subscribers in real time. 
The Publish operations do not require that the channel and resource be actually created. 
They will be considered as virtual: the channel and resource exist as long as you are publishing data to them. 
By default, published data is public, to publish a private message, you need to add `private-` prefix to the channel name like this:

    bbt.publish("private-any_channel", "any_resource", "Hello World")

If you have multiple records to publish (to one or multiple resources of the same channel), you can use the `bulk publish` method:

    bbt.publishBulk("channel1", [
        {"resource": "resource1", "data": "Hello"},
        {"resource": "resource2", "data": "World"}
    ])

### Add Channel
You can add a channel using:

    bbt.addChannel(
      "channel1",
      label = "channel label",
      description = "channel description",
      ispublic = True,
      resources = [{
          "name": "res1",
          "vtype": BBT_Types.String
        }, {
          "name": "res2",
          "label": "resource 2",
          "vtype": BBT_Types.String
        }, {
          "name": "res3",
          "description": "resource 3 description",
          "vtype": BBT_Types.Number,
          "sendOnSubscribe": True
        }
      ]
    )

### Get Channel
You can get the JSON description of a channel using:

    channel = bbt.getChannel("channel1")

If you want to get all your created channels, leave the `channel` parameter to `None`:

    channels = bbt.getChannel()

This will return an array of all your channels. 

### Delete Channel
You can delete a channel using:

    bbt.deleteChannel("channel1")

### Add Resource
You can add a resource to a channel using:

    bbt.addResource(
      channel = "testnamek2",
      name = "res1",
      vtype = BBT_Types.Number,
      label = "resource 1",
      description = "description of resource 1",
      sendOnSubscribe = False
    )

### Get Resource
You can get the JSON description of a resource of a channel using:

    resource = bbt.getResource("channel1", "resource1")

If you want to get all resources of a channel:

    resources = bbt.getResource("channel1")

This will return an array of all the channel resources. 

### Delete Resource
You can delete a resource of a channel using:

    bbt.deleteChannel("channel1", "resource1")

### Channel Token Regeneration
You can regenerate a channel token using:

```python
    channel = bbt.regenerateChannelToken("channel1")
    
    ## New token accessible using
    newToken = channel.token
```

This operation will return the JSON description of the channel with the newly generated Token.
Regenerating a channel token will invalidate the last token in use. 
Don't forget to update your code with the new Token after regeneration.

### Resource Object
The library provides a Resource Class that can be used as follows

    #Create the resource object
    resource = Resource(bbt, "channel1", "resource1")
    
    #Read data
    records = resource.read(limit = 2 /* last 2 records */)
    
    #Read the last written record
    record = resource.recentVal()
    
    #Write data
    resource.write("Hello World")
    
    #Publish data
    resource.publish("Hola amigo")

## License
Copyright 2013 - 2016 Beebotte.

[The MIT License](http://opensource.org/licenses/MIT)
