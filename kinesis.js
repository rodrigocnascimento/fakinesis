var EventEmitter = require('events');
var _            = require('lodash');

function Kinesis() {

    this._data = "";

    this._kinesisSchemaVersion = "1.0";

    this._partitionKey = "";

    this._ee = new EventEmitter();

    this._setPartitionKey = function (partitionKey) {

        this._partitionKey = partitionKey;

    };

    this._setData = function (data) {

        this._data = new Buffer(JSON.stringify(data)).toString('base64');

    };

    this._getData = function () {

        return this._data;

    };

}

Kinesis.prototype.getPartitionKey = function () {

    return this._partitionKey;

};

Kinesis.prototype.addLambda = function (lambda) {

    if (typeof lambda !== 'function') {
        throw new Error('Parameter is not a function');
    }

    this._ee.on('event', function(record) {

        lambda(_.cloneDeep(record), {}, function () {});

    });

};

Kinesis.prototype.showLambdas = function () {

    return {
        "numberOfLambdas" : EventEmitter.listenerCount(this._ee, 'event'),
        "lambdas"         : this._ee.listeners('event')
    };

};

Kinesis.prototype.putRecord = function (data, callback) {

    try {
        if (_.isEmpty(data)) {
            throw new Error("Parameter can't be empty");
        }

        this._setData(data.Data);
        this._setPartitionKey(data.PartitionKey);

        var record = {
            "Records": [
                {
                    "kinesis": {
                        "kinesisSchemaVersion": this._kinesisSchemaVersion,
                        "partitionKey": this.getPartitionKey(),
                        "sequenceNumber": "49570170006194171189355348232277756931531565503615074306",
                        "data": this._getData(),
                        "approximateArrivalTimestamp": Date.now()
                    },
                    "eventSource": "aws:kinesis",
                    "eventVersion": "1.0",
                    "eventID": "shardId-000000000000:49570170006194171189355348232277756931531565503615074306",
                    "eventName": "aws:kinesis:record",
                    "invokeIdentityArn": "arn:aws:iam::891324503292:role/APIGatewayLambdaExecRole",
                    "awsRegion": "us-east-1",
                    "eventSourceARN": "arn:aws:kinesis:us-east-1:891324503292:stream/polo-monitoring-events"
                }
            ]
        };

        this._ee.emit('event', record);

        if (typeof callback === "function") {
            callback(null);
        }
    } catch (err){
        if (typeof callback === "function") {
            callback(err);
        }
    }

};

module.exports = Kinesis;