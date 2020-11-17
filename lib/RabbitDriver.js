
var process      = require('process');
var rabbit       = require('rabbit.js');
var Promise      = require('bluebird');
var EventEmitter = require('events').EventEmitter;
var util         = require('util');
var AmqpMessage  = require('./AmqpMessage');


var DriverPushWorker = function(config, options, startClient) {
    var self         = this;
    self.context     = null;
    self.server      = null;
    self.client      = null;
    self.channel     = options.name;


    self.init = function() {
        return new Promise(function(resolve, reject) {
            self.context = rabbit.createContext(config.rabbitmq.hostname);
            self.context.on('ready', function() {
                self.server = self.context.socket('PUSH', options.server);

                self.server.connect(self.channel, function() {

                    if(startClient) {
                        self.client = self.context.socket('WORKER', options.client);
                        self.client.connect(self.channel, function() {
                            resolve(self.context);
                        });

                        self.client.on('data', function(msg) {
                            msg = JSON.parse(msg);
                            var amqpm = new AmqpMessage();
                            amqpm.fromObject(msg);
                            self.emit('data', amqpm, self.client);
                        });
                    } else {
                        resolve(self.context);
                    }
                });
            });

        });
    };

    self.publish = function(message) {
        self._publish(self.channel, message);
    };
    self._publish = function(channel, message) {
        self.server.write(JSON.stringify(message.toObject()));
    };
};



var DriverPushPull = function(config, options, startClient) {
    var self         = this;
    self.context     = null;
    self.server      = null;
    self.client      = null;
    self.channel     = options.name;
    self.startClient = options.startClient===true? true: false;

    self.init = function() {
        return new Promise(function(resolve, reject) {
            self.context = rabbit.createContext(config.rabbitmq.hostname);
            self.context.on('ready', function() {
                // self.server = self.context.socket('PUSH', options);
                // self.client = self.context.socket('PULL', options);


                // self.server.connect(self.channel, function() {

                //     if(self.startClient) {
                //         self.client.connect(self.channel, function() {
                //             resolve(self.context);
                //         });
                //     } else {
                //         resolve(self.context);
                //     }
                // });

                self.server = self.context.socket('PUSH', options.server);

                self.server.connect(self.channel, function() {
                    if(startClient) {
                        self.client = self.context.socket('PULL', options.client);
                        self.client.connect(self.channel, function() {
                            resolve(self.context);
                        });

                        self.client.on('data', function(msg) {
                            msg = JSON.parse(msg);
                            var amqpm = new AmqpMessage();
                            amqpm.fromObject(msg);
                            self.emit('data', amqpm , self.client);
                        });
                    } else {
                        resolve(self.context);
                    }
                });





            });

        });
    };
    self.publish = function(message) {
        self._publish(self.channel, message);
    };
    self._publish = function(channel, message) {
        self.server.write(JSON.stringify(message.toObject()));
    };
};

var DriverPubSub = function(config, options, startClient    ) {
    var self         = this;
    self.context     = null;
    self.server      = null;
    self.client      = null;
    self.channel     = options.name;
    self.startClient = options.startClient===true? true: false;

    self.init = function() {
        return new Promise(function(resolve, reject) {
            self.context = rabbit.createContext(config.rabbitmq.hostname);
            self.context.on('ready', function() {

                // console.log("CONNECTED pubsub");
                // self.server = self.context.socket('PUB', options);
                // self.client = self.context.socket('SUB', options);

                // self.server.connect(self.channel, function() {

                //     if(self.startClient) {
                //         self.client.connect(self.channel, function() {
                //             resolve(self.context);
                //         });
                //     } else {
                //         resolve(self.context);
                //     }
                // });

                console.log(options);

                self.server = self.context.socket('PUB', options.server);

                self.server.connect(self.channel, function() {

                    if(startClient) {
                        self.client = self.context.socket('SUB', options.client);
                        self.client.connect(self.channel, function() {
                            resolve(self.context);
                        });

                        self.client.on('data', function(msg) {
                            msg = JSON.parse(msg);
                            var amqpm = new AmqpMessage();
                            amqpm.fromObject(msg);
                            self.emit('data', amqpm , self.client);
                        });
                    } else {
                        resolve(self.context);
                    }
                });
            });

        });
    };
    self.publish = function(message) {
        self._publish(self.channel, message);
    };
    self._publish = function(channel, message) {
        self.server.write(JSON.stringify(message.toObject()));
    };
};

util.inherits(DriverPushWorker, EventEmitter);
util.inherits(DriverPubSub, EventEmitter);
util.inherits(DriverPushPull, DriverPushWorker);

module.exports = {
    pushworker : DriverPushWorker,
    pushpull   : DriverPushPull,
    pubsub     : DriverPubSub
};
