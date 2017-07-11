'use strict'

const amqp = require('amqplib');
const bunyan = require('bunyan');
const uuid = require('node-uuid');
const fs = require('fs');

let log = null

// queue address and port
let brokerAddr = '';

//socket options
let socketOptions = null;

let reconnectTime = 2000;

// control variable used to register SIGINT event listener
let registered = false;

// control variable used to check if disconnection was trigger by the user
let exit = false;

// will hold ampqlib's connection obejct
let connection = null;

// will hold ampqlib's channel object
let channel = null;

// will hold the consumer configs. Consumer configs were based in HapiJS and are config objects
// that have all consumer configuration params
let consumerConfigurations = [];

// called on fatal error, will ternimate execution
let bail = function (err) {
    log.error(err);

    if (connection) {
        connection.close(function () {
            process.exit(1);
        });
    } else {
        process.exit(1);
    }
}

// open new conection to queue
let mqConnect = function () {

    log.info('connecting to queue...');

    amqp.connect(brokerAddr, socketOptions).then((conn) => {

        log.info('connected to queue');

        if (!registered) {
            registered = true;
            // register event to clone connection on interrupt signal
            process.once('SIGINT', () => {
                exit = true;
                conn.close();
            });
        }

        connection = conn;

        // register event to log connection errors
        conn.on('error', function (err) {
            log.error(`connection error: ${err}`);
        });

        // register event on connection close. In case of error, connection close is
        // also called so there is no need to try to reconnect on error event.
        conn.on('close', function () {
            log.info(`connection closed`);

            // Only try to reconnect if the user didn't send SIGINT
            if (!exit) {
                mqConnect();
            }
        });

        log.info('creating channel...');

        return conn.createChannel();

    }).then((ch) => {

        log.info('channel created');

        channel = ch;

        channel.on('error', function (err) {
            log.error(`channel error: ${err}`);
        });

        channel.on('close', function () {
            log.info(`channel closed`);
        });

        log.info('adding consumers...');
        for (let cc of consumerConfigurations) {
            consume(cc);
        }

    }).catch((err) => {
        log.error(`mqConnect error: ${err}`);
        setTimeout(mqConnect, reconnectTime);
    });

}

let replyToQueue = function (msg, retMsg) {
    channel.sendToQueue(msg.properties.replyTo,
        Buffer.from(retMsg), {
            correlationId: msg.properties.correlationId
        });
    channel.ack(msg);
}

let consume = function (consumerConf) {

    if (!typeof consumerConf === 'object') {
        bail(new Error('consume paramater must be an object'));
        return
    }

    if (!consumerConf.queue) {
        bail(new Error('No queue'));
        return
    }

    if (!consumerConf.queue.name) {
        bail(new Error('queue name required'));
        return
    }

    if (!consumerConf.hasOwnProperty('async')) {
        bail(new Error('async is required'));
        return
    }

    if (!consumerConf.hasOwnProperty('handler')) {
        bail(new Error('handler required'));
        return
    }

    if (!typeof consumerConf.handler === 'function') {
        bail(new Error('handler must be a function'));
        return
    }

    if (!consumerConf.queue.options) {
        consumerConf.queue.options = {
            durable: false
        };
    }

    if (!consumerConf.queue.options.durable) {
        consumerConf.queue.durable = false;
    }

    if (!connection) {
        bail(new Error('No connection'));
        return
    }

    if (!channel) {
        bail(new Error('No channel'));
        return
    }

    let ok = channel.assertQueue(consumerConf.queue.name, consumerConf.queue.options);

    ok = ok.then(() => {
        channel.prefetch(1);
        if (consumerConf.async) {
            return channel.consume(consumerConf.queue.name, asyncReply);
        }
        return channel.consume(consumerConf.queue.name, syncReply);
    }).catch((err) => {
        log.error(err);
    });

    function syncReply(msg) {

        try {
            let msgObj = JSON.parse(msg.content.toString());

            consumerConf.handler({
                payload: msgObj,
                rawMessage: msg
            }, (retObj) => {

                if (retObj) {
                    try {
                        let retMsg = JSON.stringify(retObj);

                        replyToQueue(msg, retMsg);

                    } catch (err) {
                        log.error(err);
                        replyToQueue(msg, JSON.stringify({
                            error: err
                        }));
                    }
                }

            });

        } catch (err) {
            log.error(err)
            replyToQueue(msg, JSON.stringify({
                error: err
            }));
        }

    }

    function asyncReply(msg) {

        try {
            let msgObj = JSON.parse(msg.content.toString());

            consumerConf.handler({
                payload: msgObj,
                rawMessage: msg
            });

        } catch (err) {
            log.error(err);
        }

    }

    return ok.then(() => {
        log.debug('consumer added')
    }).catch((err) => {
        log.error(err);
    });

};

//begging of public methods
exports = module.exports = {};

exports.broker = function (addr, sockOpt, reconnTimeout) {

    if (!addr) {
        addr = 'amqps://localhost';
    }

    if (!sockOpt) {

        if (!log) {
            log = bunyan.createLogger({
                name: 'hyper-queue'
            });
        }

        bail(new Error('TLS configurations are required for connection.'));
        return
    }

    if (reconnTimeout) {
        if (reconnTimeout !== parseInt(reconnTimeout, 10)) {

            if (!log) {
                log = bunyan.createLogger({
                    name: 'hyper-queue'
                });
            }

            bail(new Error('Reconnection time must be an integer.'));
            return

        }

        reconnectTime = reconnTimeout;
    }

    let so = {
        cert: fs.readFileSync(sockOpt.cert),
        key: fs.readFileSync(sockOpt.key),
        passphrase: sockOpt.passphrase,
        ca: [fs.readFileSync(sockOpt.ca)]
    };

    brokerAddr = addr;

    socketOptions = so;

};

exports.logger = function (logger) {
    log = logger;
}

exports.registerCconsumers = function (confs) {

    if (!confs) {
        bail(new Error('consumers expects an array of objects'));
        return
    }

    if (confs.length === 0) {
        bail(new Error('consumers expects an array of objects with at least one object'));
        return
    }

    consumerConfigurations.push(...confs);

};

exports.sendSync = function (queue, sendObj) {

    let p = new Promise;

    let corrId = uuid();

    function maybeAnswer(msg) {
        if (msg.properties.correlationId === corrId) {

            try {

                let obj = JSON.parse(msg.content.toString());

                p.resolve(obj);

            } catch (e) {
                p.reject(e);
            }

        }
    }

    let ok = ch.assertQueue('', {
            exclusive: true
        })
        .then(function (qok) {
            return qok.queue;
        });

    ok = ok.then(function (replyQueue) {
        return ch.consume(replyQueue, maybeAnswer, {
                noAck: true
            })
            .then(function () {
                return replyQueue;
            });
    });

    ok = ok.then(function (replyQueue) {

        try {

            let objStr = JSON.stringify(sendObj);

            channel.sendToQueue(queue, Buffer.from(objStr), {
                correlationId: corrId,
                replyTo: replyQueue
            });

        } catch (e) {
            p.reject(e);
        }

    });

};

exports.sendAsync = function (queue, sendObj) {

    try {
        let sendStr = JSON.stringify(sendObj);

        channel.sendToQueue(queue, Buffer.from(sendStr));

    } catch (err) {
        log.error(err)
    }

};

exports.connect = function () {

    if (!log) {
        log = bunyan.createLogger({
            name: 'hyper-queue'
        });
    }

    mqConnect();

};