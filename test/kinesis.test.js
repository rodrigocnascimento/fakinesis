var expect = require('chai').expect;

var Kinesis = require("../kinesis");

describe('Kinesis.JS', function() {

    var kinesis;

    beforeEach(function (done) {

        kinesis = new Kinesis();

        done();

    });

    describe('#require()', function() {

        it('Deve retornar um objeto Kinesis', function() {

            expect(kinesis).to.be.an.instanceof(Kinesis);

        });

    });

    describe('#_data getter e setter', function () {

        it('Deve existir um método setter', function () {

            expect(kinesis._setData).to.be.a('function');

        });

        it('Deve existir um método getter', function () {

            expect(kinesis._getData).to.be.a('function');

        });

        it('Método get deve retornar o valor padrão', function () {

            var def = kinesis._getData();

            expect(def).to.be.equal('');

        });

        it('Método set deve converter um objeto em base64', function () {

            var obj = {
                'foo'    : 'bar',
                'number' : 123
            };

            kinesis._setData(obj);

            var objString = new Buffer(kinesis._getData(), 'base64').toString();
            var jsonObj   = JSON.parse(objString);

            expect(obj).to.be.deep.equal(jsonObj);

        });

    });

    describe('#_partitionKey getter e setter', function () {

        it('Deve existir um método setter', function () {

            expect(kinesis._setPartitionKey).to.be.a('function');

        });

        it('Deve existir um método getter', function () {

            expect(kinesis.getPartitionKey).to.be.a('function');

        });

        it('Método get deve retornar o valor padrão', function () {

            var def = kinesis.getPartitionKey();

            expect(def).to.be.equal('');

        });

        it('Método set deve definir o valor correto', function () {

            var val = "teste";

            kinesis._setPartitionKey(val);

            var partitionKey = kinesis.getPartitionKey();

            expect(partitionKey).to.be.equal(val);

        });

    });

    describe('#showLambdas()', function () {

        it('Deve existir', function () {

            expect(kinesis.showLambdas).to.be.a('function');

        });

        it('Deve exibir informações sobre os Lambdas que estão ouvindo os eventos do Kinesis', function () {

            var status = kinesis.showLambdas();

            expect(status).to.be.an('object');

            expect(status.numberOfLambdas).to.exist;
            expect(status.numberOfLambdas).to.be.a('number');
            expect(status.numberOfLambdas).to.be.equal(0);

            expect(status.lambdas).to.exist;
            expect(status.lambdas).to.be.an('array');
            expect(status.lambdas).to.be.empty;

        });

    });

    describe('#addLambda()', function () {

        it('Deve existir', function () {

            expect(kinesis.addLambda).to.be.a('function');

        });

        it('Deve aceitar uma função como parametro', function () {

            var par = function () {};

            expect(function () { kinesis.addLambda(par); }).to.not.throw('Parameter is not a function');

        });

        it('Deve adicionar uma função a pilha de funções', function () {

            var h1 = function handler1() {};

            kinesis.addLambda(h1);

            var status = kinesis.showLambdas();

            expect(status.numberOfLambdas).to.be.equal(1);

        });

        it('Deve permitir adição de multiplas funções', function () {

            var h1 = function handler1() {};
            var h2 = function handler2() {};

            kinesis.addLambda(h1);
            kinesis.addLambda(h2);

            var status = kinesis.showLambdas();

            expect(status.numberOfLambdas).to.be.equal(2);

        });

    });

    describe('#putRecord()', function () {

        it('Deve existir', function () {

            expect(kinesis.putRecord).to.be.a('function');

        });

        it('Não deve permitir executar o método sem nenhum parâmetro', function () {

            kinesis.putRecord(null, function (err) {

                expect(function () {

                    if (err) {
                        throw err;
                    }

                }).to.throw("Parameter can't be empty");

            });

        });

        it('Deve executar o Lambda após postar o evento', function () {

            var executou = false;

            var l1 = function lambda1() {

                executou = true;

            };

            kinesis.addLambda(l1);

            var record = {
                "Data"         : JSON.stringify({ "mensagem": "teste" }),
                "PartitionKey" : "mocha",
                "StreamName"   : "mocha"
            };

            kinesis.putRecord(record, function (err) {

                if (err) {
                    throw err;
                }

                expect(executou).to.be.true;

            });

        });

        it('Deve enviar para o Lambda o evento correto', function () {

            var debug  = {};
            var record = {
                "Data"         : JSON.stringify({ "mensagem": "sucesso" }),
                "PartitionKey" : "casoDeTeste",
                "StreamName"   : "mocha"
            };

            var l1 = function lambda1(event) {

                debug = event;

            };

            kinesis.addLambda(l1);

            kinesis.putRecord(record, function (err) {

                if (err) {
                    throw err;
                }

                expect(debug.Records).to.exist;
                expect(debug.Records).to.be.an('array');

                expect(debug.Records[0].kinesis).to.exist;
                expect(debug.Records[0].kinesis).to.be.an('object');

                expect(debug.Records[0].kinesis.partitionKey).to.exist;
                expect(debug.Records[0].kinesis.partitionKey).to.be.equal(record.PartitionKey);

                expect(debug.Records[0].kinesis.data).to.exist;

                var objString = new Buffer(debug.Records[0].kinesis.data, 'base64').toString();
                var jsonObj   = JSON.parse(objString);

                expect(jsonObj).to.be.deep.equal(record.Data);

            });

        });

        it('Deve emitir o mesmo evento para todos os Lambdas', function () {

            var debugL1 = {};
            var debugL2 = {};
            var record  = {
                "Data"         : JSON.stringify({ "mensagem": "sucesso" }),
                "PartitionKey" : "casoDeTeste",
                "StreamName"   : "mocha"
            };

            var l1 = function lambda1(event1) {

                debugL1 = event1;

            };

            var l2 = function lambda2(event2) {

                debugL2 = event2;

            };

            kinesis.addLambda(l1);
            kinesis.addLambda(l2);

            kinesis.putRecord(record, function (err) {

                if (err) {
                    throw err;
                }

                expect(debugL1).to.be.deep.equal(debugL2);

            });

        });

    });

});