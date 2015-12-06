(function() {
    "use strict";

    var AmqpMessage = function(type, body) {
        var self  = this;

        self.type = type;
        self.body = body;


        self.toObject = function() {
            return {
                type: self.type,
                body: self.body
            };
        };

        self.fromObject = function(obj) {
            self.type = obj.type;
            self.body = obj.body;
        };
    };

    module.exports = AmqpMessage;

})();
