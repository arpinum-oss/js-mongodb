'use strict';

let MongoDatabase = require('./MongoDatabase');

class TestInDatabase {

  constructor(options) {
    this.database = new MongoDatabase(options);
  }

  initialize() {
    beforeEach(() => {
      this._initializePromise = this._initializePromise || this.database.initialize();
      return this._initializePromise;
    });

    afterEach(() => {
      return this.database.drop();
    });
    return this;
  }
}

module.exports = TestInDatabase;
