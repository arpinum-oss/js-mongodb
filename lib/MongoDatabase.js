'use strict';

let _ = require('lodash');
let Bluebird = require('bluebird');
let MongoClient = require('mongodb').MongoClient;
let Logger = require('mongodb').Logger;

class MongoDatabase {

  constructor(options) {
    this._options = this._parseOptions(options);
  }

  initialize() {
    Logger.setLevel(this._options.databaseLogLevel);
    return MongoClient.connect(this._options.databaseUrl, {promiseLibrary: Bluebird})
      .then(db => {
        this._mongoDb = db;
        this._options.log.info('Connection to database successful');
        return this;
      })
      .catch(error => {
        throw new Error(`Impossible to connect to database: ${error.message}`);
      });
  }

  close() {
    this._mongoDb.close();
  }

  drop() {
    return this._mongoDb.dropDatabase();
  }

  findAll(collectionName, criteria, options) {
    return Bluebird.try(() => {
      let collection = this._mongoDb.collection(collectionName);
      let foundDocuments = collection.find(this._withMongoId(criteria));
      foundDocuments = this._withFindOptionsApplied(foundDocuments, options || {});
      return foundDocuments.toArray().then(docs => {
        return this._withIds(docs);
      });
    });
  }

  _withFindOptionsApplied(foundDocuments, options) {
    if (!_.isEmpty(options.sort)) {
      return foundDocuments.sort(options.sort);
    }
    return foundDocuments;
  }

  findFirst(collectionName, criteria) {
    let collection = this._mongoDb.collection(collectionName);
    return collection.findOne(this._withMongoId(criteria)).then(doc => {
      return this._withId(doc);
    });
  }

  findNear(collectionName, geolocation, options) {
    let collection = this._mongoDb.collection(collectionName);
    return collection.geoNear(geolocation.longitude, geolocation.latitude, options).then(response => {
      let results = _.map(response.results, result => {
        return {
          distance: result.dis,
          document: this._withId(result.obj)
        };
      });
      return results;
    });
  }

  count(collectionName, criteria) {
    let collection = this._mongoDb.collection(collectionName);
    return collection.count(this._withMongoId(criteria));
  }

  add(collectionName, document) {
    let collection = this._mongoDb.collection(collectionName);
    return collection.insertOne(this._withMongoId(document));
  }

  addAll(collectionName, documents) {
    let collection = this._mongoDb.collection(collectionName);
    return collection.insertMany(this._withMongoIds(documents));
  }

  updateFirst(collectionName, criteria, update, options) {
    let collection = this._mongoDb.collection(collectionName);
    return collection.updateOne(this._withMongoId(criteria), this._withMongoId(update), options);
  }

  replaceFirst(collectionName, criteria, document, options) {
    let collection = this._mongoDb.collection(collectionName);
    return collection.replaceOne(this._withMongoId(criteria), this._withMongoId(document), options);
  }

  deleteFirst(collectionName, criteria) {
    let collection = this._mongoDb.collection(collectionName);
    return collection.deleteOne(this._withMongoId(criteria));
  }

  createIndex(collectionName, index) {
    this._options.log.debug('Creating index', collectionName, index);
    let collection = this._mongoDb.collection(collectionName);
    return collection.createIndex(index);
  }

  _withIds(documents) {
    return _.map(documents, this._withId);
  }

  _withId(document) {
    if (!document) {
      return document;
    }
    return _.omit(_.merge({id: document._id}, document), '_id');
  }

  _withMongoIds(documents) {
    return _.map(documents, this._withMongoId);
  }

  _withMongoId(criteria) {
    let $criteria = criteria || {};
    if ($criteria.id) {
      return _.omit(_.merge({_id: $criteria.id}, $criteria), 'id');
    }
    return $criteria;
  }

  _parseOptions(options) {
    return _.defaults(options || {}, {
      log: console,
      databaseLogLevel: 'info',
      databaseUrl: 'to define'
    });
  }
}

module.exports = MongoDatabase;
