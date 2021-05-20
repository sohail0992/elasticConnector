const { MongoClient } = require("mongodb");
// Replace the uri string with your MongoDB deployment's connection string.
let elkClient = null;
let client = null;
async function getMongoClient(uri) {
  client = await new MongoClient(uri, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  });
  return client
}

function getElasticConnection(url) {
  var elasticsearch = require('elasticsearch');
  var elkClient = new elasticsearch.Client({
    host: url,
    log: 'trace'
  });
  return elkClient;
}

async function watch(db, collectoinToWatch) {
  try {
    const database = client.db(db);
    const collection = database.collection(collectoinToWatch);
    return collection.watch({fullDoucment: 'updateLookup'})
  } catch (err) {
    console.error(err)
  }
}

async function watchCollections(collections, mongoUrl) {
  let watcher = [];
  client = client ? client : await getMongoClient(mongoUrl);
  await client.connect();
  for await (collection of collections) {
    watcher.push({
      watcher: await watch(collection.db , collection.name),
      query: collection.query,
      index: collection.index,
      docType: collection.docType,
      id: collection.id,
      url: '',
      func: collection.func
    });
  }
  watcher.forEach(each => {
    each && each.watcher ? each.watcher.on('change', next => {
      reStructure(next, each.query, each.index, each.docType, each.id, each.func, each.elkUrl);
      console.log(next, 'k')
    }) : null;
  }) 
}

async function reStructure(document, query = null, index = null, docType, id = null, func = null,elkUrl = '') {
  try {
    const fullDoucment = document.fullDocument ? document.fullDocument : document.documentKey;
    if (!id) {
      id = Object.keys(document.documentKey)[0];
    }
    if (typeof fullDoucment[id] !== 'string') {
      fullDoucment[id] = fullDoucment[id].toString();
    } 
    const obj = {
      index: index,
      type: docType,
      id: fullDoucment[id],
      body: fullDoucment
    };
    if (index && elkUrl) {
      elkClient = elkClient ? elkClient : getElasticConnection(elkUrl);
      try {
        const response = await elkClient.update(obj);
        console.log(response, '')
      } catch (error) {
        console.error('elastic indexing error', JSON.stringify(error));
      }
    }
    if (func && func.length > 0) {
      func.forEach(each => {
        each(obj)
      })
    }
  } catch (error) {
    console.error('got error while restructuring document', JSON.stringify(error));
  }
}


// or bundled together in an object
module.exports = {
  watchCollections
};

watchCollections([{
  name: 'changeDetactions',
  elkUrl: '',
  index: '',
  query: null,
  docType: null,
  id: '_id',
  db: 'watcher',
  func: [callIt, donot]
}, 
{
  name: 'changes',
  elkUrl: '',
  index: '',
  query: null,
  docType: null,
  id: '_id',
  db: 'watcher',
  func: [callIt, donot]
}], 'mongodb://127.0.0.1:27017')


function callIt(doc) {
  console.log(doc, 'kk')
}

function donot(doc) {
  console.log(doc, 'kk')
}
// three functions
// watch db and collections 
// create a watchers array
// listen to each watch
// store it some where
// reformat it
// index it in elastic
