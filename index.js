const { MongoClient } = require("mongodb");
// Replace the uri string with your MongoDB deployment's connection string.
let elkClient = null;
let client = null;
function getMongoClient(uri) {
  client = new MongoClient(uri, {
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

async function watch(uri, db, collectoinToWatch) {
  try {
    client = client ? client : await getMongoClient(uri);
    await client.connect();
    const database = client.db(db);
    const collection = database.collection(collectoinToWatch);
    return collection.watch()
  } catch (err) {
    console.error(err)
  }
}

async function watchCollections(collections) {
  let watcher = [];
  for await (collection of collections) {
    watcher.push({
      watcher: await watch(collection.mongoUrl, collection.db , collection.name),
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
  const fullDoucment = document.fullDocument;
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
  if (func) {
    func(obj);
  }
}


// or bundled together in an object
module.exports = {
  watchCollections
};

// watchCollections([{
//   name: 'changeDetactions',
//   mongoUrl: 'mongodb://127.0.0.1:27017',
//   elkUrl: '',
//   index: '',
//   query: null,
//   docType: null,
//   id: '_id',
//   db: 'watcher',
//   func: callIt
// }])


// function callIt(doc) {
//   console.log(doc, 'kk')
// }

// three functions
// watch db and collections 
// create a watchers array
// listen to each watch
// store it some where
// reformat it
// index it in elastic
