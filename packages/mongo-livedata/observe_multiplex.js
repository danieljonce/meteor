ObserveMultiplex = function (ordered) {
  var self = this;

  self._ordered = ordered;
  self._queue = new Meteor._SynchronousQueue();
  self._handles = {};
  self._ready = false;
  self._readyFuture = new Future;
  self._cache = new LocalCollection._CachingChangeObserver({ordered: ordered});

  _.each(self._callbackNames(), function (callbackName) {
    self[callbackName] = function (/* ... */) {
      self._applyCallback(callbackName, _.toArray(arguments));
    };
  });
};

_.extend(ObserveMultiplex.prototype, {
  addHandleAndSendInitialAdds: function (handle) {
    var self = this;
    self._queue.runTask(function () {
      if (self._ready)
        self._sendAdds(handle);
      // XXX do we still have these IDs?
      self._handles[handle._observeHandleId] = handle;
    });
    // *outside* the task, since otherwise we'd deadlock
    self._waitUntilReady();
  },
  _waitUntilReady: function (handle) {
    var self = this;
    self._readyFuture.wait();
  },
  ready: function () {
    var self = this;
    self._queue.queueTask(function () {
      if (self._ready)
        throw Error("can't make ObserveMultiplex ready twice!");
      self._ready = true;
      _.each(self._handles, function (handle) {
        self._sendAdds(handle);
      });
      self._readyFuture.return();
    });
  },
  _callbackNames: function () {
    var self = this;
    if (self.ordered)
      return ["addedBefore", "changed", "movedBefore", "removed"];
    else
      return ["added", "changed", "removed"];
  },
  _applyCallback: function (callbackName, args) {
    var self = this;
    self._queue.queueTask(function () {
      // First, apply the change to the cache.
      // XXX We could make applyChange callbacks promise not to hang on to any
      // state from their arguments (assuming that their supplied callbacks
      // don't) and skip this clone. Currently 'changed' hangs on to state
      // though.
      self._cache.applyChange[callbackName].apply(null, EJSON.clone(args));
      // If we haven't finished the initial adds, we have nothing more to do.
      if (!self._ready)
        return;
      // Now multiplex the callbacks out to all observe handles. It's OK if
      // these calls yield; since we're inside a task, no other use of our queue
      // can continue until these are done. (Notably, we can't add any more
      // handles to self._handles.)
      _.each(self._handles, function (handle) {
        // clone arguments so that callbacks can mutate their arguments
        handle[callbackName].apply(null, EJSON.clone(args));
      });
    });
  },
  _sendAdds: function (handle) {
    var self = this;
    if (self._queue.safeToRunTask())
      throw Error("_sendAdds may only be called from within a task!");
    // XXX this means we don't support "add" with "movedBefore". is that ok?
    var add = self._ordered ? handle.addedBefore : handle.added;
    if (!add)
      return;
    // note: docs may be an _IdMap or an OrderedDict
    self._cache.docs.forEach(function (doc, id) {
      var fields = EJSON.clone(doc);
      delete fields._id;
      if (self._ordered)
        add(id, fields, null); // we're going in order, so add at end
      else
        add(id, fields);
    });
  }
});
