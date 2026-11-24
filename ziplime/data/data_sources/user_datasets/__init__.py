"""Private user datasets, ``dataset://<name>``: one Parquet file a user uploaded, read only by them.

    rows = await data.history(assets=context.universe, fields=["score"], bar_count=20,
                              data_source="dataset://my-signals")

The file lives at ``s3://<bucket>/users/<user_id>/<name>.parquet``. Uploading, validating and
managing it is the platform's job; this module only reads it during a backtest, under exactly the
point-in-time rules of an ``hf://`` dataset -- it builds a lazy frame and hands it to
``HuggingFaceDataSource.from_frame``, so the knowledge-date index, the event-date floor, the
ticker-to-sid mapping and ``data.current`` resolution are the same code.

Isolation
---------

The user is never taken from the strategy. ``user_id`` and the bucket reach the mount through the
run's *address context* -- ``run_simulation(address_context={"user_id": ..., "user_datasets_bucket":
...})`` -- which the hosting runtime fills from the backtest request. The address names only the
dataset: ``dataset://<uuid>/<name>``, ``dataset://../x`` or anything outside
``^[a-z0-9][a-z0-9_-]{0,62}$`` is refused, so a strategy has no way to point at another user's key.
Without a user or a bucket -- a local run -- the scheme refuses rather than returning nothing.

That is isolation of the *API*. Strategy code is ordinary Python running with the runtime's
credentials: if those may read every user's prefix and the strategy may import an S3 client, the
boundary is the credentials, not this module. See the runtime's notes on scrubbing them.

Reading
-------

Nothing is downloaded to disk. The object is scanned in place with column projection (when the
strategy names its fields) and the knowledge-date window filter, collected with the streaming
engine, so a large file costs memory for what the window keeps rather than for the file. One
``HeadObject`` per run gives the ETag and the size: the size is checked against a limit before any
data is read, and the ETag keys the cache.

Cache
-----

A bulk session runs the same strategy over the same dataset thousands of times in one process.
The mounted frame is kept in memory keyed by ``(user_id, name, etag, window, fields, resolution)``
-- the user first, so one user's entry is never another's, and the ETag, so a re-upload (same key,
new content) is read again. Least recently used entries are evicted past a byte budget.

Errors are one plain line each, never a traceback, and never name the bucket or the user.
"""
