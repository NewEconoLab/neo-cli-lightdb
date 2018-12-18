using LTLightDB = LightDB.LightDB;
using LTWriteTask = LightDB.WriteTask;
using LTDBValue = LightDB.DBValue;
using LTISnapShot = LightDB.ISnapShot;
using Neo.IO;
using Neo.IO.Caching;
using System;
using System.Collections.Generic;

namespace Neo.Persistence.LightDBL
{
    internal class DbCache<TKey, TValue> : DataCache<TKey, TValue>
        where TKey : IEquatable<TKey>, ISerializable, new()
        where TValue : class, ICloneable<TValue>, ISerializable, new()
    {
        private readonly LTLightDB db;
        private LTISnapShot snapshot;
        private readonly LTWriteTask batch;
        private readonly byte prefix;

        public DbCache(LTLightDB db, LTISnapShot snapshot, LTWriteTask batch, byte prefix)
        {
            this.db = db;
            this.snapshot = snapshot ?? db.UseSnapShot();
            this.batch = batch;
            this.prefix = prefix;
        }

        protected override void AddInternal(TKey key, TValue value)
        {
            batch?.Put(new byte[] { prefix }, key.ToArray(), LTDBValue.FromValue(LTDBValue.Type.Bytes, value.ToArray()));
        }

        public override void DeleteInternal(TKey key)
        {
            batch?.Delete(new byte[] { prefix }, key.ToArray());
        }

        protected override IEnumerable<KeyValuePair<TKey, TValue>> FindInternal(byte[] key_prefix)
        {
            var iterator = snapshot.CreateKeyIterator(new byte[] { prefix }, key_prefix);
            
            for (; iterator.MoveNext();){
                
            }
            var findder = snapshot.CreateKeyFinder(new byte[] { prefix }, key_prefix);
            findder.GetEnumerator();
            //return db.UseSnapShot().GetValueData(new byte[] { prefix }, key.ToArray(), (k, v) => new KeyValuePair<TKey, TValue>(k.ToArray().AsSerializable<TKey>(1), v.ToArray().AsSerializable<TValue>()));
            return null;
        }

        protected override TValue GetInternal(TKey key)
        {
            return snapshot.GetValueData(new byte[] { prefix }, key.ToArray()).AsSerializable<TValue>();
        }

        protected override TValue TryGetInternal(TKey key)
        {
            var val = snapshot.GetValueData(new byte[] { prefix }, key.ToArray());
            if(val == null)
            {
                return null;
            }
            return val.AsSerializable<TValue>();
        }

        protected override void UpdateInternal(TKey key, TValue value)
        {
            batch?.Put(new byte[] { prefix }, key.ToArray(), LTDBValue.FromValue(LTDBValue.Type.Bytes, value.ToArray()));
        }
    }
}
