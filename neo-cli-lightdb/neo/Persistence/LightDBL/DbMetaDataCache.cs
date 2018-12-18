using LTLightDB = LightDB.LightDB;
using LTWriteTask = LightDB.WriteTask;
using LTDBValue = LightDB.DBValue;
using LTISnapShot = LightDB.ISnapShot;
using Neo.IO;
using Neo.IO.Caching;
using System;

namespace Neo.Persistence.LightDBL
{
    class DbMetaDataCache<T>: MetaDataCache<T>
        where T : class, ICloneable<T>, ISerializable, new()
    {
        private readonly LTLightDB db;
        private readonly LTISnapShot snapshot;
        private readonly LTWriteTask batch;
        private readonly byte prefix;
        private readonly byte defaultTableid = LevelDB.Prefixes.SYS_DefaulTableId;

        public DbMetaDataCache(LTLightDB db, LTISnapShot snapshot, LTWriteTask batch, byte prefix, Func<T> factory = null)
            : base(factory)
        {
            this.db = db;
            this.snapshot = snapshot ?? db.UseSnapShot();
            this.batch = batch;
            this.prefix = prefix;
        }
        protected override void AddInternal(T item)
        {
            batch?.Put(new byte[] { defaultTableid }, new byte[] { prefix }, LTDBValue.FromValue(LTDBValue.Type.Bytes, item.ToArray()));
        }

        protected override T TryGetInternal()
        {
            var val = snapshot.GetValueData(new byte[] { defaultTableid }, new byte[] { prefix });
            if (val == null)
            {
                return null;
            }
            return val.AsSerializable<T>();
        }

        protected override void UpdateInternal(T item)
        {
            batch?.Put(new byte[] { defaultTableid }, new byte[] { prefix }, LTDBValue.FromValue(LTDBValue.Type.Bytes, item.ToArray()));
        }
    }
}
