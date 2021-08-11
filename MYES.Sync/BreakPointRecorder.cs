using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using MonkeyCache;
using MonkeyCache.LiteDB;

namespace MYES
{
    public class BreakPointRecorder
    {
        static BreakPointRecorder _current;
        public static BreakPointRecorder Current
        {
            get 
            {
                if (_current==null)
                {
                    _current = new BreakPointRecorder();
                }

                return _current;
            }
        }

        readonly static string CacheDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "cache_storage");
        private BreakPointRecorder()
        {
            Barrel.ApplicationId = "MYES.Sync";
            Barrel.Create(CacheDir, true);
        }


        public T Get<T>(string key)
        {
            return Barrel.Current.Get<T>(key);
        }

        public void Set<T>(string key,T obj, long expireInSeconds=0)
        {
            if (expireInSeconds>0)
            {
                Barrel.Current.Add<T>(key, obj, TimeSpan.FromSeconds(expireInSeconds));
            }
            else
            {
                Barrel.Current.Add<T>(key, obj, TimeSpan.FromSeconds(int.MaxValue));
            }
           
        }

        public void RemoveAll()
        {
            Barrel.Current.EmptyAll();
        }

        public void RemoveExpired()
        {
            Barrel.Current.EmptyExpired();
        }

        public void Remove(params string[] key)
        {
            Barrel.Current.Empty(key);
        }

        public bool Exists(string key)
        {
            return Barrel.Current.Exists(key);
        }

        public bool IsExpired(string key)
        {
            return Barrel.Current.IsExpired(key);
        }

    }
}
