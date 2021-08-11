using System;
using System.Collections.Generic;
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
        private BreakPointRecorder()
        {
            Barrel.ApplicationId = "MYES.Sync";
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
