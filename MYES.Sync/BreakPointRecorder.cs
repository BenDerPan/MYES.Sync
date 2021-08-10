using System;
using System.Collections.Generic;
using System.Text;
using MonkeyCache;
using MonkeyCache.LiteDB;

namespace MYES
{
    public class BreakPointRecorder
    {
        readonly object _lckObj = new object();

        readonly Dictionary<string, BreakPoint> _breakPoints = new Dictionary<string, BreakPoint>();

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

        public bool TryGetBreakPoint(string key,out BreakPoint point)
        {
            point = null;
            lock (_lckObj)
            {
                if (_breakPoints.ContainsKey(key))
                {
                    point= _breakPoints[key];
                    return true;
                }
            }

            return false;
        }

        public bool IsBreakPointExists(string key)
        {
            lock (_lckObj)
            {
                return _breakPoints.ContainsKey(key);
            }
        }

        public void AddBreakPont(string key, BreakPoint point)
        {
            lock (_lckObj)
            {
                _breakPoints[key] = point;
            }
        }

        public void RemoveBreakPont(string key)
        {
            lock (_lckObj)
            {
                if (_breakPoints.ContainsKey(key))
                {
                    _breakPoints.Remove(key);
                }
            }
        }

        public void ClearBreakPoint()
        {
            lock (_lckObj)
            {
                _breakPoints.Clear();
            }
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
