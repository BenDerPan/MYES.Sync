using Nest;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace MYES
{
    public class ESBulk
    {
        public static bool CreateIndex<T>(IElasticClient elasticClient, string indexName) where T : class
        {
            var existsResponse = elasticClient.Indices.Exists(indexName);
            // 存在则返回true 不存在创建
            if (existsResponse.Exists)
            {
                return true;
            }
            //基本配置
            IIndexState indexState = new IndexState
            {
                Settings = new IndexSettings
                {
                    NumberOfReplicas = 1,//副本数
                    NumberOfShards = 3//分片数
                }
            };

            CreateIndexResponse response = elasticClient.Indices.Create(indexName, p => p
                .InitializeUsing(indexState).Map<T>(r => r.AutoMap())
            );

            return response.IsValid;
        }

        public static bool CreateIndex(IElasticClient elasticClient, string indexName, out bool isIndexExist, int numOfReplicas=1,int numOfShards=3)
        {
            isIndexExist = false;
            var existsResponse = elasticClient.Indices.Exists(indexName);
            // 存在则返回true 不存在创建
            if (existsResponse.Exists)
            {
                isIndexExist = true;
                return true;
            }
            //基本配置
            IIndexState indexState = new IndexState
            {
                Settings = new IndexSettings
                {
                    NumberOfReplicas = numOfReplicas,//副本数
                    NumberOfShards = numOfShards//分片数
                }
            };

            CreateIndexResponse response = elasticClient.Indices.Create(indexName, p => p
                .InitializeUsing(indexState));

            return response.IsValid;
        }

        public static ElasticClient GetElasticClient(Uri uri)
        {
            ElasticClient client;
            if (uri!=null)
            {
                client = new ElasticClient(uri);
            }
            else
            {
                client = new ElasticClient();
            }
            
            return client;
        }
        public static bool BulkAll<T>(IElasticClient elasticClient, IndexName indexName, IEnumerable<T> list, out BulkAllResponse bulkAllResponse, int size= 1000) where T : class
        {
            var tokenSource = new CancellationTokenSource();

            var observableBulk = elasticClient.BulkAll(list, f => f
                    .MaxDegreeOfParallelism(4)
                    .BackOffTime(TimeSpan.FromSeconds(30))
                    .BackOffRetries(1)
                    .Size(size)
                    .RefreshOnCompleted()
                    .Index(indexName)
                    .BufferToBulk((r, buffer) => r.IndexMany(buffer))
                , tokenSource.Token);

            var countdownEvent = new CountdownEvent(1);

            Exception exception = null;
            BulkAllResponse res = null;

            void OnCompleted()
            {
                Console.WriteLine("[BulkALL] All Finished!!!!");
                countdownEvent.Signal();
            }


            var bulkAllObserver = new BulkAllObserver(
                onNext: response =>
                {
                    Console.WriteLine($"[BulkALL] Indexed {response.Page * size} with {response.Retries} retries, item count: {response.Items.Count}");
                    res = response;
                },
                onError: ex =>
                {
                    Console.WriteLine($"[BulkALL] Error: {ex.Message}");
                    exception = ex;
                    countdownEvent.Signal();
                },
                OnCompleted);

            observableBulk.Subscribe(bulkAllObserver);

            countdownEvent.Wait(tokenSource.Token);

            bulkAllResponse = res;

            if (exception != null)
            {
                Console.WriteLine($"[BulkALL] Exception : {exception}");
                return false;
            }
            else
            {
                return true;
            }
        }
    }
}
