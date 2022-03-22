using Confluent.Kafka;

namespace cnpz_kafka.GenericKafka
{
    /// <summary>
    /// Classe responsável para chamadas aos consumers (leitura mensagem) do KAFKA
    /// </summary>
    public class Consumer
    {
        /// <summary>
        /// Task para leitura de mensagem no KAFKA
        /// </summary>
        /// <param name="topic">Tópico da mensagem kafka</param>
        /// <param name="ip_port">IP e Porta para endereço da mensagem + topico</param>
        /// <param name="type">Tipo de leitura ao resetar ponteiro</param>
        static public string ReadKafka(string topic, string ip_port = "127.0.0.1:9094", string type = "latest") {   

            var conf = new ConsumerConfig {
                GroupId = topic,
                BootstrapServers = ip_port,
                AutoOffsetReset = type == "latest" ? AutoOffsetReset.Latest : AutoOffsetReset.Earliest
            };
            
            //Consumer
            using var c = new ConsumerBuilder<Ignore, string>(conf).Build();
            
            //Token de cancelamento
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                e.Cancel = true;
                cts.Cancel();
            };

            var cr = c.Consume(cts.Token);
            c.Close();

            return cr.Message.Value ;

        }
    }
}
