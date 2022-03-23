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
		static public void ReadKafka(string topic, Action<string> callback, string ip_port = "127.0.0.1:9094", string type = "latest") {

			var config = new ConsumerConfig {
                GroupId = topic,
                BootstrapServers = ip_port,
                AutoOffsetReset = type == "latest" ? AutoOffsetReset.Latest : AutoOffsetReset.Earliest
            };

            var cancelled = false;

            //Token de cancelamento
            var cancellationToken = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) => {
                cancelled = true;
                cancellationToken.Cancel();
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe(topic);

                while (!cancelled)
                {
                    var consumeResult = consumer.Consume(cancellationToken.Token);
				    callback(consumeResult.Message.Value);
                }

                consumer.Close();
            }

        }
    }
}
